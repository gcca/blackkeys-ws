import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from blackkeys.backends.broker import (
    BrokerError,
    BrokerNode,
    ParseBrokerNodes,
)
from blackkeys.backends.publishers.signup import (
    DecodeUserSignup,
    EncodeUserSignup,
    MakeSignupPublisher,
    SignupPublisher,
)
from blackkeys.backends.stores.cache import DecodeUserAuth, EncodeUserAuth
from blackkeys.blueprints.auth import Signup
from blackkeys.core.conf import Settings
from blackkeys.repositories import AuthRepository

unittest.defaultTestLoader.testMethodPrefix = "Test"


class FakeExchange:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.published: list[tuple[object, str]] = []

    async def publish(self, message, routing_key, timeout=None):
        if self.error is not None:
            raise self.error
        self.published.append((message, routing_key))


class FakeChannel:
    def __init__(self, error: Exception | None = None) -> None:
        self.is_closed = False
        self.closed = False
        self.default_exchange = FakeExchange(error)

    async def close(self) -> None:
        self.closed = True
        self.is_closed = True


class FakePublisher:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, str]] = []

    async def Publish(self, username: str, password: str) -> None:
        if self.error is not None:
            raise self.error
        self.calls.append((username, password))


def Publisher(channel: FakeChannel) -> SignupPublisher:
    publisher = SignupPublisher(
        (BrokerNode("rabbit-a", 5672),),
        "blackkeys",
        "blackkeys",
        "/",
        "blackkeys-signup",
    )
    publisher._channel = channel
    return publisher


class UserSignupMessageTests(unittest.TestCase):
    def TestFlatBufferRoundTrip(self) -> None:
        encoded = EncodeUserSignup("alice", "correct-password")

        decoded = DecodeUserSignup(encoded)

        self.assertIsNotNone(decoded)
        assert decoded is not None
        self.assertEqual(decoded.username, "alice")
        self.assertEqual(decoded.password, "correct-password")

    def TestInvalidFlatBufferIsRejected(self) -> None:
        self.assertIsNone(DecodeUserSignup(b"not-a-user-signup-message"))

    def TestUserAuthRecordIsNotAUserSignupMessage(self) -> None:
        self.assertIsNone(
            DecodeUserSignup(EncodeUserAuth("alice", "$argon2id$hash"))
        )
        self.assertIsNone(
            DecodeUserAuth(EncodeUserSignup("alice", "correct-password"))
        )

    def TestRejectsUnusableCredentials(self) -> None:
        with self.assertRaises(ValueError):
            EncodeUserSignup("", "correct-password")
        with self.assertRaises(ValueError):
            EncodeUserSignup("ali ce", "correct-password")
        with self.assertRaises(ValueError):
            EncodeUserSignup("alice", "")


class BrokerNodeTests(unittest.TestCase):
    def TestParsesOrderedNodes(self) -> None:
        self.assertEqual(
            ParseBrokerNodes(("rabbit-a:5672", " rabbit-b:5673 ")),
            (BrokerNode("rabbit-a", 5672), BrokerNode("rabbit-b", 5673)),
        )

    def TestOmittedPortUsesTheAmqpDefault(self) -> None:
        self.assertEqual(
            ParseBrokerNodes(("rabbit-a",)), (BrokerNode("rabbit-a", 5672),)
        )

    def TestRejectsUnusableNodes(self) -> None:
        for nodes in (("",), (":5672",), ("rabbit-a:port",), ("rabbit-a:0",)):
            with self.assertRaises(ValueError):
                ParseBrokerNodes(nodes)

    def TestNoNodesMeansNoPublisher(self) -> None:
        self.assertIsNone(
            MakeSignupPublisher((), "blackkeys", "blackkeys", "/", "queue")
        )


class SignupPublisherTests(unittest.TestCase):
    def TestPublishesAPersistentFlatBufferMessage(self) -> None:
        channel = FakeChannel()
        publisher = Publisher(channel)

        asyncio.run(publisher.Publish("alice", "correct-password"))

        message, routing_key = channel.default_exchange.published[0]
        decoded = DecodeUserSignup(message.body)
        self.assertEqual(routing_key, "blackkeys-signup")
        self.assertEqual(message.content_type, "application/octet-stream")
        self.assertEqual(message.delivery_mode, 2)
        assert decoded is not None
        self.assertEqual(decoded.username, "alice")
        self.assertEqual(decoded.password, "correct-password")

    def TestPublishFailureBecomesABrokerError(self) -> None:
        publisher = Publisher(FakeChannel(ConnectionError("no route")))

        with self.assertRaises(BrokerError):
            asyncio.run(publisher.Publish("alice", "correct-password"))

    def TestFailedPublishDropsTheChannel(self) -> None:
        channel = FakeChannel(ConnectionError("no route"))
        publisher = Publisher(channel)

        with self.assertRaises(BrokerError):
            asyncio.run(publisher.Publish("alice", "correct-password"))

        self.assertIsNone(publisher._channel)
        self.assertTrue(channel.closed)

    def TestUnreachableNodesBecomeABrokerError(self) -> None:
        publisher = MakeSignupPublisher(
            ("127.0.0.1:1",), "blackkeys", "blackkeys", "/", "blackkeys-signup"
        )
        assert publisher is not None

        with self.assertRaises(BrokerError):
            asyncio.run(publisher.Publish("alice", "correct-password"))


class SignupEndpointTests(unittest.TestCase):
    request = SimpleNamespace(
        json={"username": "alice", "password": "correct-password"}
    )

    def TestSignupQueuesTheCredentials(self) -> None:
        publisher = FakePublisher()
        repository = AuthRepository(None, publisher)

        with patch("blackkeys.blueprints.auth.auth_repository", repository):
            response = asyncio.run(Signup(self.request))

        self.assertEqual(response.status, 202)
        self.assertEqual(json.loads(response.body), {"status": "accepted"})
        self.assertEqual(publisher.calls, [("alice", "correct-password")])

    def TestSignupRejectsMalformedRequests(self) -> None:
        for payload in (
            None,
            {},
            {"username": "alice"},
            {"username": "", "password": "correct-password"},
            {"username": "ali ce", "password": "correct-password"},
            {"username": "alice", "password": 1},
        ):
            request = SimpleNamespace(json=payload)
            repository = AuthRepository(None, FakePublisher())

            with patch("blackkeys.blueprints.auth.auth_repository", repository):
                response = asyncio.run(Signup(request))

            self.assertEqual(response.status, 400)
            self.assertEqual(
                json.loads(response.body), {"error": "invalid-request"}
            )

    def TestSignupRequiresMqConfiguration(self) -> None:
        repository = AuthRepository(None, None)

        with patch("blackkeys.blueprints.auth.auth_repository", repository):
            response = asyncio.run(Signup(self.request))

        self.assertEqual(response.status, 503)
        self.assertEqual(
            json.loads(response.body), {"error": "signup-unavailable"}
        )

    def TestSignupReportsAnUnavailableBroker(self) -> None:
        publisher = FakePublisher(BrokerError("no reachable mq node"))
        repository = AuthRepository(None, publisher)

        with patch("blackkeys.blueprints.auth.auth_repository", repository):
            response = asyncio.run(Signup(self.request))

        self.assertEqual(response.status, 503)
        self.assertEqual(
            json.loads(response.body), {"error": "signup-unavailable"}
        )


class MqSettingsTests(unittest.TestCase):
    def TestDefaultsToNoNodesAndTheSignupQueue(self) -> None:
        loaded = Settings.FromEnv({"SECRET": "secret"})

        self.assertEqual(loaded.mq_nodes, ())
        self.assertEqual(loaded.mq_user, "guest")
        self.assertEqual(loaded.mq_password, "guest")
        self.assertEqual(loaded.mq_vhost, "/")
        self.assertEqual(
            loaded.mq_signup_queue, Settings.MQ_SIGNUP_QUEUE_DEFAULT
        )
        self.assertEqual(
            loaded.mq_hydration_queue, Settings.MQ_HYDRATION_QUEUE_DEFAULT
        )

    def TestLoadsValuesFromEnvironment(self) -> None:
        loaded = Settings.FromEnv(
            {
                "SECRET": "secret",
                "MQ_NODES": "rabbit-a:5672, rabbit-b:5673",
                "MQ_USER": "blackkeys",
                "MQ_PASSWORD": "blackkeys",
                "MQ_VHOST": "/blackkeys",
                "MQ_SIGNUP_QUEUE": "custom-signup",
                "MQ_HYDRATION_QUEUE": "custom-hydration",
            }
        )

        self.assertEqual(loaded.mq_nodes, ("rabbit-a:5672", "rabbit-b:5673"))
        self.assertEqual(loaded.mq_user, "blackkeys")
        self.assertEqual(loaded.mq_password, "blackkeys")
        self.assertEqual(loaded.mq_vhost, "/blackkeys")
        self.assertEqual(loaded.mq_signup_queue, "custom-signup")
        self.assertEqual(loaded.mq_hydration_queue, "custom-hydration")

    def TestRejectsAnEmptyNode(self) -> None:
        with self.assertRaises(ValueError):
            Settings.FromEnv({"SECRET": "secret", "MQ_NODES": "rabbit-a:5672,"})

    def TestRejectsAnEmptySignupQueue(self) -> None:
        with self.assertRaises(ValueError):
            Settings.FromEnv({"SECRET": "secret", "MQ_SIGNUP_QUEUE": ""})

    def TestRejectsAnEmptyHydrationQueue(self) -> None:
        with self.assertRaises(ValueError):
            Settings.FromEnv({"SECRET": "secret", "MQ_HYDRATION_QUEUE": ""})
