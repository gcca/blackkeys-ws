import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from argon2 import PasswordHasher, Type

from blackkeys.backends.stores.cache import (
    Authenticate,
    DecodeUserAuth,
    EncodeUserAuth,
    MakeAuthCache,
    UserAuthKey,
    VerifyPassword,
)
from blackkeys.blueprints.auth import Signin
from blackkeys.core.auth import RandomString, SignSession, VerifySession
from blackkeys.core.conf import Settings, settings
from blackkeys.repositories import AuthRepository

unittest.defaultTestLoader.testMethodPrefix = "Test"


class SessionTokenTests(unittest.TestCase):
    secret = "test-secret"
    payload = {"iat": 1_000, "exp": 1_100}

    def TestValidTokenReturnsPayload(self) -> None:
        token = SignSession(self.payload, self.secret)

        verified = VerifySession(token, self.secret, timestamp=1_050)

        self.assertIsNotNone(verified)
        assert verified is not None
        self.assertEqual(verified["iat"], self.payload["iat"])
        self.assertEqual(verified["exp"], self.payload["exp"])
        self.assertEqual(len(verified["salt"]), 16)

    def TestExpiredTokenIsRejected(self) -> None:
        token = SignSession(self.payload, self.secret)

        self.assertIsNone(VerifySession(token, self.secret, timestamp=1_101))

    def TestTamperedTokenIsRejected(self) -> None:
        token = SignSession(self.payload, self.secret)

        self.assertIsNone(
            VerifySession(token + "x", self.secret, timestamp=1_050)
        )

    def TestWrongSecretIsRejected(self) -> None:
        token = SignSession(self.payload, self.secret)

        self.assertIsNone(VerifySession(token, "wrong-secret", timestamp=1_050))

    def TestEachTokenHasRandomSalt(self) -> None:
        first = VerifySession(
            SignSession(self.payload, self.secret),
            self.secret,
            timestamp=1_050,
        )
        second = VerifySession(
            SignSession(self.payload, self.secret),
            self.secret,
            timestamp=1_050,
        )

        assert first is not None
        assert second is not None
        self.assertNotEqual(first["salt"], second["salt"])

    def TestRandomStringUsesRequestedLength(self) -> None:
        self.assertEqual(len(RandomString()), 16)
        self.assertEqual(len(RandomString(32)), 32)


class SettingsTests(unittest.TestCase):
    def TestDefaultsToSevenDays(self) -> None:
        loaded = Settings.FromEnv({"SECRET": "secret"})

        self.assertEqual(
            loaded.auth_ttl_seconds, Settings.AUTH_TTL_SECONDS_DEFAULT
        )

    def TestLoadsValuesFromEnvironment(self) -> None:
        loaded = Settings.FromEnv(
            {
                "SECRET": "secret",
                "AUTH_TTL_SECONDS": "90",
                "CACHE_NODES": "cache-a:11211,cache-b:11211",
            }
        )

        self.assertEqual(
            loaded,
            Settings("secret", 90, ("cache-a:11211", "cache-b:11211")),
        )

    def TestRejectsInvalidTtl(self) -> None:
        with self.assertRaises(ValueError):
            Settings.FromEnv(
                {
                    "SECRET": "secret",
                    "AUTH_TTL_SECONDS": "not-a-number",
                }
            )


class CachedUserAuthTests(unittest.TestCase):
    password = PasswordHasher(type=Type.ID).hash("correct-password")

    @patch("blackkeys.backends.stores.cache.pylibmc.ClientPool")
    @patch("blackkeys.backends.stores.cache.pylibmc.Client")
    def TestAuthCacheUsesConsistentHashing(
        self, client_constructor: MagicMock, pool_constructor: MagicMock
    ) -> None:
        client = client_constructor.return_value

        cache = MakeAuthCache(("cache-a:11211", "cache-b:11211"))

        client_constructor.assert_called_once_with(
            ["cache-a:11211", "cache-b:11211"],
            binary=True,
            behaviors={"ketama": True, "num_replicas": 1},
        )
        pool_constructor.assert_called_once_with(client, 4)
        self.assertIs(cache, pool_constructor.return_value)

    def TestFlatBufferRoundTrip(self) -> None:
        encoded = EncodeUserAuth("alice", self.password)

        decoded = DecodeUserAuth(encoded)

        self.assertIsNotNone(decoded)
        assert decoded is not None
        self.assertEqual(decoded.username, "alice")
        self.assertEqual(decoded.password, self.password)

    def TestInvalidFlatBufferIsRejected(self) -> None:
        self.assertIsNone(DecodeUserAuth(b"not-a-user-auth-record"))

    def TestUsesExpectedCacheKey(self) -> None:
        self.assertEqual(UserAuthKey("alice"), "auth:user:alice")

    def TestArgon2idPasswordIsVerified(self) -> None:
        self.assertTrue(VerifyPassword("correct-password", self.password))
        self.assertFalse(VerifyPassword("wrong-password", self.password))
        self.assertFalse(VerifyPassword("correct-password", "not-argon2id"))

    def TestAuthenticateReadsAndVerifiesCachedUser(self) -> None:
        cache = MagicMock()
        client = cache.reserve.return_value.__enter__.return_value
        client.get.return_value = EncodeUserAuth("alice", self.password)

        authenticated = Authenticate(cache, "alice", "correct-password")

        self.assertIsNotNone(authenticated)
        client.get.assert_called_once_with("auth:user:alice")


class SigninTests(unittest.TestCase):
    password = PasswordHasher(type=Type.ID).hash("correct-password")

    def TestSigninReturnsASevenDaySession(self) -> None:
        cache = MagicMock()
        client = cache.reserve.return_value.__enter__.return_value
        client.get.return_value = EncodeUserAuth("alice", self.password)
        request = SimpleNamespace(
            json={"username": "alice", "password": "correct-password"}
        )
        repository = AuthRepository(cache, None)

        with patch("blackkeys.blueprints.auth.auth_repository", repository):
            response = asyncio.run(Signin(request))
        token = json.loads(response.body)["token"]

        self.assertEqual(response.status, 200)
        self.assertTrue(token.startswith("blackkeys-v1_"))
        payload = VerifySession(
            token.removeprefix("blackkeys-v1_"), settings.secret
        )
        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["sub"], "alice")
        self.assertEqual(
            payload["exp"] - payload["iat"], settings.auth_ttl_seconds
        )

    def TestSigninRejectsWrongPassword(self) -> None:
        cache = MagicMock()
        client = cache.reserve.return_value.__enter__.return_value
        client.get.return_value = EncodeUserAuth("alice", self.password)
        request = SimpleNamespace(
            json={"username": "alice", "password": "wrong-password"}
        )
        repository = AuthRepository(cache, None)

        with patch("blackkeys.blueprints.auth.auth_repository", repository):
            response = asyncio.run(Signin(request))

        self.assertEqual(response.status, 401)
        self.assertEqual(
            json.loads(response.body), {"error": "invalid-credentials"}
        )

    def TestSigninRequiresCacheConfiguration(self) -> None:
        request = SimpleNamespace(
            json={"username": "alice", "password": "correct-password"}
        )
        repository = AuthRepository(None, None)

        with patch("blackkeys.blueprints.auth.auth_repository", repository):
            response = asyncio.run(Signin(request))

        self.assertEqual(response.status, 503)
