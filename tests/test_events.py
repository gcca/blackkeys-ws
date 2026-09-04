import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx2

from blackkeys.backends.publishers.hydration import (
    HYDRATE_EVENTS_MESSAGE_NAME,
    DecodeHydrationMessage,
    EncodeHydrationMessage,
)
from blackkeys.backends.services.events import (
    EventsService,
    MakeEventsService,
)
from blackkeys.backends.stores.cache import (
    EVENTS_LIST_CACHE_KEY,
    MakeEventsCache,
    ReadEventsList,
)
from blackkeys.backends.stores.local import (
    LocalCacheClear,
    LocalCacheGet,
    LocalCacheSet,
)
from blackkeys.blueprints.events import List
from blackkeys.repositories import EventsRepository

unittest.defaultTestLoader.testMethodPrefix = "Test"


class FakeResponse:
    def __init__(self, payload: object, status_error: Exception | None = None):
        self.payload = payload
        self.status_error = status_error

    def raise_for_status(self) -> None:
        if self.status_error is not None:
            raise self.status_error

    def json(self) -> object:
        return self.payload


class FakeAsyncClient:
    def __init__(self, response: FakeResponse | None, error: Exception | None):
        self.response = response
        self.error = error

    async def __aenter__(self) -> "FakeAsyncClient":
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False

    async def get(self, path: str, **_: object) -> FakeResponse:
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response


def FakeAsyncClientFactory(
    response: FakeResponse | None = None, error: Exception | None = None
):
    def Factory(*_: object, **__: object) -> FakeAsyncClient:
        return FakeAsyncClient(response, error)

    return Factory


class HydrationMessageTests(unittest.TestCase):
    def TestFlatBufferRoundTrip(self) -> None:
        encoded = EncodeHydrationMessage(HYDRATE_EVENTS_MESSAGE_NAME)

        decoded = DecodeHydrationMessage(encoded)

        self.assertIsNotNone(decoded)
        assert decoded is not None
        self.assertEqual(decoded.name, HYDRATE_EVENTS_MESSAGE_NAME)

    def TestInvalidFlatBufferIsRejected(self) -> None:
        self.assertIsNone(DecodeHydrationMessage(b"not-a-hydration-message"))

    def TestRejectsAnEmptyName(self) -> None:
        with self.assertRaises(ValueError):
            EncodeHydrationMessage("")


class EventsCacheTests(unittest.TestCase):
    @patch("blackkeys.backends.stores.cache.pylibmc.ClientPool")
    @patch("blackkeys.backends.stores.cache.pylibmc.Client")
    def TestEventsCacheUsesConsistentHashing(
        self, client_constructor: MagicMock, pool_constructor: MagicMock
    ) -> None:
        client = client_constructor.return_value

        cache = MakeEventsCache(("cache-a:11211", "cache-b:11211"))

        client_constructor.assert_called_once_with(
            ["cache-a:11211", "cache-b:11211"],
            binary=True,
            behaviors={"ketama": True, "num_replicas": 1},
        )
        pool_constructor.assert_called_once_with(client, 4)
        self.assertIs(cache, pool_constructor.return_value)

    def TestNoNodesMeansNoCache(self) -> None:
        self.assertIsNone(MakeEventsCache(()))

    def TestReadEventsListDecodesJson(self) -> None:
        cache = MagicMock()
        client = cache.reserve.return_value.__enter__.return_value
        client.get.return_value = json.dumps([{"id": 1}]).encode()

        events = ReadEventsList(cache)

        self.assertEqual(events, [{"id": 1}])
        client.get.assert_called_once_with(EVENTS_LIST_CACHE_KEY)

    def TestReadEventsListRejectsAMiss(self) -> None:
        cache = MagicMock()
        client = cache.reserve.return_value.__enter__.return_value
        client.get.return_value = None

        self.assertIsNone(ReadEventsList(cache))

    def TestReadEventsListRejectsNonListPayloads(self) -> None:
        cache = MagicMock()
        client = cache.reserve.return_value.__enter__.return_value
        client.get.return_value = json.dumps({"id": 1}).encode()

        self.assertIsNone(ReadEventsList(cache))


class EventsServiceTests(unittest.TestCase):
    def TestNoHostMeansNoService(self) -> None:
        self.assertIsNone(MakeEventsService(None))

    def TestFetchesTheEventsListOverHttp(self) -> None:
        service = EventsService("http://assets")
        response = FakeResponse([{"id": 1}])

        with patch(
            "blackkeys.backends.services.events.httpx2.AsyncClient",
            FakeAsyncClientFactory(response=response),
        ):
            events = asyncio.run(service.List())

        self.assertEqual(events, [{"id": 1}])

    def TestReturnsNoneWhenTheRequestFails(self) -> None:
        service = EventsService("http://assets")

        with patch(
            "blackkeys.backends.services.events.httpx2.AsyncClient",
            FakeAsyncClientFactory(error=httpx2.ConnectError("down")),
        ):
            self.assertIsNone(asyncio.run(service.List()))


class EventsRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        LocalCacheClear()

    def tearDown(self) -> None:
        LocalCacheClear()

    def TestReturnsFromLocalCacheWithoutTouchingAnythingElse(self) -> None:
        LocalCacheSet(EVENTS_LIST_CACHE_KEY, [{"id": 1}])
        cache = MagicMock()
        publisher = AsyncMock()
        service = AsyncMock()
        repository = EventsRepository(cache, publisher, service)

        value = asyncio.run(repository.List())

        self.assertEqual(value, [{"id": 1}])
        cache.reserve.assert_not_called()
        publisher.Publish.assert_not_called()
        service.List.assert_not_called()

    def TestReturnsFromMemcachedAndPopulatesLocalCache(self) -> None:
        cache = MagicMock()
        client = cache.reserve.return_value.__enter__.return_value
        client.get.return_value = json.dumps([{"id": 2}]).encode()
        publisher = AsyncMock()
        service = AsyncMock()
        repository = EventsRepository(cache, publisher, service)

        value = asyncio.run(repository.List())

        self.assertEqual(value, [{"id": 2}])
        publisher.Publish.assert_not_called()
        service.List.assert_not_called()
        self.assertEqual(LocalCacheGet(EVENTS_LIST_CACHE_KEY), [{"id": 2}])

    def TestFallsBackToTheAssetsServiceOnACompleteCacheMiss(self) -> None:
        publisher = AsyncMock()
        service = AsyncMock()
        service.List.return_value = [{"id": 3}]
        repository = EventsRepository(None, publisher, service)

        value = asyncio.run(repository.List())

        self.assertEqual(value, [{"id": 3}])
        publisher.Publish.assert_awaited_once_with(HYDRATE_EVENTS_MESSAGE_NAME)
        service.List.assert_awaited_once_with()
        self.assertEqual(LocalCacheGet(EVENTS_LIST_CACHE_KEY), [{"id": 3}])

    def TestReportsServiceUnavailableWhenEverythingFails(self) -> None:
        repository = EventsRepository(None, None, None)

        self.assertIsNone(asyncio.run(repository.List()))


class ListEndpointTests(unittest.TestCase):
    request = SimpleNamespace(json=None)

    def TestReturnsTheRepositoryValue(self) -> None:
        repository = AsyncMock()
        repository.List.return_value = [{"id": 1}]

        with patch("blackkeys.blueprints.events.events_repository", repository):
            response = asyncio.run(List(self.request))

        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.body), [{"id": 1}])

    def TestReportsServiceUnavailableWhenRepositoryMisses(self) -> None:
        repository = AsyncMock()
        repository.List.return_value = None

        with patch("blackkeys.blueprints.events.events_repository", repository):
            response = asyncio.run(List(self.request))

        self.assertEqual(response.status, 503)
        self.assertEqual(
            json.loads(response.body), {"error": "events-unavailable"}
        )
