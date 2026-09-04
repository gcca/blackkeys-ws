from __future__ import annotations

import asyncio

import pylibmc
from sanic.log import logger

from blackkeys.backends.broker import BrokerError
from blackkeys.backends.publishers.hydration import (
    HYDRATE_EVENTS_MESSAGE_NAME,
    HydrationPublisher,
    MakeHydrationPublisher,
)
from blackkeys.backends.publishers.signup import (
    MakeSignupPublisher,
    SignupPublisher,
)
from blackkeys.backends.services.events import (
    EventsService,
    MakeEventsService,
)
from blackkeys.backends.stores import cache as cache_store
from blackkeys.backends.stores.local import LocalCacheGet, LocalCacheSet
from blackkeys.core.conf import Settings


class AuthenticationUnavailable(Exception):
    pass


class SignupUnavailable(Exception):
    pass


class AuthRepository:
    __slots__ = ("_cache", "_signup_publisher")

    def __init__(
        self,
        cache: pylibmc.ClientPool | None,
        signup_publisher: SignupPublisher | None,
    ) -> None:
        self._cache = cache
        self._signup_publisher = signup_publisher

    def ValidUsername(self, username: str) -> bool:
        try:
            cache_store.UserAuthKey(username)
        except ValueError:
            return False
        return True

    async def Open(self) -> None:
        if self._signup_publisher is None:
            return
        try:
            await self._signup_publisher.Connect()
        except BrokerError as error:
            logger.warning("signup queue unavailable at startup: %s", error)

    async def Close(self) -> None:
        if self._signup_publisher is not None:
            await self._signup_publisher.Close()

    async def Authenticate(
        self, username: str, password: str
    ) -> cache_store.CachedUserAuth | None:
        if self._cache is None:
            raise AuthenticationUnavailable
        try:
            return await asyncio.to_thread(
                cache_store.Authenticate,
                self._cache,
                username,
                password,
            )
        except pylibmc.Error as error:
            raise AuthenticationUnavailable from error

    async def Signup(self, username: str, password: str) -> None:
        if self._signup_publisher is None:
            raise SignupUnavailable
        try:
            await self._signup_publisher.Publish(username, password)
        except BrokerError as error:
            logger.warning("signup publish failed: %s", error)
            raise SignupUnavailable from error


class EventsRepository:
    __slots__ = ("_cache", "_hydration_publisher", "_service")

    def __init__(
        self,
        cache: pylibmc.ClientPool | None,
        hydration_publisher: HydrationPublisher | None,
        service: EventsService | None,
    ) -> None:
        self._cache = cache
        self._hydration_publisher = hydration_publisher
        self._service = service

    async def Open(self) -> None:
        if self._hydration_publisher is None:
            return
        try:
            await self._hydration_publisher.Connect()
        except BrokerError as error:
            logger.warning("hydration queue unavailable at startup: %s", error)

    async def Close(self) -> None:
        if self._hydration_publisher is not None:
            await self._hydration_publisher.Close()

    async def List(self) -> list | None:
        cached = LocalCacheGet(cache_store.EVENTS_LIST_CACHE_KEY)
        if cached is not None:
            return cached

        if self._cache is not None:
            try:
                value = await asyncio.to_thread(
                    cache_store.ReadEventsList, self._cache
                )
            except pylibmc.Error as error:
                logger.warning("events cache read failed: %s", error)
                value = None
            if value is not None:
                LocalCacheSet(cache_store.EVENTS_LIST_CACHE_KEY, value)
                return value

        if self._hydration_publisher is not None:
            try:
                await self._hydration_publisher.Publish(
                    HYDRATE_EVENTS_MESSAGE_NAME
                )
            except BrokerError as error:
                logger.warning("hydration publish failed: %s", error)

        if self._service is None:
            return None

        value = await self._service.List()
        if value is not None:
            LocalCacheSet(cache_store.EVENTS_LIST_CACHE_KEY, value)
        return value


def MakeEventsRepository(settings: Settings) -> EventsRepository:
    return EventsRepository(
        cache_store.MakeEventsCache(settings.cache_nodes),
        MakeHydrationPublisher(
            settings.mq_nodes,
            settings.mq_user,
            settings.mq_password,
            settings.mq_vhost,
            settings.mq_hydration_queue,
        ),
        MakeEventsService(settings.assets_host),
    )


def MakeAuthRepository(settings: Settings) -> AuthRepository:
    return AuthRepository(
        cache_store.MakeAuthCache(settings.cache_nodes),
        MakeSignupPublisher(
            settings.mq_nodes,
            settings.mq_user,
            settings.mq_password,
            settings.mq_vhost,
            settings.mq_signup_queue,
        ),
    )
