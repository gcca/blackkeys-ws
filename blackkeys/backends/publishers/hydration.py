from __future__ import annotations

import asyncio
import struct
from contextlib import suppress
from dataclasses import dataclass

import aio_pika
import flatbuffers
import flatbuffers.util
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

from blackkeys.backends.broker import (
    BROKER_EXCEPTIONS,
    CONNECT_TIMEOUT_SECONDS,
    PUBLISH_TIMEOUT_SECONDS,
    BrokerError,
    BrokerNode,
    ParseBrokerNodes,
)
from blackkeys.schemas.HydrationMessage import (
    HydrationMessage,
    HydrationMessageAddName,
    HydrationMessageEnd,
    HydrationMessageStart,
)

HYDRATION_MESSAGE_IDENTIFIER = b"BKHM"
HYDRATION_CONTENT_TYPE = "application/octet-stream"
HYDRATE_EVENTS_MESSAGE_NAME = "blackkeys-hydrate-events"


@dataclass(frozen=True, slots=True)
class QueuedHydrationMessage:
    name: str


def EncodeHydrationMessage(name: str) -> bytes:
    if not name:
        raise ValueError("name must not be empty")

    builder = flatbuffers.Builder(256)
    name_offset = builder.CreateString(name)
    HydrationMessageStart(builder)
    HydrationMessageAddName(builder, name_offset)
    root = HydrationMessageEnd(builder)
    builder.Finish(root, file_identifier=HYDRATION_MESSAGE_IDENTIFIER)
    return bytes(builder.Output())


def DecodeHydrationMessage(
    value: bytes | bytearray | memoryview,
) -> QueuedHydrationMessage | None:
    buffer = bytes(value)
    try:
        if not flatbuffers.util.BufferHasIdentifier(
            buffer, 0, HYDRATION_MESSAGE_IDENTIFIER
        ):
            return None
        hydration_message = HydrationMessage.GetRootAs(buffer)
        name_value = hydration_message.Name()
        if name_value is None:
            return None
        name = name_value.decode()
    except (IndexError, TypeError, UnicodeDecodeError, struct.error):
        return None

    if not name:
        return None
    return QueuedHydrationMessage(name)


class HydrationPublisher:
    __slots__ = (
        "_nodes",
        "_user",
        "_password",
        "_vhost",
        "_queue_name",
        "_connection",
        "_channel",
        "_lock",
    )

    def __init__(
        self,
        nodes: tuple[BrokerNode, ...],
        user: str,
        password: str,
        vhost: str,
        queue_name: str,
    ) -> None:
        if not nodes:
            raise ValueError("nodes must not be empty")
        if not queue_name:
            raise ValueError("queue_name must not be empty")

        self._nodes = nodes
        self._user = user
        self._password = password
        self._vhost = vhost
        self._queue_name = queue_name
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._lock = asyncio.Lock()

    @property
    def queue_name(self) -> str:
        return self._queue_name

    async def Connect(self) -> AbstractRobustChannel:
        async with self._lock:
            channel = self._channel
            if channel is not None and not channel.is_closed:
                return channel

            await self.Discard()
            failures = []
            for node in self._nodes:
                try:
                    self._connection = await aio_pika.connect_robust(
                        host=node.host,
                        port=node.port,
                        login=self._user,
                        password=self._password,
                        virtualhost=self._vhost,
                        timeout=CONNECT_TIMEOUT_SECONDS,
                        client_properties={"connection_name": "blackkeys"},
                    )
                    self._channel = await self._connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                    await self._channel.declare_queue(
                        self._queue_name, durable=True
                    )
                except BROKER_EXCEPTIONS as error:
                    failures.append(f"{node.address}: {error}")
                    await self.Discard()
                    continue
                return self._channel

            raise BrokerError(f"no reachable mq node ({'; '.join(failures)})")

    async def Discard(self) -> None:
        channel, self._channel = self._channel, None
        connection, self._connection = self._connection, None
        if channel is not None:
            with suppress(*BROKER_EXCEPTIONS):
                await channel.close()
        if connection is not None:
            with suppress(*BROKER_EXCEPTIONS):
                await connection.close()

    async def Close(self) -> None:
        async with self._lock:
            await self.Discard()

    async def Publish(self, name: str) -> None:
        message = aio_pika.Message(
            EncodeHydrationMessage(name),
            content_type=HYDRATION_CONTENT_TYPE,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        channel = await self.Connect()
        try:
            await channel.default_exchange.publish(
                message,
                routing_key=self._queue_name,
                timeout=PUBLISH_TIMEOUT_SECONDS,
            )
        except BROKER_EXCEPTIONS as error:
            await self.Close()
            raise BrokerError(
                f"could not publish to '{self._queue_name}': {error}"
            ) from error


def MakeHydrationPublisher(
    nodes: tuple[str, ...],
    user: str,
    password: str,
    vhost: str,
    queue_name: str,
) -> HydrationPublisher | None:
    if not nodes:
        return None
    return HydrationPublisher(
        ParseBrokerNodes(nodes), user, password, vhost, queue_name
    )
