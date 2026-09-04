from __future__ import annotations

from dataclasses import dataclass

from aio_pika.exceptions import CONNECTION_EXCEPTIONS, DeliveryError

DEFAULT_MQ_PORT = 5672
CONNECT_TIMEOUT_SECONDS = 5.0
PUBLISH_TIMEOUT_SECONDS = 5.0

BROKER_EXCEPTIONS = (*CONNECTION_EXCEPTIONS, DeliveryError)


class BrokerError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class BrokerNode:
    host: str
    port: int

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


def ParseBrokerNodes(nodes: tuple[str, ...]) -> tuple[BrokerNode, ...]:
    parsed = []
    for node in nodes:
        entry = node.strip()
        if not entry:
            raise ValueError("node must not be empty")

        host, separator, port_value = entry.rpartition(":")
        if not separator:
            host, port_value = entry, str(DEFAULT_MQ_PORT)
        if not host:
            raise ValueError(f"node '{node}' has no host")

        try:
            port = int(port_value)
        except ValueError as error:
            raise ValueError(f"node '{node}' has an invalid port") from error
        if not 1 <= port <= 65535:
            raise ValueError(f"node '{node}' has an out-of-range port")

        parsed.append(BrokerNode(host, port))
    return tuple(parsed)
