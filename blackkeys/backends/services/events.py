from __future__ import annotations

import httpx2
from sanic.log import logger

ASSETS_EVENTS_PATH = "/events"
ASSETS_TIMEOUT_SECONDS = 5.0


class EventsService:
    __slots__ = ("_host",)

    def __init__(self, host: str) -> None:
        if not host:
            raise ValueError("host must not be empty")
        self._host = host

    async def List(self) -> list | None:
        try:
            async with httpx2.AsyncClient(
                base_url=self._host,
                timeout=ASSETS_TIMEOUT_SECONDS,
            ) as client:
                response = await client.get(ASSETS_EVENTS_PATH)
                response.raise_for_status()
                payload = response.json()
        except (httpx2.HTTPError, ValueError) as error:
            logger.warning("assets service request failed: %s", error)
            return None

        if not isinstance(payload, list):
            return None
        return payload


def MakeEventsService(host: str | None) -> EventsService | None:
    if not host:
        return None
    return EventsService(host)
