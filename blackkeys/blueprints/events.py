import sanic
import sanic.response

from blackkeys.core.conf import settings
from blackkeys.repositories import MakeEventsRepository

blueprint = sanic.Blueprint("events", url_prefix="/events", version=1)
events_repository = MakeEventsRepository(settings)


@blueprint.before_server_start
async def OpenEventsRepository(_: sanic.Sanic) -> None:
    await events_repository.Open()


@blueprint.after_server_stop
async def CloseEventsRepository(_: sanic.Sanic) -> None:
    await events_repository.Close()


@blueprint.get("/list")
async def List(_: sanic.Request) -> sanic.HTTPResponse:
    value = await events_repository.List()
    if value is not None:
        return sanic.response.json(value)

    return sanic.response.json({"error": "events-unavailable"}, status=503)
