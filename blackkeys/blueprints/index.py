import sanic
import sanic.response

blueprint = sanic.Blueprint("index")


@blueprint.route("/healthcheck")
async def Healthcheck(_: sanic.Request) -> sanic.HTTPResponse:
    return sanic.response.text("🍻")
