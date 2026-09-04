import time

import sanic
import sanic.response

from blackkeys.core.auth import SignSession
from blackkeys.core.conf import settings
from blackkeys.repositories import (
    AuthenticationUnavailable,
    MakeAuthRepository,
    SignupUnavailable,
)

blueprint = sanic.Blueprint("auth", version=1)
auth_repository = MakeAuthRepository(settings)


def ReadCredentials(payload: object) -> tuple[str, str] | None:
    if not isinstance(payload, dict):
        return None

    username = payload.get("username")
    password = payload.get("password")
    if not isinstance(username, str) or not isinstance(password, str):
        return None
    if not username or not password:
        return None
    if not auth_repository.ValidUsername(username):
        return None
    return username, password


@blueprint.before_server_start
async def OpenAuthRepository(_: sanic.Sanic) -> None:
    await auth_repository.Open()


@blueprint.after_server_stop
async def CloseAuthRepository(_: sanic.Sanic) -> None:
    await auth_repository.Close()


@blueprint.post("/signin")
async def SignIn(request: sanic.Request) -> sanic.HTTPResponse:
    credentials = ReadCredentials(request.json)
    if credentials is None:
        return sanic.response.json({"error": "invalid-request"}, status=400)
    username, password = credentials

    try:
        user_auth = await auth_repository.Authenticate(username, password)
    except AuthenticationUnavailable:
        return sanic.response.json(
            {"error": "authentication-unavailable"}, status=503
        )

    if user_auth is None:
        return sanic.response.json({"error": "invalid-credentials"}, status=401)

    issued_at = int(time.time())
    token = SignSession(
        {
            "sub": user_auth.username,
            "iat": issued_at,
            "exp": issued_at + settings.auth_ttl_seconds,
        },
        secret=settings.secret,
    )
    return sanic.response.json({"token": f"blackkeys-v1_{token}"})


@blueprint.post("/signup")
async def SignUp(request: sanic.Request) -> sanic.HTTPResponse:
    credentials = ReadCredentials(request.json)
    if credentials is None:
        return sanic.response.json({"error": "invalid-request"}, status=400)
    username, password = credentials

    try:
        await auth_repository.Signup(username, password)
    except SignupUnavailable:
        return sanic.response.json({"error": "signup-unavailable"}, status=503)

    return sanic.response.json({"status": "accepted"}, status=202)
