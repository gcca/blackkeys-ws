from __future__ import annotations

import json
import struct
from dataclasses import dataclass

import flatbuffers
import flatbuffers.util
import pylibmc
from argon2 import PasswordHasher, Type
from argon2.exceptions import InvalidHashError, VerificationError

from blackkeys.schemas.UserAuth import (
    UserAuth,
    UserAuthAddPassword,
    UserAuthAddUsername,
    UserAuthEnd,
    UserAuthStart,
)

AUTH_USER_KEY_PREFIX = "auth:user:"
AUTH_CACHE_POOL_SIZE = 4
USER_AUTH_IDENTIFIER = b"BKUA"
EVENTS_LIST_CACHE_KEY = "blackkeys-events-list"
EVENTS_CACHE_POOL_SIZE = 4
password_hasher: PasswordHasher = PasswordHasher(type=Type.ID)


@dataclass(frozen=True, slots=True)
class CachedUserAuth:
    username: str
    password: str


def UserAuthKey(username: str) -> str:
    if not username:
        raise ValueError("username must not be empty")

    key = f"{AUTH_USER_KEY_PREFIX}{username}"
    encoded_key = key.encode()
    if any(byte <= 32 or byte == 127 for byte in encoded_key):
        raise ValueError("username contains unsupported characters")
    if len(encoded_key) > 250:
        raise ValueError("username is too long")
    return key


def EncodeUserAuth(username: str, password: str) -> bytes:
    UserAuthKey(username)
    if not password:
        raise ValueError("password must not be empty")

    builder = flatbuffers.Builder(256)
    username_offset = builder.CreateString(username)
    password_offset = builder.CreateString(password)
    UserAuthStart(builder)
    UserAuthAddUsername(builder, username_offset)
    UserAuthAddPassword(builder, password_offset)
    root = UserAuthEnd(builder)
    builder.Finish(root, file_identifier=USER_AUTH_IDENTIFIER)
    return bytes(builder.Output())


def DecodeUserAuth(
    value: bytes | bytearray | memoryview,
) -> CachedUserAuth | None:
    buffer = bytes(value)
    try:
        if not flatbuffers.util.BufferHasIdentifier(
            buffer, 0, USER_AUTH_IDENTIFIER
        ):
            return None
        user_auth = UserAuth.GetRootAs(buffer)
        username_value = user_auth.Username()
        password_value = user_auth.Password()
        if username_value is None or password_value is None:
            return None
        username = username_value.decode()
        password = password_value.decode()
    except (IndexError, TypeError, UnicodeDecodeError, struct.error):
        return None

    if not username or not password:
        return None
    return CachedUserAuth(username, password)


def VerifyPassword(password: str, encoded: str) -> bool:
    if not encoded.startswith("$argon2id$"):
        return False
    try:
        return password_hasher.verify(encoded, password)
    except (InvalidHashError, VerificationError):
        return False


def MakeAuthCache(
    nodes: tuple[str, ...], pool_size: int = AUTH_CACHE_POOL_SIZE
) -> pylibmc.ClientPool | None:
    if not nodes:
        return None
    if pool_size <= 0:
        raise ValueError("pool_size must be greater than zero")

    client = pylibmc.Client(
        list(nodes),
        binary=True,
        behaviors={
            "ketama": True,
            "num_replicas": max(len(nodes) - 1, 0),
        },
    )
    return pylibmc.ClientPool(client, pool_size)


def ReadUserAuth(
    cache: pylibmc.ClientPool, username: str
) -> CachedUserAuth | None:
    with cache.reserve(block=True) as client:
        value = client.get(UserAuthKey(username))
    if value is None or not isinstance(value, (bytes, bytearray, memoryview)):
        return None
    return DecodeUserAuth(value)


def Authenticate(
    cache: pylibmc.ClientPool, username: str, password: str
) -> CachedUserAuth | None:
    user_auth = ReadUserAuth(cache, username)
    if user_auth is None or user_auth.username != username:
        return None
    if not VerifyPassword(password, user_auth.password):
        return None
    return user_auth


def MakeEventsCache(
    nodes: tuple[str, ...], pool_size: int = EVENTS_CACHE_POOL_SIZE
) -> pylibmc.ClientPool | None:
    if not nodes:
        return None
    if pool_size <= 0:
        raise ValueError("pool_size must be greater than zero")

    client = pylibmc.Client(
        list(nodes),
        binary=True,
        behaviors={
            "ketama": True,
            "num_replicas": max(len(nodes) - 1, 0),
        },
    )
    return pylibmc.ClientPool(client, pool_size)


def ReadEventsList(cache: pylibmc.ClientPool) -> list | None:
    with cache.reserve(block=True) as client:
        value = client.get(EVENTS_LIST_CACHE_KEY)
    if value is None or not isinstance(value, (bytes, bytearray, memoryview)):
        return None
    try:
        decoded = json.loads(bytes(value))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, list):
        return None
    return decoded
