from __future__ import annotations

import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class Settings:

    AUTH_TTL_SECONDS_DEFAULT: ClassVar[int] = 7 * 24 * 60 * 60
    MQ_USER_DEFAULT: ClassVar[str] = "guest"
    MQ_PASSWORD_DEFAULT: ClassVar[str] = "guest"
    MQ_VHOST_DEFAULT: ClassVar[str] = "/"
    MQ_SIGNUP_QUEUE_DEFAULT: ClassVar[str] = "blackkeys-signup"
    MQ_HYDRATION_QUEUE_DEFAULT: ClassVar[str] = "blackkeys-hydration"

    secret: str
    auth_ttl_seconds: int
    cache_nodes: tuple[str, ...] = ()
    mq_nodes: tuple[str, ...] = ()
    mq_user: str = MQ_USER_DEFAULT
    mq_password: str = MQ_PASSWORD_DEFAULT
    mq_vhost: str = MQ_VHOST_DEFAULT
    mq_signup_queue: str = MQ_SIGNUP_QUEUE_DEFAULT
    mq_hydration_queue: str = MQ_HYDRATION_QUEUE_DEFAULT
    assets_host: str | None = None

    @staticmethod
    def ReadNodes(value: str, name: str) -> tuple[str, ...]:
        nodes = tuple(node.strip() for node in value.split(","))
        if nodes == ("",):
            return ()
        if any(not node for node in nodes):
            raise ValueError(f"{name} contains an empty node")
        return nodes

    @staticmethod
    def FromEnv(environ: Mapping[str, str] | None = None) -> Settings:
        values = os.environ if environ is None else environ
        return Settings(
            secret=_ReadSecret(values),
            auth_ttl_seconds=_ReadAuthTtlSeconds(values),
            cache_nodes=_ReadCacheNodes(values),
            mq_nodes=_ReadMqNodes(values),
            mq_user=_ReadMqUser(values),
            mq_password=_ReadMqPassword(values),
            mq_vhost=_ReadMqVhost(values),
            mq_signup_queue=_ReadMqSignupQueue(values),
            mq_hydration_queue=_ReadMqHydrationQueue(values),
            assets_host=_ReadAssetsHost(values),
        )


def _ReadSecret(values: Mapping[str, str]) -> str:
    secret = values.get("SECRET")
    if secret is None:
        secret = secrets.token_urlsafe(32)
    if not secret:
        raise ValueError("SECRET must not be empty")
    return secret


def _ReadAuthTtlSeconds(values: Mapping[str, str]) -> int:
    try:
        auth_ttl_seconds = int(
            values.get(
                "AUTH_TTL_SECONDS",
                str(Settings.AUTH_TTL_SECONDS_DEFAULT),
            )
        )
    except ValueError as error:
        raise ValueError("AUTH_TTL_SECONDS must be an integer") from error
    if auth_ttl_seconds <= 0:
        raise ValueError("AUTH_TTL_SECONDS must be greater than zero")
    return auth_ttl_seconds


def _ReadCacheNodes(values: Mapping[str, str]) -> tuple[str, ...]:
    return Settings.ReadNodes(values.get("CACHE_NODES", ""), "CACHE_NODES")


def _ReadMqNodes(values: Mapping[str, str]) -> tuple[str, ...]:
    return Settings.ReadNodes(values.get("MQ_NODES", ""), "MQ_NODES")


def _ReadMqUser(values: Mapping[str, str]) -> str:
    return values.get("MQ_USER", Settings.MQ_USER_DEFAULT)


def _ReadMqPassword(values: Mapping[str, str]) -> str:
    return values.get("MQ_PASSWORD", Settings.MQ_PASSWORD_DEFAULT)


def _ReadMqVhost(values: Mapping[str, str]) -> str:
    return values.get("MQ_VHOST", Settings.MQ_VHOST_DEFAULT)


def _ReadMqSignupQueue(values: Mapping[str, str]) -> str:
    mq_signup_queue = values.get(
        "MQ_SIGNUP_QUEUE", Settings.MQ_SIGNUP_QUEUE_DEFAULT
    )
    if not mq_signup_queue:
        raise ValueError("MQ_SIGNUP_QUEUE must not be empty")
    return mq_signup_queue


def _ReadMqHydrationQueue(values: Mapping[str, str]) -> str:
    mq_hydration_queue = values.get(
        "MQ_HYDRATION_QUEUE", Settings.MQ_HYDRATION_QUEUE_DEFAULT
    )
    if not mq_hydration_queue:
        raise ValueError("MQ_HYDRATION_QUEUE must not be empty")
    return mq_hydration_queue


def _ReadAssetsHost(values: Mapping[str, str]) -> str | None:
    return values.get("ASSETS_HOST") or None


settings = Settings.FromEnv()
