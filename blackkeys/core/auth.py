from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import string
import time
from typing import Any


def AsBytes(value: str | bytes) -> bytes:
    return value if isinstance(value, bytes) else value.encode()


def RandomString(length: int = 16) -> str:
    if length <= 0:
        raise ValueError("length must be greater than zero")
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def Message(payload_b64: bytes, salt: str) -> bytes:
    return salt.encode() + b":" + payload_b64


def SignSession(payload: dict[str, Any], secret: str | bytes) -> str:
    salt = RandomString()
    signed_payload = {**payload, "salt": salt}
    payload_bytes = json.dumps(signed_payload, separators=(",", ":")).encode()
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=")
    signature = hmac.new(
        AsBytes(secret), Message(payload_b64, salt), hashlib.sha256
    ).digest()
    signature_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=")
    return f"{payload_b64.decode()}.{signature_b64.decode()}"


def VerifySession(
    token: str,
    secret: str | bytes,
    *,
    timestamp: float | None = None,
) -> dict[str, Any] | None:
    try:
        payload_b64, supplied_signature = token.split(".")
    except (AttributeError, ValueError):
        return None

    padded_payload = payload_b64 + "=" * (-len(payload_b64) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded_payload))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    salt = payload.get("salt")
    if not isinstance(salt, str) or not salt:
        return None

    expected_signature = hmac.new(
        AsBytes(secret),
        Message(payload_b64.encode(), salt),
        hashlib.sha256,
    ).digest()
    expected_signature_b64 = base64.urlsafe_b64encode(
        expected_signature
    ).rstrip(b"=")

    if not hmac.compare_digest(
        supplied_signature.encode(), expected_signature_b64
    ):
        return None

    expires_at = payload.get("exp")
    if (
        not isinstance(expires_at, (int, float))
        or isinstance(expires_at, bool)
        or expires_at < (time.time() if timestamp is None else timestamp)
    ):
        return None

    return payload


HashHmac = SignSession
VerifyHmac = VerifySession
