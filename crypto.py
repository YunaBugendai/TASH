"""
Lightweight local-network authentication.

This is NOT meant to defend against a sophisticated network attacker
sitting on the same segment; it is meant to stop accidental or casual
unauthorized connections. It works in two steps:

  1. Pairing code: the Master generates a short numeric code and shows
     it on screen. A Worker operator must read that code (out loud, over
     chat, whatever) and type it in before the Master will even show an
     authorization prompt. This proves the Worker operator is someone
     the Master operator actually talked to.

  2. Session token: once the Master operator explicitly approves a
     Worker, the Master issues a random per-worker token. The Worker
     can use this token to reconnect quietly after a network blip
     without bothering the Master operator again -- but a stranger
     who doesn't have the token cannot impersonate that worker_id.
"""
import hmac
import hashlib
import secrets

from shared.constants import PAIRING_CODE_LENGTH, TOKEN_BYTES


def generate_pairing_code() -> str:
    """A short numeric code the Master operator reads out to Worker operators."""
    return "".join(secrets.choice("0123456789") for _ in range(PAIRING_CODE_LENGTH))


def generate_session_token() -> str:
    """A per-worker secret issued after successful, explicit pairing."""
    return secrets.token_hex(TOKEN_BYTES)


def sign(token: str, message: str) -> str:
    return hmac.new(token.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def verify(token: str, message: str, signature: str) -> bool:
    return hmac.compare_digest(sign(token, message), signature)
