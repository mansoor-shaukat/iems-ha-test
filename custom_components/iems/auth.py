"""Auth provider — API key → temporary IoT credentials.

Implements the client side of the Cognito Developer-Authenticated
Identities flow specified by the Cloud team in
`cognito_auth_flow.md`. Wiring plan:

  1. User pastes API key (`iems_live_<26>`) into config flow.
  2. HACS POSTs the key to `IEMS_AUTH_URL` → response is
     `{identity_id, open_id_token, expires_at_s, region,
      identity_pool_id, iot_endpoint, user_sub, ...}`.
  3. HACS calls `cognito-identity:GetCredentialsForIdentity` locally
     with the OpenID token to obtain temp AWS creds.
  4. The adapter (iot_core.py) consumes `Credentials` to sign the
     MQTT-over-WSS URL.
  5. Before expiry (T - CREDENTIAL_REFRESH_LEAD_SECONDS), HACS
     re-hits /hacs-auth with the same API key.

Providers:
  IemsAuthProvider       — structural Protocol every provider satisfies
  MockAuthProvider       — canned creds for unit tests (NEVER prod)
  IemsCloudAuthProvider  — real impl; production path
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from .const import (
    API_KEY_LENGTH,
    API_KEY_PREFIX,
    API_KEY_REGEX,
    CREDENTIAL_REFRESH_LEAD_SECONDS,
    IEMS_AUTH_HTTP_TIMEOUT_SECONDS,
    IEMS_AUTH_URL,
)

log = logging.getLogger("iems.auth")

_API_KEY_RE = re.compile(API_KEY_REGEX)


# ============================================================================
# Exceptions
# ============================================================================

class InvalidApiKey(ValueError):
    """Raised when an API key fails format validation (client-side)."""


class AuthExchangeError(RuntimeError):
    """Raised when the API-key → credentials exchange fails.

    Distinguish between permanent (PermanentAuthError) and transient
    (TransientAuthError) via subclasses so callers know whether to
    surface to user vs. back off and retry.
    """


class PermanentAuthError(AuthExchangeError):
    """4xx responses — key invalid, revoked, or malformed body.

    Caller should NOT retry; surface to user via HA config-entry
    auth-repair flow.
    """


class TransientAuthError(AuthExchangeError):
    """5xx or 429 — back off and retry.

    `retry_after` is seconds to wait; obeys the Retry-After header if
    present, else falls back to exponential backoff.
    """

    def __init__(self, message: str, retry_after: float = 30.0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


# ============================================================================
# API key format validation
# ============================================================================

def validate_api_key(key: str | None) -> None:
    """Raise InvalidApiKey if format is wrong.

    Format check only — does NOT check server-side validity. Real
    validation happens at `/hacs-auth` exchange. This runs client-side
    before any network call so a garbage paste never leaves the user's
    machine.
    """
    if not isinstance(key, str):
        raise InvalidApiKey("API key must be a string")
    if len(key) != API_KEY_LENGTH:
        raise InvalidApiKey(
            f"API key length {len(key)} != expected {API_KEY_LENGTH}"
        )
    if not _API_KEY_RE.match(key):
        raise InvalidApiKey("API key format is invalid")


# ============================================================================
# Credentials container — tokens are repr=False so they never leak
# ============================================================================

@dataclass
class Credentials:
    """Temp IoT credentials + routing data from the exchange.

    All token-bearing fields are `repr=False` to prevent accidental
    leaks in log lines using %r, f-string repr(), or default
    dataclass __repr__.
    """
    user_id: str
    iot_endpoint: str
    region: str
    identity_pool_id: str
    identity_id: str
    expires_at: float  # UNIX epoch seconds; earliest of STS + OpenID expiry
    access_key_id: str = field(repr=False, default="")
    secret_access_key: str = field(repr=False, default="")
    session_token: str = field(repr=False, default="")

    def expires_in(self) -> float:
        """Seconds remaining until expiry. Negative = already expired."""
        return self.expires_at - time.time()

    def is_fresh(self, lead_s: float = CREDENTIAL_REFRESH_LEAD_SECONDS) -> bool:
        """True iff creds valid for at least `lead_s` more seconds."""
        return self.expires_in() > lead_s


# ============================================================================
# Provider interface
# ============================================================================

class IemsAuthProvider(Protocol):
    """Every auth provider implements this shape.

    `get_credentials()` must be cheap-when-cached and only hit the
    network when creds are stale per is_fresh().
    """

    async def get_credentials(self) -> Credentials: ...

    async def close(self) -> None: ...


# ============================================================================
# Mock — unit tests only
# ============================================================================

class MockAuthProvider:
    """Canned credentials for unit tests. NEVER used in production.

    Token fields are obvious sentinels (`MOCK_*`) so that if one ever
    leaks into a log line, reviewers can spot it immediately.
    """

    def __init__(
        self,
        *,
        user_id: str = "mock-user-00000000-0000-0000-0000-000000000000",
        iot_endpoint: str = "mock.iot.eu-central-1.amazonaws.com",
        region: str = "eu-central-1",
        identity_pool_id: str = "eu-central-1:mock-pool",
        identity_id: str = "eu-central-1:mock-identity",
        ttl_seconds: float = 3600.0,
    ) -> None:
        self._user_id = user_id
        self._iot_endpoint = iot_endpoint
        self._region = region
        self._identity_pool_id = identity_pool_id
        self._identity_id = identity_id
        self._ttl = ttl_seconds
        self._calls = 0
        self._closed = False

    @property
    def call_count(self) -> int:
        return self._calls

    async def get_credentials(self) -> Credentials:
        self._calls += 1
        return Credentials(
            user_id=self._user_id,
            iot_endpoint=self._iot_endpoint,
            region=self._region,
            identity_pool_id=self._identity_pool_id,
            identity_id=self._identity_id,
            expires_at=time.time() + self._ttl,
            access_key_id="MOCK_ACCESS_KEY",
            secret_access_key="MOCK_SECRET_KEY",
            session_token="MOCK_SESSION_TOKEN",
        )

    async def close(self) -> None:
        self._closed = True


# ============================================================================
# Production provider
# ============================================================================

class IemsCloudAuthProvider:
    """Real auth provider — exchanges API key for temp IoT creds.

    Implements the two-step flow from cognito_auth_flow.md §6:
      1. POST {api_key} to IEMS_AUTH_URL → {identity_id, open_id_token, ...}
      2. boto3 cognito-identity:GetCredentialsForIdentity(identity_id, token)
         → {AccessKeyId, SecretKey, SessionToken, Expiration}

    Caches credentials in memory; only exchanges when stale per
    Credentials.is_fresh(). Thread-safe across concurrent
    get_credentials() calls via an internal asyncio.Lock so we don't
    hammer /hacs-auth when multiple tasks notice expiry simultaneously.

    The API key is stored in a name-mangled private slot and NEVER
    appears in __repr__ / __str__ output.
    """

    def __init__(
        self,
        *,
        api_key: str,
        auth_url: str = IEMS_AUTH_URL,
        http_timeout_s: float = IEMS_AUTH_HTTP_TIMEOUT_SECONDS,
    ) -> None:
        validate_api_key(api_key)
        # Name-mangled so the key never appears in default __dict__ dumps
        self.__api_key = api_key
        self._auth_url = auth_url
        self._http_timeout = http_timeout_s
        self._cached: Credentials | None = None
        self._lock = asyncio.Lock()
        # Lazily import aiohttp inside methods so unit tests can patch
        # without adding aiohttp to the test venv.

    async def get_credentials(self) -> Credentials:
        """Return fresh credentials, exchanging only when stale."""
        async with self._lock:
            if self._cached is not None and self._cached.is_fresh():
                return self._cached
            self._cached = await self._exchange()
            return self._cached

    async def close(self) -> None:
        self._cached = None

    async def _exchange(self) -> Credentials:
        """Full two-step exchange.

        Raises:
          PermanentAuthError on 4xx — caller surfaces to user.
          TransientAuthError on 429/5xx — caller backs off and retries.
        """
        auth_response = await self._post_hacs_auth()
        creds = await self._exchange_openid_for_aws(auth_response)
        return creds

    async def _post_hacs_auth(self) -> dict[str, Any]:
        """Step 1: POST api_key to /hacs-auth, get OpenID token bundle."""
        import aiohttp

        try:
            timeout = aiohttp.ClientTimeout(total=self._http_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self._auth_url,
                    json={"api_key": self.__api_key},
                ) as resp:
                    status = resp.status
                    try:
                        body = await resp.json()
                    except (aiohttp.ContentTypeError, ValueError):
                        body = {"error": "non_json", "message": await resp.text()}
        except asyncio.TimeoutError as exc:
            raise TransientAuthError(
                f"/hacs-auth timed out after {self._http_timeout}s"
            ) from exc
        except aiohttp.ClientError as exc:
            raise TransientAuthError(
                f"/hacs-auth network error: {type(exc).__name__}"
            ) from exc

        # Status-code dispatch per spec §7 Q4
        if status == 200:
            return body
        code = body.get("error", "unknown") if isinstance(body, dict) else "unknown"
        message = (
            body.get("message", "") if isinstance(body, dict) else ""
        )
        if status in (400, 401):
            # Permanent — user must regenerate key. Log at warning WITHOUT
            # exposing the key (the 401 response is deliberately vague).
            log.warning(
                "hacs-auth permanent fail status=%s code=%s",
                status, code,
            )
            raise PermanentAuthError(f"{code}: {message}")
        if status == 429:
            # Rate-limited — back off
            retry_after = self._parse_retry_after(resp, default=30.0)  # type: ignore[name-defined]
            raise TransientAuthError(
                f"rate limited: {message}", retry_after=retry_after,
            )
        # 5xx
        log.warning("hacs-auth 5xx status=%s code=%s", status, code)
        raise TransientAuthError(f"{code}: {message}")

    @staticmethod
    def _parse_retry_after(_resp: Any, *, default: float) -> float:
        """Retry-After header parser (seconds). Falls back to default."""
        try:
            header = _resp.headers.get("Retry-After")
            if header:
                return float(header)
        except (AttributeError, TypeError, ValueError):
            pass
        return default

    async def _exchange_openid_for_aws(
        self, auth_response: dict[str, Any],
    ) -> Credentials:
        """Step 2: boto3 GetCredentialsForIdentity with the OpenID token."""
        try:
            import boto3  # type: ignore[import-not-found]
            from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover
            raise TransientAuthError(
                f"boto3 not available in this HA install: {exc}"
            ) from exc

        required = (
            "identity_id", "open_id_token", "region",
            "identity_pool_id", "iot_endpoint", "user_sub",
        )
        missing = [k for k in required if k not in auth_response]
        if missing:
            # Server-side bug — surface loudly but as transient so we retry
            raise TransientAuthError(
                f"/hacs-auth response missing fields: {missing}"
            )

        def _call() -> dict[str, Any]:
            client = boto3.client("cognito-identity", region_name=auth_response["region"])
            return client.get_credentials_for_identity(
                IdentityId=auth_response["identity_id"],
                Logins={
                    "cognito-identity.amazonaws.com": auth_response["open_id_token"],
                },
            )

        try:
            resp = await asyncio.to_thread(_call)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "ClientError")
            # NotAuthorizedException is permanent (token expired/invalid)
            if code in ("NotAuthorizedException", "InvalidParameterException"):
                raise PermanentAuthError(f"cognito-identity: {code}") from exc
            raise TransientAuthError(f"cognito-identity: {code}") from exc
        except BotoCoreError as exc:  # pragma: no cover
            raise TransientAuthError(f"cognito-identity: {type(exc).__name__}") from exc

        sts = resp.get("Credentials") or {}
        sts_expiration = sts.get("Expiration")
        # STS returns a datetime; spec says take earliest of OpenID + STS expiry
        sts_epoch = (
            sts_expiration.timestamp()
            if hasattr(sts_expiration, "timestamp") else float("inf")
        )
        openid_epoch = float(auth_response.get("expires_at_s", 0.0))
        expires_at = min(sts_epoch, openid_epoch) if openid_epoch > 0 else sts_epoch

        return Credentials(
            user_id=auth_response["user_sub"],
            iot_endpoint=auth_response["iot_endpoint"],
            region=auth_response["region"],
            identity_pool_id=auth_response["identity_pool_id"],
            identity_id=auth_response["identity_id"],
            expires_at=expires_at,
            access_key_id=sts.get("AccessKeyId", ""),
            secret_access_key=sts.get("SecretKey", ""),
            session_token=sts.get("SessionToken", ""),
        )

    def __repr__(self) -> str:
        # Never leak the api_key via repr or f-string %r
        return f"IemsCloudAuthProvider(url={self._auth_url!r}, cached={self._cached is not None})"

    def __str__(self) -> str:
        return self.__repr__()
