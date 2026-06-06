"""Config flow for the iEMS HACS integration.

Single-step UI: user pastes an API key from the iEMS portal. We format-
validate it locally (rejecting obvious garbage before any network call),
then the full credential exchange happens inside async_setup_entry via
the auth provider.

Pure helpers (`validate_api_key`, `derive_user_id_placeholder`) are
importable without Home Assistant installed for unit tests. The HA
ConfigFlow class is guarded behind an import-try so the module stays
importable on dev machines without HA.
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from .auth import (
    IemsCloudAuthProvider,
    InvalidApiKey,
    validate_api_key,
)
from .const import CONF_API_KEY, DOMAIN

log = logging.getLogger("iems.config_flow")


def derive_user_id_placeholder(_api_key: str) -> str:
    """Placeholder used before the real credential exchange happens.

    The real user_id comes from `auth.get_credentials().user_id` after
    the server exchange. During config flow we don't do that exchange
    (requires network, HA wants config flow to be snappy) — we store
    the API key and let `async_setup_entry` exchange it later.
    """
    return ""  # empty → filled in at setup_entry


def _is_legacy_unique_id(uid: str | None) -> bool:
    """True if a stored config-entry unique_id predates the v0.4.3 scheme.

    v0.4.3+ stores the resolved Cognito ``identity_id`` as the entry
    unique_id. That value ALWAYS contains a ``:`` (region:uuid form, e.g.
    ``eu-central-1:066de039-2251-cce1-...``). Earlier schemes never did:

      * 0.4.2  — ``sha256(api_key)[:32]`` → 32-char lowercase hex, no colon.
      * <=0.4.1 — ``derive_user_id(api_key)`` → a 36-char UUID, no colon.

    The colon is therefore a reliable, scheme-agnostic discriminator: a
    legacy entry is any non-empty unique_id WITHOUT a colon. We branch on
    this (not on length / charset) so any future identity format that keeps
    the ``region:...`` shape is still recognised as non-legacy.

    Pure + HA-free so the reauth/migration decision is unit-testable without
    Home Assistant (mirrors the `validate_api_key` / `resolve_account_identity`
    pattern at the top of this module).
    """
    return bool(uid) and ":" not in uid


# Reauth-confirm decision outcomes — what the flow does once the new key has
# been exchanged and proved valid (so `new_identity` is known).
REAUTH_MIGRATE_LEGACY = "migrate_legacy"   # accept valid key, migrate unique_id
REAUTH_STRICT_IDENTITY = "strict_identity"  # enforce same-account guard


def decide_reauth_action(stored_unique_id: str | None) -> str:
    """Decide how reauth handles a valid new key given the entry's stored uid.

    Pure decision split, HA-free so it is unit-testable without Home Assistant:

      * stored uid is LEGACY (no colon — a sha256 hash or derived UUID from
        <=0.4.2) → ``REAUTH_MIGRATE_LEGACY``: the stored value is a one-way
        hash, NOT reversible to an account identity, so same-account cannot be
        proven. Accept the (already-validated) new key and migrate unique_id to
        its resolved identity. SECURITY trade-off: any valid key is accepted for
        a legacy entry (it is the user's own reauth on their own install).
      * stored uid is IDENTITY-format (contains a colon) → ``REAUTH_STRICT_IDENTITY``:
        keep the strict same-account guard — set unique_id to the new identity
        then abort on mismatch, so reauth can't repoint the entry at a different
        account.

    The HA `async_step_reauth_confirm` method is a thin wrapper over this
    decision (the HA flow APIs — async_set_unique_id / _abort_if_unique_id_mismatch
    / async_update_reload_and_abort — aren't importable in the unit test env, so
    the branch logic lives here where it can be tested directly).
    """
    if _is_legacy_unique_id(stored_unique_id):
        return REAUTH_MIGRATE_LEGACY
    return REAUTH_STRICT_IDENTITY


# Type of the provider factory: `IemsCloudAuthProvider(api_key=...)`.
ProviderFactory = Callable[..., Any]


async def resolve_account_identity(
    api_key: str,
    *,
    provider_factory: ProviderFactory = IemsCloudAuthProvider,
) -> str:
    """Exchange an API key for the ACCOUNT identity it belongs to.

    Returns the Cognito ``identity_id`` — the stable, account-scoped
    identifier the IoT policy and MQTT ClientId key on. It is invariant
    across a key rotation (a re-minted key for the same account resolves
    to the same identity) and across DAI credential refresh.

    This is the linchpin of the same-account guard. unique_id is derived
    from the RESOLVED ACCOUNT IDENTITY, not from the key string, so:
      * a different KEY for the SAME account → same identity → reauth passes,
      * a key for a DIFFERENT account → different identity → mismatch aborts.

    Raises:
      InvalidApiKey      — format check failed (local, no network).
      PermanentAuthError — key revoked/invalid/wrong (4xx exchange). Caller
                           maps to the ``invalid_api_key`` form error.
      TransientAuthError — 5xx/429/network. Caller should retry, NOT treat
                           as an invalid key.

    `provider_factory` is injected so tests can supply a controlled
    identity without network/cert/boto3. Production uses the default
    `IemsCloudAuthProvider`.
    """
    # Local format validation first — a garbage paste never leaves the box.
    validate_api_key(api_key)

    provider = provider_factory(api_key=api_key)
    try:
        creds = await provider.get_credentials()
        return creds.identity_id
    finally:
        # Don't leak the aiohttp session / cached creds, on success or failure.
        close: Callable[[], Awaitable[None]] | None = getattr(
            provider, "close", None
        )
        if close is not None:
            await close()


# ---------------------------------------------------------------------------
# HA-facing ConfigFlow — only wired up if Home Assistant is importable.
# ---------------------------------------------------------------------------
try:
    import voluptuous as vol
    from homeassistant import config_entries
    from homeassistant.data_entry_flow import FlowResult

    from .auth import AuthExchangeError, PermanentAuthError, TransientAuthError

    STEP_USER_SCHEMA = vol.Schema({vol.Required(CONF_API_KEY): str})

    class IemsConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
        """Handle the iEMS config flow — single step, API key only.

        unique_id is the RESOLVED ACCOUNT IDENTITY (Cognito ``identity_id``),
        obtained by exchanging the pasted key. Using the account identity
        rather than a hash of the key string means:
          * duplicate-install prevention is account-scoped (a second key for
            an already-installed account is correctly rejected), and
          * reauth with a rotated key for the same account succeeds.
        """

        VERSION = 1

        async def _resolve_or_form_error(
            self, api_key: str, errors: dict[str, str], step_id: str
        ) -> "tuple[str | None, FlowResult | None]":
            """Resolve account identity, mapping failures to form errors.

            Returns ``(identity_id, None)`` on success, or
            ``(None, <form>)`` with the error already attached:
              * malformed paste → invalid_api_key
              * server-rejected (revoked/wrong) key → auth_failed
              * cloud unreachable / 5xx / 429 → cannot_connect (retryable)
            """
            try:
                identity_id = await resolve_account_identity(api_key)
            except InvalidApiKey:
                # Malformed paste — fails locally before any network call.
                errors["base"] = "invalid_api_key"
            except PermanentAuthError:
                # Format was fine but the server rejected it (revoked / wrong /
                # unrecognised key). Distinct copy from the format error.
                errors["base"] = "auth_failed"
            except (TransientAuthError, AuthExchangeError, OSError, TimeoutError):
                # 5xx / 429 / network — transient, let the user retry.
                errors["base"] = "cannot_connect"
            else:
                return identity_id, None
            return None, self.async_show_form(
                step_id=step_id,
                data_schema=STEP_USER_SCHEMA,
                errors=errors,
            )

        async def async_step_user(
            self, user_input: dict[str, Any] | None = None
        ) -> "FlowResult":
            errors: dict[str, str] = {}
            if user_input is not None:
                api_key = user_input[CONF_API_KEY].strip()
                identity_id, form = await self._resolve_or_form_error(
                    api_key, errors, step_id="user"
                )
                if form is not None:
                    return form
                # identity_id is the account identity — dedupe installs per
                # account, not per key string.
                await self.async_set_unique_id(identity_id)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title="iEMS",
                    data={CONF_API_KEY: api_key},
                )

            return self.async_show_form(
                step_id="user",
                data_schema=STEP_USER_SCHEMA,
                errors=errors,
            )

        async def async_step_reauth(
            self, entry_data: dict[str, Any]
        ) -> "FlowResult":
            """Entry point HA calls when async_setup_entry raises
            ConfigEntryAuthFailed (key revoked / rotated / expired).

            Stores nothing here — just routes to the confirm step that
            re-prompts for the key. `entry_data` is the existing entry's data
            (unused; we only need a fresh key).
            """
            return await self.async_step_reauth_confirm()

        async def async_step_reauth_confirm(
            self, user_input: dict[str, Any] | None = None
        ) -> "FlowResult":
            """Re-prompt for the API key in place and update the existing entry.

            Same key-only schema as async_step_user. On a valid key we update
            the existing config entry's data + reload it, so the user never has
            to delete and re-add the integration after a key rotation.
            """
            errors: dict[str, str] = {}
            # The entry being re-authenticated (modern HA gives us this helper).
            reauth_entry = self.hass.config_entries.async_get_entry(
                self.context["entry_id"]
            )
            if user_input is not None:
                api_key = user_input[CONF_API_KEY].strip()
                identity_id, form = await self._resolve_or_form_error(
                    api_key, errors, step_id="reauth_confirm"
                )
                if form is not None:
                    return form

                stored_uid = reauth_entry.unique_id if reauth_entry else None
                action = decide_reauth_action(stored_uid)

                if action == REAUTH_MIGRATE_LEGACY:
                    # SECURITY: legacy entries (created <=0.4.2) store a one-way
                    # sha256(api_key)[:32] hash or a derived UUID — neither is
                    # reversible to an account identity, so we CANNOT prove the
                    # new key belongs to the same account the way we can for an
                    # identity-format entry. We therefore RELAX the same-account
                    # guard for legacy entries ONLY: a successful key exchange
                    # above already proved the new key is VALID, and this is the
                    # user's own reauth on their own install (the common path is
                    # "user revoked the old key, then pasted a fresh one for the
                    # same account"). We accept the valid key and MIGRATE the
                    # unique_id to the resolved identity_id, so every subsequent
                    # reauth runs the strict identity-format path. The strict
                    # guard is preserved untouched for identity-format entries —
                    # this relaxation can never weaken an entry that already
                    # carries a colon-form identity.
                    await self.async_set_unique_id(identity_id)
                    return self.async_update_reload_and_abort(
                        reauth_entry,
                        data={CONF_API_KEY: api_key},
                    )

                # REAUTH_STRICT_IDENTITY — identity-format entry (contains a
                # colon): keep the entry's identity stable across a re-key —
                # assert the new key resolves to the SAME ACCOUNT (same
                # identity_id), so reauth can't silently repoint the entry at a
                # different account. A rotated key for the same account resolves
                # to the same identity → passes. A different-account key →
                # mismatch abort (the "remove and re-add" escape hatch stays
                # valid).
                await self.async_set_unique_id(identity_id)
                self._abort_if_unique_id_mismatch(reason="reauth_account_mismatch")
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data={CONF_API_KEY: api_key},
                )

            return self.async_show_form(
                step_id="reauth_confirm",
                data_schema=STEP_USER_SCHEMA,
                errors=errors,
            )

except ImportError:  # pragma: no cover - dev env without HA
    log.debug("homeassistant not available; ConfigFlow class not registered")
