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
from typing import Any

from .auth import InvalidApiKey, validate_api_key
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


# ---------------------------------------------------------------------------
# HA-facing ConfigFlow — only wired up if Home Assistant is importable.
# ---------------------------------------------------------------------------
try:
    import voluptuous as vol
    from homeassistant import config_entries
    from homeassistant.data_entry_flow import FlowResult

    STEP_USER_SCHEMA = vol.Schema({vol.Required(CONF_API_KEY): str})

    class IemsConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
        """Handle the iEMS config flow — single step, API key only."""

        VERSION = 1

        async def async_step_user(
            self, user_input: dict[str, Any] | None = None
        ) -> "FlowResult":
            errors: dict[str, str] = {}
            if user_input is not None:
                api_key = user_input[CONF_API_KEY].strip()
                try:
                    validate_api_key(api_key)
                except InvalidApiKey:
                    errors["base"] = "invalid_api_key"
                else:
                    # unique_id derived from the key itself (hash) — we
                    # don't know the real user_id until setup_entry
                    # exchanges the key. Using a hash prevents duplicate
                    # installs of the same key.
                    import hashlib
                    unique_id = hashlib.sha256(api_key.encode()).hexdigest()[:32]
                    await self.async_set_unique_id(unique_id)
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

except ImportError:  # pragma: no cover - dev env without HA
    log.debug("homeassistant not available; ConfigFlow class not registered")
