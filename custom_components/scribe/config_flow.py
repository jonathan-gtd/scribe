"""Config flow for Scribe integration.

Supports both UI configuration (async_step_user) and YAML import (async_step_import).
YAML config takes priority: if a config entry already exists it is updated in-place.
"""
import logging

import asyncpg
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import selector

from .const import (
    DOMAIN,
    CONF_DB_URL,
    CONF_RECORD_STATES, DEFAULT_RECORD_STATES,
    CONF_RECORD_EVENTS, DEFAULT_RECORD_EVENTS,
    CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE,
    CONF_FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL,
    CONF_MAX_QUEUE_SIZE, DEFAULT_MAX_QUEUE_SIZE,
    CONF_BUFFER_ON_FAILURE, DEFAULT_BUFFER_ON_FAILURE,
    CONF_INCLUDE_DOMAINS, CONF_INCLUDE_ENTITIES, CONF_INCLUDE_ENTITY_GLOBS,
    CONF_EXCLUDE_DOMAINS, CONF_EXCLUDE_ENTITIES, CONF_EXCLUDE_ENTITY_GLOBS,
    CONF_EXCLUDE_ATTRIBUTES,
)

_LOGGER = logging.getLogger(__name__)


def _normalize_dsn(db_url: str) -> str:
    """Strip SQLAlchemy dialect prefix if present."""
    return db_url.replace("postgresql+asyncpg://", "postgresql://")


async def _test_connection(db_url: str) -> bool:
    """Attempt a real connection to validate the database URL."""
    try:
        conn = await asyncpg.connect(dsn=_normalize_dsn(db_url), timeout=5)
        await conn.close()
        return True
    except Exception as e:
        _LOGGER.debug("Connection test failed: %s", e)
        return False


def _build_options_schema(current: dict) -> vol.Schema:
    """Build the options schema pre-filled with current values."""

    def _get(key, default):
        return current.get(key, default)

    _multi_text = selector.SelectSelectorConfig(
        options=[], multiple=True, custom_value=True,
        mode=selector.SelectSelectorMode.LIST,
    )

    return vol.Schema({
        vol.Optional(CONF_RECORD_STATES, default=_get(CONF_RECORD_STATES, DEFAULT_RECORD_STATES)):
            selector.BooleanSelector(),
        vol.Optional(CONF_RECORD_EVENTS, default=_get(CONF_RECORD_EVENTS, DEFAULT_RECORD_EVENTS)):
            selector.BooleanSelector(),
        vol.Optional(CONF_BATCH_SIZE, default=_get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE)):
            selector.NumberSelector(selector.NumberSelectorConfig(
                min=1, max=10000, step=1, mode=selector.NumberSelectorMode.BOX,
            )),
        vol.Optional(CONF_FLUSH_INTERVAL, default=_get(CONF_FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL)):
            selector.NumberSelector(selector.NumberSelectorConfig(
                min=1, max=300, step=1, mode=selector.NumberSelectorMode.BOX,
            )),
        vol.Optional(CONF_MAX_QUEUE_SIZE, default=_get(CONF_MAX_QUEUE_SIZE, DEFAULT_MAX_QUEUE_SIZE)):
            selector.NumberSelector(selector.NumberSelectorConfig(
                min=100, max=100000, step=100, mode=selector.NumberSelectorMode.BOX,
            )),
        vol.Optional(CONF_BUFFER_ON_FAILURE, default=_get(CONF_BUFFER_ON_FAILURE, DEFAULT_BUFFER_ON_FAILURE)):
            selector.BooleanSelector(),
        vol.Optional(CONF_INCLUDE_DOMAINS, default=_get(CONF_INCLUDE_DOMAINS, [])):
            selector.SelectSelector(_multi_text),
        vol.Optional(CONF_EXCLUDE_DOMAINS, default=_get(CONF_EXCLUDE_DOMAINS, [])):
            selector.SelectSelector(_multi_text),
        vol.Optional(CONF_INCLUDE_ENTITIES, default=_get(CONF_INCLUDE_ENTITIES, [])):
            selector.EntitySelector(selector.EntitySelectorConfig(multiple=True)),
        vol.Optional(CONF_EXCLUDE_ENTITIES, default=_get(CONF_EXCLUDE_ENTITIES, [])):
            selector.EntitySelector(selector.EntitySelectorConfig(multiple=True)),
        vol.Optional(CONF_INCLUDE_ENTITY_GLOBS, default=_get(CONF_INCLUDE_ENTITY_GLOBS, [])):
            selector.SelectSelector(_multi_text),
        vol.Optional(CONF_EXCLUDE_ENTITY_GLOBS, default=_get(CONF_EXCLUDE_ENTITY_GLOBS, [])):
            selector.SelectSelector(_multi_text),
        vol.Optional(CONF_EXCLUDE_ATTRIBUTES, default=_get(CONF_EXCLUDE_ATTRIBUTES, [])):
            selector.SelectSelector(_multi_text),
    })


def _coerce_options(data: dict) -> dict:
    """Convert NumberSelector floats to ints and ensure lists where expected."""
    result = dict(data)
    for key in (CONF_BATCH_SIZE, CONF_FLUSH_INTERVAL, CONF_MAX_QUEUE_SIZE):
        if key in result:
            result[key] = int(result[key])
    # Ensure list fields are actually lists (defensive)
    for key in (
        CONF_INCLUDE_DOMAINS, CONF_EXCLUDE_DOMAINS,
        CONF_INCLUDE_ENTITIES, CONF_EXCLUDE_ENTITIES,
        CONF_INCLUDE_ENTITY_GLOBS, CONF_EXCLUDE_ENTITY_GLOBS,
        CONF_EXCLUDE_ATTRIBUTES,
    ):
        if key in result and not isinstance(result[key], list):
            result[key] = [result[key]] if result[key] else []
    return result


class ScribeConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Scribe."""

    VERSION = 1

    async def async_step_user(self, user_input=None) -> FlowResult:
        """Step 1: ask for the database URL and test the connection."""
        errors = {}

        if user_input is not None:
            db_url = user_input.get(CONF_DB_URL, "").strip()
            if not db_url:
                errors[CONF_DB_URL] = "cannot_connect"
            elif not await _test_connection(db_url):
                errors["base"] = "cannot_connect"
            else:
                await self.async_set_unique_id(DOMAIN)
                self._abort_if_unique_id_configured(updates={CONF_DB_URL: db_url})
                return self.async_create_entry(
                    title="Scribe",
                    data={CONF_DB_URL: db_url},
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_DB_URL): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.URL)
                ),
            }),
            errors=errors,
        )

    async def async_step_import(self, user_input=None) -> FlowResult:
        """Handle import from configuration.yaml (YAML-first setup)."""
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured(updates=user_input)
        return self.async_create_entry(title="Scribe", data=user_input or {})

    @staticmethod
    def async_get_options_flow(config_entry):
        """Return the options flow handler."""
        return ScribeOptionsFlowHandler(config_entry)


class ScribeOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle Scribe runtime options (editable after initial setup)."""

    def __init__(self, config_entry):
        """Store the config entry for later use."""
        self.config_entry = config_entry

    async def async_step_init(self, user_input=None) -> FlowResult:
        """Show and process the options form."""
        # Merge entry data + existing options so current values pre-fill the form
        current = {**self.config_entry.data, **self.config_entry.options}
        errors = {}

        if user_input is not None:
            if not user_input.get(CONF_RECORD_STATES) and not user_input.get(CONF_RECORD_EVENTS):
                errors["base"] = "must_record_something"
            else:
                return self.async_create_entry(title="", data=_coerce_options(user_input))

        return self.async_show_form(
            step_id="init",
            data_schema=_build_options_schema(current),
            errors=errors,
        )
