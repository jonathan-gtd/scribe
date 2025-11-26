"""Test Scribe config flow."""
import pytest
from unittest.mock import MagicMock, patch
from homeassistant import config_entries, data_entry_flow
from custom_components.scribe.const import DOMAIN, CONF_DB_URL, CONF_RECORD_STATES, CONF_RECORD_EVENTS

@pytest.mark.asyncio
async def test_flow_user_init(hass):
    """Test the initialization of the user flow."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] == data_entry_flow.FlowResultType.FORM
    assert result["step_id"] == "user"

@pytest.mark.asyncio
async def test_flow_user_valid_input(hass):
    """Test valid user input."""
    with patch("custom_components.scribe.config_flow.create_engine") as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = MagicMock()
        
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER},
            data={
                CONF_DB_URL: "postgresql://user:pass@host/db",
                CONF_RECORD_STATES: True,
                CONF_RECORD_EVENTS: True
            }
        )
        
        assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
        assert result["title"] == "Scribe"
        assert result["data"][CONF_DB_URL] == "postgresql://user:pass@host/db"

@pytest.mark.asyncio
async def test_flow_user_connection_error(hass):
    """Test user input with connection error."""
    with patch("custom_components.scribe.config_flow.create_engine", side_effect=Exception("Connection failed")):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER},
            data={
                CONF_DB_URL: "postgresql://user:pass@host/db",
                CONF_RECORD_STATES: True,
                CONF_RECORD_EVENTS: True
            }
        )
        
        assert result["type"] == data_entry_flow.FlowResultType.FORM
        assert result["errors"]["base"] == "cannot_connect"

@pytest.mark.asyncio
async def test_flow_import(hass):
    """Test import from YAML."""
    with patch("custom_components.scribe.config_flow.create_engine"):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_IMPORT},
            data={
                CONF_DB_URL: "postgresql://user:pass@host/db",
                CONF_RECORD_STATES: True,
                CONF_RECORD_EVENTS: True
            }
        )
        
        assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
        assert result["title"] == "Scribe"
