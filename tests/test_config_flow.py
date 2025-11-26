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
async def test_flow_user_single_instance(hass):
    """Test that only one instance is allowed."""
    # Mock an existing entry
    mock_entry = config_entries.ConfigEntry(
        version=1,
        minor_version=1,
        domain=DOMAIN,
        title="Scribe",
        data={},
        source=config_entries.SOURCE_USER,
        unique_id=DOMAIN,
        entry_id="mock_entry_id"
    )
    hass.config_entries._entries[mock_entry.entry_id] = mock_entry
    
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] == data_entry_flow.FlowResultType.ABORT
    assert result["reason"] == "single_instance_allowed"

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

@pytest.mark.asyncio
async def test_flow_import_existing_legacy(hass):
    """Test import from YAML when a legacy entry exists (no unique_id)."""
    # Mock an existing legacy entry
    mock_entry = config_entries.ConfigEntry(
        version=1,
        minor_version=1,
        domain=DOMAIN,
        title="Scribe",
        data={},
        source=config_entries.SOURCE_USER,
        unique_id=None, # Legacy entry has no unique_id
        entry_id="legacy_entry_id"
    )
    hass.config_entries._entries[mock_entry.entry_id] = mock_entry
    
    with patch("custom_components.scribe.config_flow.create_engine"), \
         patch.object(hass.config_entries, "async_reload", return_value=None):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_IMPORT},
            data={
                CONF_DB_URL: "postgresql://user:pass@host/db",
                CONF_RECORD_STATES: True,
                CONF_RECORD_EVENTS: True
            }
        )
        
        # Should abort with already_configured, but update the entry
        assert result["type"] == data_entry_flow.FlowResultType.ABORT
        assert result["reason"] == "already_configured"
        
        # Verify entry was updated
        assert mock_entry.unique_id == DOMAIN
