"""Test ScribeWriter device writing."""
import pytest
from unittest.mock import MagicMock
from custom_components.scribe.writer import ScribeWriter

@pytest.fixture
def writer(hass):
    """Create a writer instance with a mock engine."""
    mock_engine = MagicMock()
    writer = ScribeWriter(
        hass=hass,
        db_url="postgresql://user:pass@host/db",
        chunk_interval="7 days",
        compress_after="60 days",
        record_states=True,
        record_events=True,
        batch_size=10,
        flush_interval=5,
        max_queue_size=100,
        buffer_on_failure=False,
        table_name_states="states",
        table_name_events="events",
        engine=mock_engine
    )
    return writer

@pytest.mark.asyncio
async def test_write_devices_converts_int_to_str(writer):
    """Test that integer sw_version is converted to string."""
    mock_engine = writer._engine
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn

    devices = [{
        "device_id": "test_device",
        "name": "Test Device",
        "name_by_user": None,
        "model": "Test Model",
        "manufacturer": "Test Mfg",
        "sw_version": 20211110, # Integer version
        "area_id": "test_area",
        "primary_config_entry": "test_entry"
    }]

    await writer.write_devices(devices)

    # Verify that execute was called
    assert mock_conn.execute.called
    
    # Get the arguments passed to execute
    # call_args[0] is positional args (stmt, parameters)
    # call_args[0][1] is the list of parameters (devices)
    call_args = mock_conn.execute.call_args
    params = call_args[0][1]
    
    assert len(params) == 1
    # Check that sw_version is a string
    assert isinstance(params[0]["sw_version"], str)
    assert params[0]["sw_version"] == "20211110"
