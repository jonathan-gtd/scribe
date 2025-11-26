"""Test Scribe sensors."""
import pytest
from unittest.mock import MagicMock
from custom_components.scribe.sensor import (
    ScribeStatesWrittenSensor,
    ScribeEventsWrittenSensor,
    ScribeBufferSizeSensor,
    ScribeWriteDurationSensor,
    ScribeStatesSizeBeforeCompressionSensor,
    ScribeStatesSizeAfterCompressionSensor,
    ScribeEventsSizeBeforeCompressionSensor,
    ScribeEventsSizeAfterCompressionSensor,
    ScribeCompressionRatioSensor
)

@pytest.mark.asyncio
async def test_writer_sensors():
    """Test sensor values directly from writer."""
    writer = MagicMock()
    writer.running = True
    writer._states_written = 10
    writer._events_written = 20
    writer._queue = [1, 2, 3]
    writer._last_write_duration = 0.5
    
    entry = MagicMock()
    entry.entry_id = "test_entry"
    
    # Test States Written
    sensor = ScribeStatesWrittenSensor(writer, entry)
    assert sensor.native_value == 10
    assert sensor.available is True
    
    # Test Events Written
    sensor = ScribeEventsWrittenSensor(writer, entry)
    assert sensor.native_value == 20
    
    # Test Buffer Size
    sensor = ScribeBufferSizeSensor(writer, entry)
    assert sensor.native_value == 3
    
    # Test Write Duration
    sensor = ScribeWriteDurationSensor(writer, entry)
    assert sensor.native_value == 0.5

@pytest.mark.asyncio
async def test_coordinator_sensors():
    """Test coordinator sensors."""
    coordinator = MagicMock()
    coordinator.data = {
        "states_size_bytes": 1000,
        "states_compressed_total_bytes": 200,
        "states_uncompressed_total_bytes": 500,
        "events_size_bytes": 2000,
        "events_compressed_total_bytes": 400,
        "events_uncompressed_total_bytes": 800
    }
    
    entry = MagicMock()
    entry.entry_id = "test_entry"
    
    # Test States Size Before Compression
    # Formula: (Total - Compressed) + Uncompressed = (1000 - 200) + 500 = 1300
    sensor = ScribeStatesSizeBeforeCompressionSensor(coordinator, entry, "key", "name")
    assert sensor.native_value == 1300
    
    # Test States Size After Compression
    sensor = ScribeStatesSizeAfterCompressionSensor(coordinator, entry, "key", "name")
    assert sensor.native_value == 1000

    # Test Events Size Before Compression
    # Formula: (Total - Compressed) + Uncompressed = (2000 - 400) + 800 = 2400
    sensor = ScribeEventsSizeBeforeCompressionSensor(coordinator, entry, "key", "name")
    assert sensor.native_value == 2400
    
    # Test Events Size After Compression
    sensor = ScribeEventsSizeAfterCompressionSensor(coordinator, entry, "key", "name")
    assert sensor.native_value == 2000

@pytest.mark.asyncio
async def test_compression_ratio_sensor():
    """Test compression ratio sensor."""
    coordinator = MagicMock()
    entry = MagicMock()
    entry.entry_id = "test_entry"
    
    sensor = ScribeCompressionRatioSensor(coordinator, entry, "key", "name")
    
    # Case 1: Normal data
    coordinator.data = {
        "states_uncompressed_total_bytes": 1000,
        "states_compressed_total_bytes": 200
    }
    # Ratio = (1 - (200/1000)) * 100 = 80.0%
    assert sensor.native_value == 80.0
    
    # Case 2: No compression (0 bytes)
    coordinator.data = {
        "states_uncompressed_total_bytes": 1000,
        "states_compressed_total_bytes": 0
    }
    # Should return None or handle it? Code says: if not uncompressed or not compressed: return None
    # Wait, if compressed is 0, it returns None?
    # Let's check the code:
    # if not uncompressed or not compressed: return None
    # So if compressed is 0, it returns None.
    assert sensor.native_value is None
    
    # Case 3: Missing data
    coordinator.data = {}
    assert sensor.native_value is None

@pytest.mark.asyncio
async def test_async_setup_entry_statistics(hass):
    """Test setup entry with statistics enabled."""
    from custom_components.scribe.sensor import async_setup_entry
    from custom_components.scribe.const import DOMAIN, CONF_ENABLE_STATISTICS
    
    entry = MagicMock()
    entry.entry_id = "test_entry"
    entry.options = {CONF_ENABLE_STATISTICS: True}
    
    writer = MagicMock()
    coordinator = MagicMock()
    
    hass.data = {DOMAIN: {entry.entry_id: {"writer": writer, "coordinator": coordinator}}}
    
    async_add_entities = MagicMock()
    
    await async_setup_entry(hass, entry, async_add_entities)
    
    async_add_entities.assert_called_once()
    entities = async_add_entities.call_args[0][0]
    # 4 base sensors + 5 stats sensors = 9
    assert len(entities) == 9

@pytest.mark.asyncio
async def test_coordinator_sensors_no_compression():
    """Test coordinator sensors when no compression exists."""
    coordinator = MagicMock()
    coordinator.data = {
        "states_size_bytes": 1000,
        "states_compressed_total_bytes": 0,
        "states_uncompressed_total_bytes": 0,
        "events_size_bytes": 2000,
        "events_compressed_total_bytes": 0,
        "events_uncompressed_total_bytes": 0
    }
    
    entry = MagicMock()
    entry.entry_id = "test_entry"
    
    # Test States Size Before Compression
    # If compressed is 0, return total size (1000)
    sensor = ScribeStatesSizeBeforeCompressionSensor(coordinator, entry, "key", "name")
    assert sensor.native_value == 1000
    
    # Test Events Size Before Compression
    # If compressed is 0, return total size (2000)
    sensor = ScribeEventsSizeBeforeCompressionSensor(coordinator, entry, "key", "name")
    assert sensor.native_value == 2000
