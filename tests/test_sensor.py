"""Test Scribe sensors."""
import pytest
from unittest.mock import MagicMock
from custom_components.scribe.sensor import (
    ScribeStatesWrittenSensor,
    ScribeEventsWrittenSensor,
    ScribeBufferSizeSensor,
    ScribeWriteDurationSensor,
    ScribeStatesSizeBeforeCompressionSensor
)
from custom_components.scribe.const import DOMAIN

@pytest.mark.asyncio
async def test_sensors():
    """Test sensor values."""
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
        "states_uncompressed_total_bytes": 500
    }
    
    entry = MagicMock()
    entry.entry_id = "test_entry"
    
    # Test States Size Before Compression
    # Formula: (Total - Compressed) + Uncompressed = (1000 - 200) + 500 = 1300
    sensor = ScribeStatesSizeBeforeCompressionSensor(coordinator, entry, "key", "name")
    assert sensor.native_value == 1300
