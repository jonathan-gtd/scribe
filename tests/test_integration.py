"""Integration tests for Scribe."""
import os
import pytest
import asyncio
import asyncpg
from datetime import datetime
from unittest.mock import patch
from custom_components.scribe.writer import ScribeWriter

# Get DB URL from env or skip
DB_URL = os.getenv("SCRIBE_INTEGRATION_TEST_DB_URL")

@pytest.fixture(autouse=True)
def disable_mock_pool():
    """Disable the autouse mock_create_pool from conftest.py for integration tests."""
    # We patch it back to the real create_pool
    from asyncpg import create_pool
    with patch("custom_components.scribe.writer.asyncpg.create_pool", side_effect=create_pool) as mock:
        yield mock

@pytest.mark.skipif(not DB_URL, reason="Integration test DB URL not set")
@pytest.mark.asyncio
async def test_integration_write_and_read(hass, socket_enabled):
    """Test writing to a real database and reading back."""
    
    # 1. Setup Writer
    writer = ScribeWriter(
        hass=hass,
        db_url=DB_URL,
        chunk_interval="7 days",
        compress_after="60 days",
        record_states=True,
        record_events=True,
        batch_size=1, # Write immediately
        flush_interval=1,
        max_queue_size=100,
        buffer_on_failure=False,
        table_name_states="states",
        table_name_events="events"
    )
    
    await writer.start()
    
    try:
        # 2. Add entity metadata first! 
        # In scribe 3.0, states_raw joins with entities.
        # Since we bypass HA setup, we must ensure the entity exists in the entities table.
        # ScribeWriter uses _ensure_metadata_ids in _flush, but it needs a real entity record.
        
        # 3. Write Data
        # State
        writer.enqueue({
            "type": "state",
            "entity_id": "sensor.integration_test",
            "state": "123.45",
            "attributes": {"unit": "C"},
            "time": datetime.now(),
            "value": 123.45
        })
        
        # Event
        writer.enqueue({
            "type": "event",
            "event_type": "integration_event",
            "event_data": {"foo": "bar"},
            "time": datetime.now(),
            "origin": "LOCAL",
            "context_id": "ctx_1",
            "context_user_id": "user_1",
            "context_parent_id": "parent_1"
        })
        
        # Wait for flush
        await asyncio.sleep(3)
        
        # 4. Verify with direct DB connection
        conn = await asyncpg.connect(DB_URL)
        
        try:
            # Check States (should work through the view)
            row = await conn.fetchrow("SELECT * FROM states WHERE entity_id = 'sensor.integration_test'")
            assert row is not None
            assert row["state"] == "123.45"
            assert row["value"] == 123.45
            
            # Check Events
            row = await conn.fetchrow("SELECT * FROM events WHERE event_type = 'integration_event'")
            assert row is not None
            # event_data is JSONB in DB, asyncpg returns it as a dict
            assert row["event_data"]["foo"] == "bar"
            
        finally:
            await conn.close()
            
    finally:
        await writer.stop()

@pytest.mark.skipif(not DB_URL, reason="Integration test DB URL not set")
@pytest.mark.asyncio
async def test_integration_compression_setup(hass, socket_enabled):
    """Test that compression policies are correctly set up."""
    writer = ScribeWriter(
        hass=hass,
        db_url=DB_URL,
        chunk_interval="7 days",
        compress_after="60 days",
        record_states=True,
        record_events=True,
        batch_size=10,
        flush_interval=5,
        max_queue_size=100,
        buffer_on_failure=False,
        table_name_states="states",
        table_name_events="events"
    )
    
    await writer.start()
    await writer.stop()
    
    conn = await asyncpg.connect(DB_URL)
    try:
        # Check for compression policies in TimescaleDB jobs
        # In scribe 3.0, we have policies for 'states_raw' and 'events' (or custom name)
        rows = await conn.fetch("SELECT * FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression'")
        
        # We expect at least one for states_raw and one for events
        # Note: hypertable name for states is hardcoded to 'states_raw'
        hypertable_names = [row['hypertable_name'] for row in rows]
        assert "states_raw" in hypertable_names
        assert "events" in hypertable_names
        assert len(rows) >= 2
        
    finally:
        await conn.close()
