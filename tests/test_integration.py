"""Integration tests for Scribe."""
import os
import pytest
import asyncio
import asyncpg
import logging
import sys
from datetime import datetime
from unittest.mock import patch
from custom_components.scribe.writer import ScribeWriter

# Configure logging to see DEBUG logs in CI
# We use a higher level here to ensure pytest captures it if configured
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
_LOGGER = logging.getLogger(__name__)

# Get DB URL from env or skip
DB_URL = os.getenv("SCRIBE_INTEGRATION_TEST_DB_URL")

@pytest.fixture(autouse=True)
def mock_create_pool():
    """Definitively override the mock_create_pool from conftest.py with the real one."""
    from asyncpg import create_pool
    # Overriding by name is the most reliable way in pytest to replace a fixture from conftest.py
    with patch("custom_components.scribe.writer.asyncpg.create_pool", side_effect=create_pool) as mock:
        yield mock

async def setup_db_for_integration(url):
    """Ensure extension and basic permissions."""
    _LOGGER.info(f"Connecting to {url} for setup...")
    conn = await asyncpg.connect(url)
    try:
        _LOGGER.info("Enuring timescaledb extension...")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        _LOGGER.info("Granting permissions...")
        await conn.execute("GRANT ALL ON SCHEMA public TO public;")
    except Exception as e:
        _LOGGER.warning(f"DB setup failed: {e}")
    finally:
        await conn.close()

async def log_db_state(url):
    """Log tables, views and entities for debugging failure."""
    conn = await asyncpg.connect(url)
    try:
        _LOGGER.info("--- DATABASE STATE DEBUG ---")
        tables = await conn.fetch("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'")
        _LOGGER.info(f"Tables/Views in DB: {[(t['table_name'], t['table_type']) for t in tables]}")
        
        entities_exists = await conn.fetchval("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'entities')")
        if entities_exists:
            entities = await conn.fetch("SELECT * FROM entities")
            _LOGGER.info(f"Entities in DB: {entities}")
        
        raw_exists = await conn.fetchval("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'states_raw')")
        if raw_exists:
            count = await conn.fetchval("SELECT COUNT(*) FROM states_raw")
            _LOGGER.info(f"states_raw count: {count}")
            
            # Check the view definition
            view_def = await conn.fetchval("SELECT view_definition FROM information_schema.views WHERE table_name = 'states'")
            _LOGGER.info(f"View 'states' definition: {view_def}")
    except Exception as e:
        _LOGGER.error(f"Failed to log DB state: {e}")
    finally:
        await conn.close()

@pytest.mark.skipif(not DB_URL, reason="Integration test DB URL not set")
@pytest.mark.asyncio
async def test_integration_write_and_read(hass, socket_enabled):
    """Test writing to a real database and reading back."""
    _LOGGER.info("Starting test_integration_write_and_read")
    await setup_db_for_integration(DB_URL)
    
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
        # 2. Manually insert entity to be extra sure metadata IDs work
        conn = await asyncpg.connect(DB_URL)
        try:
            _LOGGER.info("Manually inserting test entity...")
            await conn.execute("INSERT INTO entities (entity_id) VALUES ('sensor.integration_test') ON CONFLICT (entity_id) DO NOTHING")
        finally:
            await conn.close()

        # 3. Write Data
        _LOGGER.info("Enqueuing test data...")
        writer.enqueue({
            "type": "state",
            "entity_id": "sensor.integration_test",
            "state": "123.45",
            "attributes": {"unit": "C"},
            "time": datetime.now(),
            "value": 123.45
        })
        
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
        _LOGGER.info("Waiting for flush (8s for CI safety)...")
        await asyncio.sleep(8)
        
        # 4. Verify with direct DB connection
        conn = await asyncpg.connect(DB_URL)
        
        try:
            # Check States
            _LOGGER.info("Verifying states through view...")
            row = await conn.fetchrow("SELECT * FROM states WHERE entity_id = 'sensor.integration_test'")
            if not row:
                await log_db_state(DB_URL)
                assert False, "Relation 'states' returned no row for sensor.integration_test"
            
            assert row["state"] == "123.45"
            assert row["value"] == 123.45
            
            # Check Events
            _LOGGER.info("Verifying events...")
            row = await conn.fetchrow("SELECT * FROM events WHERE event_type = 'integration_event'")
            assert row is not None
            assert row["event_data"]["foo"] == "bar"
            
        except asyncpg.exceptions.UndefinedTableError as e:
            _LOGGER.error(f"UndefinedTableError: {e}")
            await log_db_state(DB_URL)
            raise e
        finally:
            await conn.close()
            
    finally:
        await writer.stop()

@pytest.mark.skipif(not DB_URL, reason="Integration test DB URL not set")
@pytest.mark.asyncio
async def test_integration_compression_setup(hass, socket_enabled):
    """Test that compression policies are correctly set up."""
    await setup_db_for_integration(DB_URL)
    
    writer = ScribeWriter(
        hass=hass,
        db_url=DB_URL,
        chunk_interval="1 day",
        compress_after="1 day",
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
        _LOGGER.info("Checking compression policies in timescaledb_information.jobs...")
        rows = await conn.fetch("SELECT * FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression'")
        hypertable_names = [row['hypertable_name'] for row in rows]
        _LOGGER.info(f"Hypertables with compression: {hypertable_names}")
        
        if "states_raw" not in hypertable_names:
            await log_db_state(DB_URL)
            assert "states_raw" in hypertable_names
            
        assert "events" in hypertable_names
        assert len(rows) >= 2
        
    finally:
        await conn.close()
