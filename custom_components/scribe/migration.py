import logging
import asyncio
from datetime import timedelta

import asyncpg

_LOGGER = logging.getLogger(__name__)


async def migrate_database(
    hass,
    pool: asyncpg.Pool,
    record_states: bool,
    enable_table_entities: bool,
    chunk_time_interval: str = "7 days",
    compress_after: str = "7 days"
):
    # Quickly check if migration is finished to skip the 60s delay
    skip_delay = False
    try:
        async with pool.acquire() as conn:
            skip_delay = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'states_raw')"
            )
    except Exception:
        skip_delay = False

    if skip_delay:
        _LOGGER.debug("✅ states_raw detected and migrated, skipping 60s startup delay.")
    else:
        _LOGGER.info("⏳ Migration or initialization needed. Waiting 60 seconds for Home Assistant to complete bootstrap...")
        await asyncio.sleep(60)

    if not skip_delay:
        _LOGGER.info("🔧 Starting background database migration...")

    performed_any = False
    try:
        if record_states and enable_table_entities:
            # 1. Add constraints FIRST (PK/FK on empty table = instant)
            if await _migrate_states_raw_constraints(pool, chunk_time_interval, compress_after):
                performed_any = True

            # 2. Migrate legacy data (chunks, non-blocking)
            if await migrate_states_data(pool):
                performed_any = True

            # 3. Convert to hypertable AFTER data (avoid blocking bootstrap)
            if await _convert_to_hypertable(pool, chunk_time_interval, compress_after):
                performed_any = True

        if await _migrate_events_pk(pool):
            performed_any = True

        if performed_any:
            _LOGGER.info("🎉 All database migrations completed successfully!")

    except Exception as e:
        _LOGGER.error(f"❌ Database migration failed: {e}", exc_info=True)
        _LOGGER.error("   Scribe will continue to work, but some features may be limited.")


async def _migrate_states_raw_constraints(pool: asyncpg.Pool, chunk_time_interval: str = "7 days", compress_after: str = "7 days"):
    """
    Add PRIMARY KEY and FOREIGN KEY constraints to states_raw.
    Idempotent: if constraints already exist, migration is skipped.
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Check if PRIMARY KEY already exists
                pk_exists = await conn.fetchrow("""
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'states_raw_pkey' 
                      AND conrelid = 'states_raw'::regclass
                """)

                if pk_exists:
                    _LOGGER.debug("✅ states_raw: PK already exists, skipping")
                    return False

                _LOGGER.info("📊 states_raw: Checking for duplicate rows...")

                # Count duplicates
                dup_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM (
                        SELECT metadata_id, time
                        FROM states_raw
                        GROUP BY metadata_id, time
                        HAVING COUNT(*) > 1
                    ) AS duplicates
                """) or 0

                if dup_count > 0:
                    _LOGGER.warning(f"⚠️  states_raw: Found {dup_count} duplicate pairs")
                    _LOGGER.info("🧹 states_raw: Cleaning duplicates (keeping last row based on ctid)...")

                    # asyncpg.execute() returns a status string like "DELETE 5"
                    status = await conn.execute("""
                        DELETE FROM states_raw 
                        WHERE ctid NOT IN (
                            SELECT MAX(ctid) 
                            FROM states_raw 
                            GROUP BY metadata_id, time
                        )
                    """)
                    # Parse "DELETE N" to get count
                    deleted_count = int(status.split()[-1]) if status else 0
                    _LOGGER.info(f"✅ states_raw: Cleaned {deleted_count} duplicate rows")
                else:
                    _LOGGER.debug("✅ states_raw: No duplicates found")

                # Add PRIMARY KEY
                _LOGGER.info("🔑 states_raw: Adding PRIMARY KEY (metadata_id, time)...")
                await conn.execute("""
                    ALTER TABLE states_raw 
                    ADD PRIMARY KEY (metadata_id, time)
                """)
                _LOGGER.info("✅ states_raw: PRIMARY KEY added")

                # Add FOREIGN KEY
                _LOGGER.info("🔗 states_raw: Adding FOREIGN KEY (metadata_id → entities.id)...")
                await conn.execute("""
                    ALTER TABLE states_raw 
                    ADD CONSTRAINT fk_states_raw_entity 
                    FOREIGN KEY (metadata_id) 
                    REFERENCES entities(id)
                """)
                _LOGGER.debug("✅ states_raw: FOREIGN KEY added")
                return True

    except Exception as e:
        _LOGGER.error(f"❌ states_raw constraints migration failed: {e}")
        raise e


async def _convert_to_hypertable(pool: asyncpg.Pool, chunk_time_interval: str = "7 days", compress_after: str = "7 days"):
    """
    Convert states_raw to TimescaleDB hypertable and enable compression.
    This is done AFTER data migration to avoid blocking HA bootstrap.
    """
    try:
        async with pool.acquire() as conn:
            # Check if already a hypertable
            already_hypertable = await conn.fetchrow("""
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'states_raw'
            """)

            if not already_hypertable:
                _LOGGER.info(f"🕐 states_raw: Converting to TimescaleDB hypertable (chunk: {chunk_time_interval})...")
                await conn.execute(f"""
                    SELECT create_hypertable('states_raw', 'time', 
                        chunk_time_interval => INTERVAL '{chunk_time_interval}',
                        if_not_exists => TRUE
                    )
                """)
                _LOGGER.debug(f"✅ states_raw: Converted to hypertable ({chunk_time_interval} chunks)")

                # Add compression policy
                _LOGGER.debug(f"🗜️ states_raw: Adding compression policy (compress after {compress_after})...")
                await conn.execute("""
                    ALTER TABLE states_raw SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'metadata_id'
                    )
                """)
                await conn.execute(f"""
                    SELECT add_compression_policy('states_raw', INTERVAL '{compress_after}')
                """)
                _LOGGER.debug(f"✅ states_raw: Compression enabled (after {compress_after})")
                return True
            else:
                _LOGGER.debug("✅ states_raw: Already a hypertable")
                return False

    except Exception as e:
        _LOGGER.warning(f"⚠️ states_raw: Could not convert to hypertable: {e}")
        _LOGGER.info("   (TimescaleDB extension may not be available)")
        return False


async def _migrate_events_pk(pool: asyncpg.Pool):
    """
    Add ID column (PRIMARY KEY) to events table.
    Idempotent: checks if column exists.
    Skips TimescaleDB hypertables (they require time in PK).
    """
    try:
        async with pool.acquire() as conn:
            # Check if events table exists first
            table_exists = await conn.fetchrow("""
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'events'
            """)
            if not table_exists:
                _LOGGER.debug("events table not found (or custom name used), skipping events migration")
                return False

            # Check if it's a TimescaleDB hypertable (skip if yes)
            is_hypertable = await conn.fetchrow("""
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'events'
            """)
            if is_hypertable:
                _LOGGER.debug("ℹ️ events: Skipping PK migration (TimescaleDB hypertable)")
                return False

            # Check if 'id' column already exists
            id_exists = await conn.fetchrow("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'events' AND column_name = 'id'
            """)
            if id_exists:
                _LOGGER.debug("✅ events: ID column already exists, skipping")
                return False

            _LOGGER.info("🔑 events: Adding 'id' BIGSERIAL PRIMARY KEY column...")
            _LOGGER.info("   (This might take a while on large tables)")

            async with conn.transaction():
                await conn.execute("""
                    ALTER TABLE events 
                    ADD COLUMN id BIGSERIAL PRIMARY KEY
                """)

            _LOGGER.debug("✅ events: PRIMARY KEY added successfully")
            return True

    except Exception as e:
        _LOGGER.error(f"❌ events migration failed: {e}")
        # We don't raise here to avoid blocking other migrations if this one fails
        return False


async def migrate_entities_table(conn):
    """Migrate entities table to use SERIAL id as PRIMARY KEY if needed.
    
    Receives an asyncpg connection directly (called from within an open transaction
    in writer.py's _init_entities_table).
    """
    # Check if 'entities' table exists
    table_exists = await conn.fetchval(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'entities')"
    )

    if not table_exists:
        _LOGGER.debug("entities table not found, skipping migration (it will be created)")
        return

    # Check if 'id' column exists
    id_exists = await conn.fetchval(
        "SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'entities' AND column_name = 'id')"
    )

    if not id_exists:
        _LOGGER.warning("⚠️ Detected legacy 'entities' table (text PK). Migrating to SERIAL PK...")

        # 1. Add 'id' column (auto-populates with sequence)
        await conn.execute("ALTER TABLE entities ADD COLUMN id SERIAL")

        # 2. Drop old PK (usually entity_id)
        await conn.execute("ALTER TABLE entities DROP CONSTRAINT IF EXISTS entities_pkey CASCADE")

        # 3. Add new PK
        await conn.execute("ALTER TABLE entities ADD PRIMARY KEY (id)")

        # 4. Add UNIQUE constraint to entity_id (logical key)
        await conn.execute("ALTER TABLE entities ADD CONSTRAINT unique_entity_id UNIQUE (entity_id)")

        _LOGGER.info("✅ Entities table migration completed.")


async def migrate_states_data(pool: asyncpg.Pool):
    """Migrate data from states_legacy to states_raw in chunks."""
    try:
        # Check if 'states_legacy' exists
        async with pool.acquire() as conn:
            legacy_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'states_legacy')"
            )
            if not legacy_exists:
                return False

        _LOGGER.info("📊 states_legacy: Found legacy data. Starting background migration...")

        # 1. Populate Entities (ensure all legacy entities exist)
        async with pool.acquire() as conn:
            async with conn.transaction():
                _LOGGER.info("Populating entities from legacy states...")
                await conn.execute("""
                    INSERT INTO entities (entity_id)
                    SELECT DISTINCT entity_id FROM states_legacy
                    ON CONFLICT (entity_id) DO NOTHING
                """)

        # 2. Migrate Data (query time range first)
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT MIN(time), MAX(time) FROM states_legacy")
            start_time, end_time = row[0], row[1]

        if start_time is None or end_time is None:
            _LOGGER.info("Legacy table is empty.")
        else:
            _LOGGER.info("Copying state history to states_raw (chunked)...")
            chunk_size = timedelta(hours=12)
            current_time = start_time
            total_duration = (end_time - start_time).total_seconds()

            while current_time < end_time:
                next_time = current_time + chunk_size

                # Use a fresh connection + transaction per chunk
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute("""
                            INSERT INTO states_raw (time, metadata_id, state, value, attributes)
                            SELECT 
                                s.time, 
                                e.id, 
                                s.state, 
                                s.value, 
                                s.attributes
                            FROM states_legacy s
                            JOIN entities e ON s.entity_id = e.entity_id
                            WHERE s.time >= $1 AND s.time < $2
                            ON CONFLICT (metadata_id, time) DO NOTHING
                        """, current_time, next_time)

                # Progress logging
                elapsed = (current_time - start_time).total_seconds()
                if total_duration > 0:
                    progress = (elapsed / total_duration) * 100
                    _LOGGER.info(f"Migration progress: {progress:.1f}% (current chunk: {current_time})")

                current_time = next_time
                await asyncio.sleep(0.1)  # Yield to event loop

        # 3. Cleanup
        async with pool.acquire() as conn:
            async with conn.transaction():
                _LOGGER.info("Dropping legacy states table...")
                await conn.execute("DROP TABLE states_legacy")

        _LOGGER.debug("✅ Data migration completed successfully!")
        return True

    except Exception as e:
        _LOGGER.error(f"❌ Data migration failed: {e}")
