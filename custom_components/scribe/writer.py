"""Database writer for Scribe.

This module handles the asynchronous writing of data to the TimescaleDB database.
It implements an asyncio-based writer that buffers events and writes them in batches
to minimize database connection overhead and blocking.

NOTE: This version uses asyncpg directly (no SQLAlchemy) to avoid the greenlet
dependency which is not available on Python 3.14 / Alpine Linux (Home Assistant OS).
"""
import logging
import asyncio
import ssl
import time
from pathlib import Path
from typing import Any, Dict
from collections import deque
import json
import math

import asyncpg

from homeassistant.helpers.json import JSONEncoder
from homeassistant.core import HomeAssistant

# NOTE: 'migration' is imported lazily (inside methods) to avoid circular imports.
# __init__.py imports ScribeWriter, so a top-level 'from . import migration' here
# would trigger a circular import before the package is fully initialized.

_LOGGER = logging.getLogger(__name__)


def _create_ssl_context(ssl_root_cert=None, ssl_cert_file=None, ssl_key_file=None) -> ssl.SSLContext:
    """Create and configure SSL context in executor thread.
    
    asyncpg calls ssl.load_cert_chain() synchronously when establishing SSL connections.
    By creating the SSLContext here (in an executor thread) and passing it to asyncpg,
    we avoid blocking Home Assistant's event loop.
    
    This function must be run via hass.async_add_executor_job().
    
    Returns:
        Configured SSLContext ready to be used by asyncpg
    """
    _LOGGER.debug("Creating SSL context in executor thread...")
    
    # Create SSL context
    ssl_context = ssl.create_default_context()
    
    # Load system CA certificates
    try:
        ssl_context.load_default_certs()
        _LOGGER.debug("Loaded system CA certificates")
    except Exception as e:
        _LOGGER.debug("Could not load system CA certificates: %s", e)
    
    # Load PostgreSQL client certificates
    if ssl_cert_file and Path(ssl_cert_file).exists():
        try:
            _LOGGER.debug("Loading PostgreSQL client certificate from %s", ssl_cert_file)
            ssl_context.load_cert_chain(ssl_cert_file, ssl_key_file)
        except Exception as e:
            _LOGGER.error("Could not load cert chain from %s: %s", ssl_cert_file, e)
    
    # Load CA certificate for server verification
    if ssl_root_cert and Path(ssl_root_cert).exists():
        try:
            _LOGGER.debug("Loading CA certificate from %s", ssl_root_cert)
            ssl_context.load_verify_locations(ssl_root_cert)
        except Exception as e:
            _LOGGER.error("Could not load CA cert from %s: %s", ssl_root_cert, e)
    
    _LOGGER.debug("SSL context created successfully")
    return ssl_context


def _normalize_dsn(db_url: str) -> str:
    """Convert SQLAlchemy-style DSN to plain asyncpg DSN.
    
    asyncpg uses postgresql:// (or postgres://), not postgresql+asyncpg://.
    """
    return db_url.replace("postgresql+asyncpg://", "postgresql://")


class ScribeWriter:
    """Handle database connections and writing.
    
    This class runs as an asyncio task. It maintains a queue of events to be written.
    Data is flushed to the database when the queue reaches BATCH_SIZE or when
    FLUSH_INTERVAL seconds have passed.
    
    Uses asyncpg directly (no SQLAlchemy) to avoid the greenlet dependency.
    """

    def __init__(
        self, 
        hass: HomeAssistant, 
        db_url: str, 
        chunk_interval: str, 
        compress_after: str, 
        record_states: bool, 
        record_events: bool, 
        batch_size: int, 
        flush_interval: int, 
        max_queue_size: int, 
        buffer_on_failure: bool, 
        table_name_states: str, 
        table_name_events: str,
        use_ssl: bool = False,
        ssl_root_cert: str = None,
        ssl_cert_file: str = None,
        ssl_key_file: str = None,
        enable_areas: bool = True,
        enable_devices: bool = True,
        enable_table_areas: bool = True,
        enable_table_devices: bool = True,
        enable_table_entities: bool = True,
        enable_table_integrations: bool = True,
        enable_table_users: bool = True,
        engine: Any = None
    ):
        """Initialize the writer."""
        self.hass = hass
        
        # Normalize DSN - strip SQLAlchemy dialect prefix if present
        self.db_url = _normalize_dsn(db_url)
            
        self.chunk_interval = chunk_interval
        self.compress_after = compress_after
        self.record_states = record_states
        self.record_events = record_events
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        self.buffer_on_failure = buffer_on_failure
        self.table_name_states = table_name_states
        self.table_name_events = table_name_events
        self.use_ssl = use_ssl
        self.ssl_root_cert = ssl_root_cert
        self.ssl_cert_file = ssl_cert_file
        self.ssl_key_file = ssl_key_file
        self.enable_table_areas = enable_table_areas
        self.enable_table_devices = enable_table_devices
        self.enable_table_entities = enable_table_entities
        self.enable_table_integrations = enable_table_integrations
        self.enable_table_users = enable_table_users
        
        # Stats for sensors
        self._states_written = 0
        self._events_written = 0
        self._last_write_duration = None
        self._connected = False
        self._last_error = None
        self._states_history = deque()
        self._events_history = deque()
        self._dropped_events = 0
        
        # Queue
        self._queue: deque = deque(maxlen=max_queue_size)
        self._flush_pending = False  # Prevent multiple flush tasks
        
        # asyncpg connection pool (replaces SQLAlchemy engine)
        self._pool: asyncpg.Pool = None
        # Keep _engine as alias for migration.py compatibility
        self._engine = None
        
        self._task = None
        self._running = False
        
        # ID Cache: entity_id -> metadata_id
        self._entity_id_map: Dict[str, int] = {}
        # Reverse Cache: metadata_id -> entity_id (for debugging/renames if needed)
        self._metadata_id_map: Dict[int, str] = {}

    # ------------------------------------------------------------------
    # Internal helpers: acquire connection with/without transaction
    # ------------------------------------------------------------------

    async def _execute(self, sql: str, *args):
        """Execute a statement (no return value needed) using a pooled connection."""
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *args)

    async def _execute_many(self, sql: str, args_list: list):
        """Execute a statement for each row in args_list inside a transaction."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, args_list)

    async def _fetchval(self, sql: str, *args):
        """Fetch a single scalar value."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval(sql, *args)

    async def _fetchrow(self, sql: str, *args):
        """Fetch a single row."""
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def _fetch(self, sql: str, *args):
        """Fetch all rows."""
        async with self._pool.acquire() as conn:
            return await conn.fetch(sql, *args)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Start the writer task."""
        try:
            if self._running:
                return
                
            _LOGGER.debug("Starting ScribeWriter...")
            self._running = True
            
            # Create connection pool
            if not self._pool:
                try:
                    _LOGGER.debug(f"Creating asyncpg pool for {self.db_url.split('@')[-1]}")
                    
                    ssl_arg = False  # default: no SSL
                    if self.use_ssl:
                        # Resolve paths relative to HA config dir if they are relative
                        def resolve_path(path_str):
                            if not path_str:
                                return None
                            path = Path(path_str)
                            if not path.is_absolute():
                                return str(Path(self.hass.config.config_dir) / path)
                            return str(path)

                        root_cert = resolve_path(self.ssl_root_cert)
                        cert_file = resolve_path(self.ssl_cert_file)
                        key_file = resolve_path(self.ssl_key_file)

                        # Create SSL context in executor to avoid blocking the event loop
                        _LOGGER.debug("SSL enabled, creating SSL context in executor...")
                        ssl_arg = await self.hass.async_add_executor_job(
                            _create_ssl_context, 
                            root_cert, 
                            cert_file, 
                            key_file
                        )

                    self._pool = await asyncpg.create_pool(
                        dsn=self.db_url,
                        min_size=1,
                        max_size=10,
                        ssl=ssl_arg,
                    )
                    # Expose pool as _engine so migration.py can use it
                    self._engine = self._pool

                    _LOGGER.debug("asyncpg pool created successfully")
                except Exception as e:
                    _LOGGER.error(f"Failed to create pool: {e}", exc_info=True)
                    return

            # Perform initialization
            try:
                _LOGGER.debug("Starting database initialization...")
                await self.init_db()
                _LOGGER.debug("Database initialization completed")
                
                # Loop launch (background)
                self._task = asyncio.create_task(self._run())
                _LOGGER.info("ScribeWriter started successfully")
            except Exception as e:
                _LOGGER.error(f"Initialization failed: {e}", exc_info=True)
                self._connected = False
                raise e

        except Exception as e:
            _LOGGER.error(f"Unexpected error starting ScribeWriter: {e}", exc_info=True)
            raise e

    async def _get_initial_counts(self):
        """Fetch initial row counts from database."""
        _LOGGER.debug("Fetching initial row counts...")
        try:
            if self.record_states:
                self._states_written = await self._fetchval(f"SELECT count(*) FROM {self.table_name_states}") or 0
            
            if self.record_events:
                self._events_written = await self._fetchval(f"SELECT count(*) FROM {self.table_name_events}") or 0
                    
            _LOGGER.debug(f"Initial counts: states={self._states_written}, events={self._events_written}")
        except Exception as e:
            _LOGGER.warning(f"Failed to fetch initial counts: {e}")

    async def _ensure_metadata_ids(self, entity_ids: list[str]):
        """Ensure all entity_ids have a metadata_id in the cache."""
        missing = [eid for eid in entity_ids if eid not in self._entity_id_map]
        if not missing:
            return

        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # Insert missing entities
                    await conn.executemany(
                        "INSERT INTO entities (entity_id) VALUES ($1) ON CONFLICT (entity_id) DO NOTHING",
                        [(eid,) for eid in missing]
                    )
                    
                    # Fetch IDs for the missing ones
                    rows = await conn.fetch(
                        "SELECT entity_id, id FROM entities WHERE entity_id = ANY($1)",
                        missing
                    )
                    
                    count = 0
                    for row in rows:
                        self._entity_id_map[row['entity_id']] = row['id']
                        self._metadata_id_map[row['id']] = row['entity_id']
                        count += 1
                    
                    if count > 0:
                        _LOGGER.debug(f"Registered {count} new entities")
                    
        except Exception as e:
            _LOGGER.error(f"Error registering new entities: {e}")

    async def stop(self):
        """Stop the writer task."""
        _LOGGER.debug("Stopping ScribeWriter...")
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                _LOGGER.error(f"Error waiting for writer task to stop: {e}", exc_info=True)
        
        # Final flush
        try:
            await self._flush()
        except Exception as e:
            _LOGGER.error(f"Error during final flush: {e}", exc_info=True)
        
        if self._pool:
            try:
                await self._pool.close()
                self._pool = None
                self._engine = None
                _LOGGER.debug("Pool closed")
            except Exception as e:
                 _LOGGER.error(f"Error closing pool: {e}", exc_info=True)

    async def _run(self):
        """Main loop."""
        _LOGGER.debug("ScribeWriter loop started")
        
        # 1. Register listener to launch migration AFTER HA finishes bootstrap
        async def _launch_migration(event):
            """Launch migration after HA is fully started."""
            _LOGGER.debug("HA fully started, launching background migration task")
            try:
                from . import migration  # lazy import to avoid circular dependency
                await migration.migrate_database(
                    self.hass, 
                    self._pool,   # pass pool (migration.py uses it as 'engine')
                    self.record_states, 
                    self.enable_table_entities,
                    self.chunk_interval,
                    self.compress_after
                )
                
                # Create the view now that migration is done (the table was renamed/dropped)
                async with self._pool.acquire() as conn:
                    await self._init_states_view(conn)
                    
            except Exception as e:
                _LOGGER.error(f"Background migration failed: {e}", exc_info=True)
        
        from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
        self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _launch_migration)

        # 2. Fetch initial counts (background - might take a while on large DBs)
        try:
            await self._get_initial_counts()
        except Exception as e:
             _LOGGER.warning(f"Failed to fetch initial (background) counts: {e}")

        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOGGER.error(f"Error in writer loop: {e}", exc_info=True)
                # Prevent tight loop if persistent error
                await asyncio.sleep(5)

    def enqueue(self, data: Dict[str, Any]):
        """Add data to the queue.
        
        This is called from the main loop, so it shouldn't block.
        We use deque with maxlen, so old items are automatically dropped if full.
        """
        try:
            self._queue.append(data)
            
            # Trigger flush if batch size reached (but only if no flush is already pending)
            if len(self._queue) >= self.batch_size and not self._flush_pending:
                self._flush_pending = True
                _LOGGER.debug(f"Batch size reached ({len(self._queue)} >= {self.batch_size}), triggering flush")
                asyncio.create_task(self._flush())
        except Exception as e:
            _LOGGER.error(f"Error enqueuing data: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Database initialisation
    # ------------------------------------------------------------------

    async def init_db(self):
        """Initialize database tables."""
        _LOGGER.debug("Initializing database...")
        if not self._pool:
            return

        try:
            # 1. Check and Perform Migration (own transaction)
            if self.record_states:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await self._check_and_migrate_states(conn)
            
            # 2. Create tables (own transaction)
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # Initialize entities FIRST (states view depends on it)
                    if self.enable_table_entities:
                        await self._init_entities_table(conn)
                    
                    # Always init users table
                    if self.enable_table_users:
                        await self._init_users_table(conn)
                    
                    if self.enable_table_areas:
                        await self._init_areas_table(conn)
                    if self.enable_table_devices:
                        await self._init_devices_table(conn)
                    if self.enable_table_integrations:
                        await self._init_integrations_table(conn)
                    
                    # Initialize states and events AFTER entities table exists
                    if self.record_states:
                        await self._init_states_table(conn)
                    if self.record_events:
                        await self._init_events_table(conn)

            # Hypertable & Compression (each operation in its own transaction)
            if self.record_states:
                try:
                    await self._init_hypertable("states_raw", "metadata_id")
                except Exception as e:
                    _LOGGER.error(f"Failed to init hypertable/compression for states: {e}", exc_info=True)
            
            if self.record_events:
                try:
                    await self._init_hypertable(self.table_name_events, "event_type")
                except Exception as e:
                    _LOGGER.error(f"Failed to init hypertable/compression for events: {e}", exc_info=True)
                    
            _LOGGER.info("Database initialized successfully")
            self._connected = True

        except Exception as e:
            _LOGGER.error(f"Error initializing database: {e}", exc_info=True)
            self._connected = False

    async def _check_and_migrate_states(self, conn):
        """Check if migration from 'states' (legacy) to 'states_raw' is needed."""
        try:
            states_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'states' AND table_type = 'BASE TABLE')"
            )
            
            states_raw_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'states_raw')"
            )
            
            if states_exists and not states_raw_exists:
                _LOGGER.warning("Detected legacy 'states' table. Starting migration to 'states_raw'...")
                _LOGGER.info("1/2 Renaming 'states' to 'states_legacy'. Data migration will happen in background.")
                await conn.execute("ALTER TABLE states RENAME TO states_legacy")
                
        except Exception as e:
            _LOGGER.error(f"Migration check failed: {e}")

    async def _init_states_table(self, conn):
        """Initialize states_raw table and View."""
        
        # 1. Create states_raw
        _LOGGER.debug("Creating table states_raw if not exists")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS states_raw (
                time TIMESTAMPTZ NOT NULL,
                metadata_id INTEGER NOT NULL,
                state TEXT,
                value DOUBLE PRECISION,
                attributes JSONB
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS states_raw_meta_time_idx 
            ON states_raw (metadata_id, time DESC);
        """)
        
        # 2. The view creation is now handled in `_init_states_view` 
        # to ensure it doesn't conflict with the `states` table before migration.
        await self._init_states_view(conn)

    async def _init_states_view(self, conn):
        """Create the backward-compatible states view, if the name isn't taken by a table."""
        try:
            is_table = await conn.fetchval(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{self.table_name_states}' AND table_type = 'BASE TABLE')")
            if is_table:
                _LOGGER.debug(f"'{self.table_name_states}' is currently a table. Skipping view creation until migration finishes.")
                return

            _LOGGER.debug(f"Creating/Replacing view '{self.table_name_states}'")
            await conn.execute(f"DROP VIEW IF EXISTS {self.table_name_states} CASCADE;")
            await conn.execute(f"""
                CREATE VIEW {self.table_name_states} AS
                SELECT
                    s.time,
                    e.entity_id,
                    s.state,
                    s.value,
                    s.attributes
                FROM entities e
                CROSS JOIN LATERAL (
                    SELECT * FROM states_raw s 
                    WHERE s.metadata_id = e.id
                ) s;
            """)
        except Exception as e:
            _LOGGER.error(f"Failed to create states view: {e}")

    async def _init_events_table(self, conn):
        """Initialize events table."""
        _LOGGER.debug(f"Creating table {self.table_name_events} if not exists")
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name_events} (
                time TIMESTAMPTZ NOT NULL,
                event_type TEXT NOT NULL,
                event_data JSONB,
                origin TEXT,
                context_id TEXT,
                context_user_id TEXT,
                context_parent_id TEXT
            );
        """)
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS {self.table_name_events}_type_time_idx 
            ON {self.table_name_events} (event_type, time DESC);
        """)

    async def _init_users_table(self, conn):
        """Initialize users table."""
        _LOGGER.debug("Creating table users if not exists")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                name TEXT,
                is_owner BOOLEAN,
                is_active BOOLEAN,
                system_generated BOOLEAN,
                group_ids JSONB
            );
        """)

    async def write_users(self, users: list[dict]):
        """Write users to the database (upsert)."""
        if not self._pool or not users:
            return

        _LOGGER.debug(f"Writing {len(users)} users to database...")
        try:
            # Sanitize text fields (ensure string, remove null bytes)
            text_fields = ["user_id", "name"]
            for user in users:
                for field in text_fields:
                    if user.get(field) is not None:
                        user[field] = str(user[field]).replace("\0", "")
                if user.get("group_ids"):
                    user["group_ids"] = self._sanitize_obj(user["group_ids"])

            rows = [
                (
                    u.get("user_id"),
                    u.get("name"),
                    u.get("is_owner"),
                    u.get("is_active"),
                    u.get("system_generated"),
                    json.dumps(u["group_ids"]) if u.get("group_ids") else None,
                )
                for u in users
            ]

            await self._execute_many("""
                INSERT INTO users (user_id, name, is_owner, is_active, system_generated, group_ids)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                ON CONFLICT (user_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    is_owner = EXCLUDED.is_owner,
                    is_active = EXCLUDED.is_active,
                    system_generated = EXCLUDED.system_generated,
                    group_ids = EXCLUDED.group_ids;
            """, rows)
            _LOGGER.debug("Users written successfully")
        except Exception as e:
            _LOGGER.error(f"Error writing users: {e}", exc_info=True)

    async def _init_entities_table(self, conn):
        """Initialize entities table."""
        _LOGGER.debug("Creating table entities if not exists")
        
        # Ensure schema is up to date (migrate from old text-PK schema if needed)
        from . import migration  # lazy import to avoid circular dependency
        await migration.migrate_entities_table(conn)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                id SERIAL PRIMARY KEY,
                entity_id TEXT UNIQUE,
                unique_id TEXT,
                platform TEXT,
                domain TEXT,
                name TEXT,
                device_id TEXT,
                area_id TEXT,
                capabilities JSONB
            );
        """)
        
        # Index for entity lookup by entity_id (UNIQUE constraint already creates an index)
        # await conn.execute("CREATE INDEX IF NOT EXISTS entities_entity_id_idx ON entities (entity_id)")
        
        # Populate Cache on startup
        try:
            rows = await conn.fetch("SELECT entity_id, id FROM entities")
            for row in rows:
                self._entity_id_map[row['entity_id']] = row['id']
                self._metadata_id_map[row['id']] = row['entity_id']
            _LOGGER.debug(f"Loaded {len(self._entity_id_map)} entities into ID cache")
        except Exception as e:
            _LOGGER.warning(f"Failed to populate entity cache: {e}")

    async def write_entities(self, entities: list[dict]):
        """Write entities to the database (upsert)."""
        if not self._pool or not entities:
            return

        _LOGGER.debug(f"Writing {len(entities)} entities to database...")
        try:
            text_fields = ["entity_id", "unique_id", "platform", "domain", "name", "device_id", "area_id"]
            for entity in entities:
                for field in text_fields:
                    if entity.get(field) is not None:
                        entity[field] = str(entity[field]).replace("\0", "")
                if entity.get("capabilities"):
                    entity["capabilities"] = self._sanitize_obj(entity["capabilities"])

            rows = [
                (
                    e.get("entity_id"),
                    e.get("unique_id"),
                    e.get("platform"),
                    e.get("domain"),
                    e.get("name"),
                    e.get("device_id"),
                    e.get("area_id"),
                    json.dumps(e["capabilities"]) if e.get("capabilities") else None,
                )
                for e in entities
            ]

            await self._execute_many("""
                INSERT INTO entities (entity_id, unique_id, platform, domain, name, device_id, area_id, capabilities)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                ON CONFLICT (entity_id) DO UPDATE SET
                    unique_id = EXCLUDED.unique_id,
                    platform = EXCLUDED.platform,
                    domain = EXCLUDED.domain,
                    name = EXCLUDED.name,
                    device_id = EXCLUDED.device_id,
                    area_id = EXCLUDED.area_id,
                    capabilities = EXCLUDED.capabilities;
            """, rows)
            _LOGGER.debug("Entities written successfully")
        except Exception as e:
            _LOGGER.error(f"Error writing entities: {e}", exc_info=True)

    async def _init_areas_table(self, conn):
        """Initialize areas table."""
        _LOGGER.debug("Creating table areas if not exists")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS areas (
                area_id TEXT PRIMARY KEY,
                name TEXT,
                picture TEXT
            );
        """)

    async def write_areas(self, areas: list[dict]):
        """Write areas to the database (upsert)."""
        if not self._pool or not areas:
            return

        _LOGGER.debug(f"Writing {len(areas)} areas to database...")
        try:
            text_fields = ["area_id", "name", "picture"]
            for area in areas:
                for field in text_fields:
                    if area.get(field) is not None:
                        area[field] = str(area[field]).replace("\0", "")

            rows = [(a.get("area_id"), a.get("name"), a.get("picture")) for a in areas]

            await self._execute_many("""
                INSERT INTO areas (area_id, name, picture)
                VALUES ($1, $2, $3)
                ON CONFLICT (area_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    picture = EXCLUDED.picture;
            """, rows)
            _LOGGER.debug("Areas written successfully")
        except Exception as e:
            _LOGGER.error(f"Error writing areas: {e}", exc_info=True)

    async def _init_devices_table(self, conn):
        """Initialize devices table."""
        _LOGGER.debug("Creating table devices if not exists")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                name TEXT,
                name_by_user TEXT,
                model TEXT,
                manufacturer TEXT,
                sw_version TEXT,
                area_id TEXT,
                primary_config_entry TEXT
            );
        """)

    async def write_devices(self, devices: list[dict]):
        """Write devices to the database (upsert)."""
        if not self._pool or not devices:
            return

        _LOGGER.debug(f"Writing {len(devices)} devices to database...")
        
        try:
            text_fields = ["device_id", "name", "name_by_user", "model", "manufacturer", "sw_version", "area_id", "primary_config_entry"]
            for device in devices:
                for field in text_fields:
                    if device.get(field) is not None:
                        device[field] = str(device[field]).replace("\0", "")

            rows = [
                (
                    d.get("device_id"),
                    d.get("name"),
                    d.get("name_by_user"),
                    d.get("model"),
                    d.get("manufacturer"),
                    d.get("sw_version"),
                    d.get("area_id"),
                    d.get("primary_config_entry"),
                )
                for d in devices
            ]

            await self._execute_many("""
                INSERT INTO devices (device_id, name, name_by_user, model, manufacturer, sw_version, area_id, primary_config_entry)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (device_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    name_by_user = EXCLUDED.name_by_user,
                    model = EXCLUDED.model,
                    manufacturer = EXCLUDED.manufacturer,
                    sw_version = EXCLUDED.sw_version,
                    area_id = EXCLUDED.area_id,
                    primary_config_entry = EXCLUDED.primary_config_entry;
            """, rows)
            _LOGGER.debug("Devices written successfully")
        except Exception as e:
            _LOGGER.error(f"Error writing devices: {e}", exc_info=True)

    async def _init_integrations_table(self, conn):
        """Initialize integrations table."""
        _LOGGER.debug("Creating table integrations if not exists")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS integrations (
                entry_id TEXT PRIMARY KEY,
                domain TEXT,
                title TEXT,
                state TEXT,
                source TEXT
            );
        """)

    async def write_integrations(self, integrations: list[dict]):
        """Write integrations to the database (upsert)."""
        if not self._pool or not integrations:
            return

        _LOGGER.debug(f"Writing {len(integrations)} integrations to database...")
        try:
            text_fields = ["entry_id", "domain", "title", "state", "source"]
            for integration in integrations:
                for field in text_fields:
                    if integration.get(field) is not None:
                        integration[field] = str(integration[field]).replace("\0", "")

            rows = [
                (
                    i.get("entry_id"),
                    i.get("domain"),
                    i.get("title"),
                    i.get("state"),
                    i.get("source"),
                )
                for i in integrations
            ]

            await self._execute_many("""
                INSERT INTO integrations (entry_id, domain, title, state, source)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (entry_id) DO UPDATE SET
                    domain = EXCLUDED.domain,
                    title = EXCLUDED.title,
                    state = EXCLUDED.state,
                    source = EXCLUDED.source;
            """, rows)
            _LOGGER.debug("Integrations written successfully")
        except Exception as e:
            _LOGGER.error(f"Error writing integrations: {e}", exc_info=True)

    async def _init_hypertable(self, table_name, segment_by):
        """Initialize hypertable and compression.
        
        Each operation is done in its own transaction to avoid
        'transaction aborted' errors when one operation fails.
        """
        
        # Convert to hypertable
        try:
            _LOGGER.debug(f"Converting {table_name} to hypertable...")
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"SELECT create_hypertable('{table_name}', 'time', chunk_time_interval => INTERVAL '{self.chunk_interval}', if_not_exists => TRUE);"
                )
        except Exception as e:
            _LOGGER.warning(f"Hypertable creation failed (might not be TimescaleDB or already exists): {e}")

        # Enable compression
        try:
            _LOGGER.debug(f"Enabling compression for {table_name}...")
            async with self._pool.acquire() as conn:
                await conn.execute(f"""
                    ALTER TABLE {table_name} SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = '{segment_by}',
                        timescaledb.compress_orderby = 'time DESC'
                    );
                """)
        except Exception as e:
            _LOGGER.debug(f"Compression enable failed: {e}")

        # Add compression policy
        try:
            _LOGGER.debug(f"Adding compression policy for {table_name}...")
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"SELECT add_compression_policy('{table_name}', INTERVAL '{self.compress_after}', if_not_exists => TRUE);"
                )
        except Exception as e:
            _LOGGER.debug(f"Compression policy failed: {e}")

    # ------------------------------------------------------------------
    # Sanitization
    # ------------------------------------------------------------------

    def _sanitize_obj(self, obj: Any, depth: int = 0) -> Any:
        try:
            if depth > 100:
                return str(obj)

            if isinstance(obj, float):
                if math.isinf(obj) or math.isnan(obj):
                    return None

            if isinstance(obj, str):
                if "\0" in obj:
                    _LOGGER.warning(f"Sanitized string containing null byte: {obj!r}")
                    return obj.replace("\0", "")
                return obj
            if isinstance(obj, dict):
                 return {k: self._sanitize_obj(v, depth + 1) for k, v in obj.items()}
            if isinstance(obj, list):
                 return [self._sanitize_obj(v, depth + 1) for v in obj]
            if isinstance(obj, tuple):
                 return tuple(self._sanitize_obj(v, depth + 1) for v in obj)
            return obj
        except Exception as e:
            _LOGGER.error(f"Error serializing obj: {e}", exc_info=True)
            return obj

    # ------------------------------------------------------------------
    # Flush / write batch
    # ------------------------------------------------------------------

    async def _flush(self):
        """Flush the queue to the database."""
        try:
            self._flush_pending = False  # Reset flag immediately
            
            # Prune history first (maintain rolling window even if idle)
            now = time.time()
            while self._states_history and now - self._states_history[0][0] > 60:
                self._states_history.popleft()
            while self._events_history and now - self._events_history[0][0] > 60:
                self._events_history.popleft()

            if not self._queue:
                return

            # Swap queue - drain the deque
            batch = list(self._queue)
            self._queue.clear()
            
            start_time = time.time()
            
            try:
                def _process_batch(batch_items):
                    sanitized_batch = [self._sanitize_obj(item) for item in batch_items]
                    
                    states_res = []
                    events_res = []
                    
                    for x in sanitized_batch:
                        if x['type'] == 'state':
                            for field in ['entity_id', 'state']:
                                if x.get(field) is not None:
                                    x[field] = str(x[field]).replace('\0', '')

                            if isinstance(x.get('attributes'), dict):
                                x['attributes'] = json.dumps(x['attributes'], default=str)
                            states_res.append(x)
                        elif x['type'] == 'event':
                            for field in ['event_type', 'origin', 'context_id', 'context_user_id', 'context_parent_id']:
                                if x.get(field) is not None:
                                    x[field] = str(x[field]).replace('\0', '')

                            if isinstance(x.get('event_data'), dict):
                                x['event_data'] = json.dumps(x['event_data'], cls=JSONEncoder)
                            events_res.append(x)
                    return states_res, events_res

                # Run CPU-intensive serialization in executor
                states_data, events_data = await self.hass.async_add_executor_job(_process_batch, batch)

                # Resolve Metadata IDs for states
                if states_data:
                    eids = set()
                    for s in states_data:
                        if 'entity_id' in s:
                            eids.add(s['entity_id'])
                    
                    if eids:
                        await self._ensure_metadata_ids(list(eids))
                    
                    final_states_data = []
                    for s in states_data:
                        eid = s.pop('entity_id', None)
                        if eid and eid in self._entity_id_map:
                            s['metadata_id'] = self._entity_id_map[eid]
                            final_states_data.append(s)
                        else:
                            _LOGGER.warning(f"Skipping state for unknown entity_id: {eid}")
                    
                    states_data = final_states_data

                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        if states_data:
                            await conn.executemany(
                                "INSERT INTO states_raw (time, metadata_id, state, value, attributes) VALUES ($1, $2, $3, $4, $5::jsonb)",
                                [(s['time'], s['metadata_id'], s.get('state'), s.get('value'), s.get('attributes')) for s in states_data]
                            )
                        if events_data:
                            await conn.executemany(
                                f"INSERT INTO {self.table_name_events} (time, event_type, event_data, origin, context_id, context_user_id, context_parent_id) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)",
                                [(e['time'], e['event_type'], e.get('event_data'), e.get('origin'), e.get('context_id'), e.get('context_user_id'), e.get('context_parent_id')) for e in events_data]
                            )
                
                duration = time.time() - start_time
                self._states_written += len(states_data)
                self._events_written += len(events_data)
                
                self._states_history.append((time.time(), len(states_data)))
                self._events_history.append((time.time(), len(events_data)))
                
                self._last_write_duration = duration
                
                if not self._connected:
                    _LOGGER.info(f"Database connection restored. Flushed {len(states_data)} states and {len(events_data)} events.")

                self._connected = True
                self._last_error = None

            except asyncpg.PostgresError as e:
                msg = str(e)
                if "\n" in msg:
                    msg = msg.split("\n")[0]
                
                _LOGGER.error(f"Database error during flush: {msg}")

                self._connected = False
                self._last_error = msg
                
                if self.buffer_on_failure:
                    _LOGGER.warning(f"Buffering {len(batch)} items due to failure. Current queue size: {len(self._queue)}")
                    
            except Exception as e:
                _LOGGER.error(f"Error flushing batch: {e}")
                self._connected = False
                self._last_error = str(e)
                
                if self.buffer_on_failure:
                    _LOGGER.warning(f"Buffering {len(batch)} items due to failure. Current queue size: {len(self._queue)}")
                    self._queue = deque(batch + list(self._queue), maxlen=self.max_queue_size)
                    
                    if len(self._queue) == self.max_queue_size:
                        _LOGGER.warning(f"Buffer full! Queue size: {len(self._queue)}")
                else:
                    self._dropped_events += len(batch)
                    _LOGGER.warning(f"Dropped {len(batch)} items (buffering disabled)")
        except Exception as e:
            _LOGGER.error(f"Critical error in _flush: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Entity rename
    # ------------------------------------------------------------------

    async def rename_entity(self, old_entity_id: str, new_entity_id: str):
        """Rename an entity in the database (Metadata only).
        
        Updates the entity_id in 'entities' table.
        The 'states_raw' table uses metadata_id, so no data migration of history is needed!
        """
        if not self._pool:
            return

        _LOGGER.info(f"Renaming entity {old_entity_id} to {new_entity_id}")
        
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    try:
                        await conn.execute(
                            "UPDATE entities SET entity_id = $1 WHERE entity_id = $2",
                            new_entity_id, old_entity_id
                        )
                    except asyncpg.UniqueViolationError:
                        _LOGGER.warning(f"Cannot rename {old_entity_id} to {new_entity_id}: Target already exists.")
                        return
                
            # Update cache
            if old_entity_id in self._entity_id_map:
                mid = self._entity_id_map.pop(old_entity_id)
                self._entity_id_map[new_entity_id] = mid
                self._metadata_id_map[mid] = new_entity_id

            _LOGGER.info(f"Renamed entity {old_entity_id} to {new_entity_id} successfully")

        except Exception as e:
            _LOGGER.error(f"Failed to rename entity {old_entity_id} to {new_entity_id}: {e}")

    # ------------------------------------------------------------------
    # Query / stats
    # ------------------------------------------------------------------

    async def query(self, sql: str):
        """Execute a read-only query against the database."""
        if not self._pool:
            raise RuntimeError("Database not connected")

        _LOGGER.debug("Executing query (Read-Only): %s", sql)
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("SET LOCAL TRANSACTION READ ONLY")
                    try:
                        rows = await conn.fetch(sql)
                        return [dict(row) for row in rows]
                    except Exception:
                        raise
        except Exception as e:
            _LOGGER.error("Error executing query: %s", e)
            raise e

    async def get_db_stats(self, stats_type: str = "all"):
        """Fetch database statistics using TimescaleDB chunks view.
        
        Args:
            stats_type: Type of stats to fetch - "chunk", "size", or "all"
        """
        stats = {}
        if not self._pool:
            return stats
        
        tasks = []
        
        async def get_states_chunk_stats():
            try:
                row = await self._fetchrow("""
                    SELECT 
                        COUNT(*) AS total_chunks,
                        SUM(CASE WHEN is_compressed THEN 1 ELSE 0 END) AS compressed_chunks,
                        SUM(CASE WHEN NOT is_compressed THEN 1 ELSE 0 END) AS uncompressed_chunks
                    FROM timescaledb_information.chunks
                    WHERE hypertable_name = 'states_raw'
                """)
                if row:
                    return {
                        "states_total_chunks": row['total_chunks'] or 0,
                        "states_compressed_chunks": row['compressed_chunks'] or 0,
                        "states_uncompressed_chunks": row['uncompressed_chunks'] or 0
                    }
            except Exception as e:
                _LOGGER.debug(f"Failed to get states chunk stats: {e}")
            return {}

        async def get_states_size_stats():
            total_bytes = 0
            compressed_bytes = 0
            before_bytes = 0
            after_bytes = 0

            try:
                total_bytes = await self._fetchval("SELECT total_bytes FROM hypertable_detailed_size('states_raw')") or 0
            except Exception as e:
                _LOGGER.debug(f"Failed to get states total size: {e}")
            
            try:
                compressed_bytes = await self._fetchval("SELECT after_compression_total_bytes FROM hypertable_compression_stats('states_raw')") or 0
            except Exception as e:
                _LOGGER.debug(f"Failed to get states compressed size: {e}")

            try:
                row = await self._fetchrow("SELECT before_compression_total_bytes, after_compression_total_bytes FROM hypertable_compression_stats('states_raw')")
                if row:
                    before_bytes = row['before_compression_total_bytes'] or 0
                    after_bytes = row['after_compression_total_bytes'] or 0
            except Exception as e:
                _LOGGER.debug(f"Failed to get states compression ratio stats: {e}")

            return {
                "states_total_size": total_bytes,
                "states_compressed_size": compressed_bytes,
                "states_uncompressed_size": max(0, total_bytes - compressed_bytes),
                "states_before_compression_total_bytes": before_bytes,
                "states_after_compression_total_bytes": after_bytes
            }

        async def get_events_chunk_stats():
            try:
                row = await self._fetchrow(f"""
                    SELECT 
                        COUNT(*) AS total_chunks,
                        SUM(CASE WHEN is_compressed THEN 1 ELSE 0 END) AS compressed_chunks,
                        SUM(CASE WHEN NOT is_compressed THEN 1 ELSE 0 END) AS uncompressed_chunks
                    FROM timescaledb_information.chunks
                    WHERE hypertable_name = '{self.table_name_events}'
                """)
                if row:
                    return {
                        "events_total_chunks": row['total_chunks'] or 0,
                        "events_compressed_chunks": row['compressed_chunks'] or 0,
                        "events_uncompressed_chunks": row['uncompressed_chunks'] or 0
                    }
            except Exception as e:
                _LOGGER.debug(f"Failed to get events chunk stats: {e}")
            return {}

        async def get_events_size_stats():
            total_bytes = 0
            compressed_bytes = 0
            before_bytes = 0
            after_bytes = 0

            try:
                total_bytes = await self._fetchval(f"SELECT total_bytes FROM hypertable_detailed_size('{self.table_name_events}')") or 0
            except Exception as e:
                _LOGGER.debug(f"Failed to get events total size: {e}")
            
            try:
                compressed_bytes = await self._fetchval(f"SELECT after_compression_total_bytes FROM hypertable_compression_stats('{self.table_name_events}')") or 0
            except Exception as e:
                _LOGGER.debug(f"Failed to get events compressed size: {e}")

            try:
                row = await self._fetchrow(f"SELECT before_compression_total_bytes, after_compression_total_bytes FROM hypertable_compression_stats('{self.table_name_events}')")
                if row:
                    before_bytes = row['before_compression_total_bytes'] or 0
                    after_bytes = row['after_compression_total_bytes'] or 0
            except Exception as e:
                _LOGGER.debug(f"Failed to get events compression ratio stats: {e}")

            return {
                "events_total_size": total_bytes,
                "events_compressed_size": compressed_bytes,
                "events_uncompressed_size": max(0, total_bytes - compressed_bytes),
                "events_before_compression_total_bytes": before_bytes,
                "events_after_compression_total_bytes": after_bytes
            }

        async def get_states_compression_stats():
            try:
                row = await self._fetchrow(f"SELECT * FROM hypertable_compression_stats('{self.table_name_states}')")
                if row:
                    return {
                        "states_before_compression_total_bytes": row['before_compression_total_bytes'] or 0,
                        "states_after_compression_total_bytes": row['after_compression_total_bytes'] or 0
                    }
            except Exception as e:
                _LOGGER.debug(f"Failed to get states compression stats: {e}")
            return {}

        async def get_events_compression_stats():
            try:
                row = await self._fetchrow(f"SELECT * FROM hypertable_compression_stats('{self.table_name_events}')")
                if row:
                    return {
                        "events_before_compression_total_bytes": row['before_compression_total_bytes'] or 0,
                        "events_after_compression_total_bytes": row['after_compression_total_bytes'] or 0
                    }
            except Exception as e:
                _LOGGER.debug(f"Failed to get events compression stats: {e}")
            return {}

        if self.record_states:
            if stats_type in ("chunk", "all"):
                tasks.append(get_states_chunk_stats())
            if stats_type in ("size", "all"):
                tasks.append(get_states_size_stats())
                tasks.append(get_states_compression_stats())

        if self.record_events:
            if stats_type in ("chunk", "all"):
                tasks.append(get_events_chunk_stats())
            if stats_type in ("size", "all"):
                tasks.append(get_events_size_stats())
                tasks.append(get_events_compression_stats())

        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                stats.update(result)
            
        return stats

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def running(self):
        return self._running

    @property
    def buffer_size(self):
        return len(self._queue)

    @property
    def states_rate_minute(self):
        """Return states written per minute (rolling window)."""
        return sum(count for _, count in self._states_history)

    @property
    def events_rate_minute(self):
        """Return events written per minute (rolling window)."""
        return sum(count for _, count in self._events_history)
