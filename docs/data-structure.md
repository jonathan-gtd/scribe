# Scribe Database Structure & Querying Guide

Scribe is a long-term storage integration for Home Assistant. It stores entity states in a PostgreSQL/TimescaleDB database. This guide explains the database schema and how to query it — for example from Grafana.

---

## Tables & Views Overview

| Name | Type | Description |
|---|---|---|
| `states_raw` | Table (Hypertable) | All recorded state values |
| `entities` | Table | Registry of all HA entities |
| `devices` | Table | Physical devices |
| `areas` | Table | Rooms / zones |
| `integrations` | Table | HA integrations (platforms) |
| `users` | Table | HA user accounts |
| `states` | View | Convenience join of `states_raw` + `entities` |

---

## Table Details

### `states_raw` — The measurements

The core time-series table. Every state change recorded by Scribe ends up here. It is a **TimescaleDB hypertable**, meaning it is automatically partitioned into chunks by time for performance. You never interact with the chunks directly — `states_raw` behaves like a normal table.

| Column | Type | Description |
|---|---|---|
| `time` | `timestamptz` | Timestamp of the state change |
| `metadata_id` | `integer` | Foreign key → `entities.id` |
| `state` | `text` | Raw state string (e.g. `"on"`, `"unavailable"`) |
| `value` | `double precision` | Numeric value, if applicable |
| `attributes` | `jsonb` | HA entity attributes at time of recording |

> `metadata_id` is the only link to the entity name. Always JOIN with `entities` to filter by `entity_id`.

---

### `entities` — The entity registry

One row per Home Assistant entity. Scribe assigns each entity a numeric `id` which is used as the foreign key in `states_raw`.

| Column | Type | Description |
|---|---|---|
| `id` | `integer` | Internal numeric ID (used in `states_raw.metadata_id`) |
| `entity_id` | `text` | HA entity ID, e.g. `sensor.ping_de_jitter` |
| `name` | `text` | Friendly name |
| `domain` | `text` | HA domain, e.g. `sensor`, `binary_sensor` |
| `platform` | `text` | Integration platform, e.g. `mqtt` |
| `device_id` | `text` | FK → `devices.device_id` |
| `area_id` | `text` | FK → `areas.area_id` |
| `capabilities` | `jsonb` | HA entity capabilities |
| `unique_id` | `text` | HA unique ID |

---

### `devices` — Physical devices

One row per physical device registered in HA. A device can have multiple entities.

| Column | Type | Description |
|---|---|---|
| `device_id` | `text` | HA device ID |
| `name` | `text` | Device name |
| `name_by_user` | `text` | User-assigned name |
| `manufacturer` | `text` | e.g. `Shelly`, `FoxESS` |
| `model` | `text` | Device model |
| `sw_version` | `text` | Firmware version |
| `area_id` | `text` | FK → `areas.area_id` |
| `primary_config_entry` | `text` | FK → `integrations.entry_id` |

---

### `areas` — Rooms and zones

Mirrors the HA area registry. Relevant if you want to group entities or devices by room.

| Column | Type | Description |
|---|---|---|
| `area_id` | `text` | HA area ID |
| `name` | `text` | e.g. `Living Room`, `Office` |
| `picture` | `text` | Optional picture URL |

---

### `integrations` — HA config entries

One row per integration loaded in HA.

| Column | Type | Description |
|---|---|---|
| `entry_id` | `text` | HA config entry ID |
| `domain` | `text` | Integration domain, e.g. `mqtt`, `foxess_modbus` |
| `title` | `text` | Human-readable title |
| `state` | `text` | Current state of the integration |
| `source` | `text` | How it was set up (e.g. `user`, `import`) |

---

### `users` — HA user accounts

Mirrors the HA user registry. Not relevant for time-series queries.

---

### `states` — Convenience view

A pre-built JOIN of `states_raw` and `entities`. Exposes `entity_id` directly so no manual JOIN is needed.

```sql
-- Approximate definition
SELECT s.time, e.entity_id, s.state, s.value, s.attributes
FROM states_raw s
JOIN entities e ON e.id = s.metadata_id
```

---

## Entity Relationships

```
integrations
      │
      │ platform / domain
      ▼
  entities ──────────────────────── states_raw
  (entity_id, device_id, area_id)   (time, metadata_id, value, ...)
      │
      ├──→ devices (device_id)
      │        │
      │        └──→ areas (area_id)
      │
      └──→ areas (area_id)

users  →  standalone, no relation to measurements
```

---

## Querying

### Find an entity ID

```sql
SELECT id, entity_id, name
FROM entities
WHERE entity_id LIKE '%ping%';
```

### Query a single metric (using the view)

The simplest approach — no JOIN required:

```sql
SELECT time, value
FROM states
WHERE entity_id = 'sensor.ping_de_jitter'
ORDER BY time DESC
LIMIT 100;
```

### Query a single metric (using the raw table)

Preferred for large time ranges — TimescaleDB can use chunk pruning more efficiently:

```sql
SELECT s.time, s.value
FROM states_raw s
JOIN entities e ON e.id = s.metadata_id
WHERE e.entity_id = 'sensor.ping_de_jitter'
ORDER BY s.time DESC
LIMIT 100;
```

### Grafana query (with time filter)

```sql
SELECT s.time AS "time", s.value AS "Jitter (ms)"
FROM states_raw s
JOIN entities e ON e.id = s.metadata_id
WHERE e.entity_id = 'sensor.ping_de_jitter'
AND $__timeFilter(s.time)
ORDER BY s.time;
```

### Query multiple metrics as columns (pivot)

Useful when you want all metrics for one host in a single result:

```sql
SELECT
    date_trunc('second', s.time)                                              AS time,
    MAX(CASE WHEN e.entity_id = 'sensor.ping_de_jitter'    THEN s.value END) AS jitter,
    MAX(CASE WHEN e.entity_id = 'sensor.ping_de_round_min' THEN s.value END) AS round_trip_min,
    MAX(CASE WHEN e.entity_id = 'sensor.ping_de_round_max' THEN s.value END) AS round_trip_max,
    MAX(CASE WHEN e.entity_id = 'sensor.ping_de_round_avg' THEN s.value END) AS round_trip_avg
FROM states_raw s
JOIN entities e ON e.id = s.metadata_id
WHERE e.entity_id IN (
    'sensor.ping_de_jitter',
    'sensor.ping_de_round_min',
    'sensor.ping_de_round_max',
    'sensor.ping_de_round_avg'
)
AND $__timeFilter(s.time)
GROUP BY date_trunc('second', s.time)
HAVING COUNT(DISTINCT e.entity_id) = 4
ORDER BY time;
```

`HAVING COUNT(DISTINCT e.entity_id) = 4` ensures only complete sets (all 4 metrics arrived) are returned.

### Query all entities of a device

```sql
SELECT e.entity_id, e.name, s.time, s.value
FROM states_raw s
JOIN entities e ON e.id = s.metadata_id
JOIN devices d ON d.device_id = e.device_id
WHERE d.name = 'My Device Name'
AND s.time > now() - interval '1 hour'
ORDER BY s.time DESC;
```

### Query all entities in a room

```sql
SELECT e.entity_id, e.name, s.time, s.value
FROM states_raw s
JOIN entities e ON e.id = s.metadata_id
JOIN areas a ON a.area_id = e.area_id
WHERE a.name = 'Living Room'
AND s.time > now() - interval '1 hour'
ORDER BY s.time DESC;
```

---

## Notes

- **`state` vs `value`**: Numeric sensors populate `value`; non-numeric states (e.g. `on`/`off`, `unavailable`) are stored in `state`. Both can be set simultaneously.
- **`attributes`**: Contains the full HA attribute dict as JSONB at time of recording (unit, device class, friendly name, etc.). Query example: `attributes->>'unit_of_measurement'`.
- **Performance**: For Grafana dashboards covering large time ranges, always use `states_raw` + explicit JOIN rather than the `states` view, and make sure the `$__timeFilter` is on `s.time` so TimescaleDB can prune chunks effectively.
