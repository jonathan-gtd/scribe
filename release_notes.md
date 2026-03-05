### ⚠️ BREAKING CHANGE
- **Database Schema Migration (states_raw)**: Scribe 3.0.0 introduces a major refactoring of the database schema for the states history to improve performance and TimescaleDB compression drastically.
- The previous `states` table will be renamed to `states_legacy`. A background migration will occur linearly after HA startup to copy your historical states back into the new `states_raw` table without freezing the interface.
- **Dependency Update**: Scribe now uses native `asyncpg` connection pools instead of `SQLAlchemy`, fixing initialization issues on newer Home Assistant versions (>=2025.1.0) and Python >=3.12.

### 🔄 Migration Details
When you install this version and restart Home Assistant, Scribe will:
1. Wait 60 seconds to ensure Home Assistant completes its bootstrap properly without database locks.
2. Rename your old `states` table to `states_legacy`.
3. Create the new `states_raw` hypertable.
4. Smoothly migrate your old data from `states_legacy` into `states_raw` in small 12-hour chunks in the background.
5. Provide a legacy view named `states` so your existing Grafana dashboards remain perfectly compatible!

**Note from the author:**
> It's strongly recommended you create a database backup BEFORE upgrading to this version just in case, even though my personal migration of 50GB went smoothly!
