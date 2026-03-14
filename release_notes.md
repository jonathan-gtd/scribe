### 🐛 Bug Fixes

- **Fix migration on compressed hypertables**: The `states_raw` constraints migration (PK/FK) now temporarily disables TimescaleDB compression before `ALTER TABLE` and re-enables it afterwards. This fixes the `operation not supported on hypertables that have compression enabled` error when upgrading from earlier 3.x versions.
- **Fix `states_legacy` cleanup**: Changed `DROP TABLE states_legacy` to `DROP TABLE states_legacy CASCADE` to properly handle dependent TimescaleDB internal views (continuous aggregates, partial views) that could block the drop.
