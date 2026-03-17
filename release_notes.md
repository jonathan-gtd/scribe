### 🐛 Bug Fixes

- **YAML Configuration Priority**: All configuration options defined in `configuration.yaml` now correctly take priority over settings configured via the Home Assistant UI.
- **Statistics Sensors**: Fixed an issue where `enable_stats_io`, `enable_stats_chunk`, and `enable_stats_size` were ignored when provided via YAML.
