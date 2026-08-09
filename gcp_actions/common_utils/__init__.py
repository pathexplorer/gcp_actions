"""
Common utilities for logging, timing, environment detection, and configuration.

Includes:
- handle_logs: Cloud Run JSON / local colored logging setup
- timer: Function and pipeline stage timing utilities
- local_runner: Cloud Run vs local environment detection and .env loading
- validate_entity: Resource name validation
- generate: Signed URL generation for GCS
- init_config: Configuration merging from Firestore, Secret Manager, local overrides
"""