"""
Secret Manager emulator for local development.

Provides a Flask-based HTTP API compatible with the Secret Manager v1 REST API.
Run via Docker (included Dockerfile) or directly:
    python -m gcp_actions.emulators.secret_manager.emulator

Persists secrets to JSON file for survival across container restarts.
"""