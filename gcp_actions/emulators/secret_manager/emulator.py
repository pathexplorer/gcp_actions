"""
Secret Manager Emulator — lightweight Flask server for local development.

Supports:
  - POST /v1/projects/<project>/secrets          → create a secret
  - POST /v1/projects/<project>/secrets/<name>:addVersion  → add a new version (JSON payload)
  - GET  /v1/projects/<project>/secrets/<name>/versions/<version> → access a version

Stores secrets in a JSON file so they survive container restarts.

Usage:
    # Directly:
    SM_EMULATOR_DATA_FILE=/tmp/secrets.json python -m gcp_actions.emulators.secret_manager.emulator

    # Or via the Dockerfile (containerized):
    podman-compose -f path/to/compose.yaml up -d
"""

import json
import os
from pathlib import Path

from flask import Flask, request, jsonify

app = Flask(__name__)

DATA_FILE = Path(os.environ.get("SM_EMULATOR_DATA_FILE", "/data/secrets.json"))
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_store() -> dict:
    """Load the secrets store from disk, or return an empty dict."""
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_store(store: dict) -> None:
    """Persist the secrets store to disk."""
    DATA_FILE.write_text(json.dumps(store, indent=2))


def _secret_path(project: str, secret_name: str) -> str:
    return f"projects/{project}/secrets/{secret_name}"


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "emulator": "secret-manager"})


# ---------------------------------------------------------------------------
# Create secret
# ---------------------------------------------------------------------------
@app.route("/v1/projects/<project>/secrets", methods=["POST"])
def create_secret(project: str):
    """
    Minimal create-secret endpoint.
    Body example (we mostly ignore it in emulator):
      {"secret_id": "my-secret", "secret": {"replication": {"automatic": {}}}}
    """
    body = request.get_json(silent=True) or {}
    secret_id = body.get("secret_id")
    if not secret_id:
        # fallback: try query param
        secret_id = request.args.get("secret_id")
    if not secret_id:
        return jsonify({"error": "secret_id is required"}), 400

    store = _load_store()
    path = _secret_path(project, secret_id)
    if path in store:
        return jsonify({"error": "secret already exists"}), 409

    store[path] = {"versions": {}}  # no versions yet
    _save_store(store)
    return jsonify({"name": path}), 201


# ---------------------------------------------------------------------------
# Add secret version
# ---------------------------------------------------------------------------
@app.route("/v1/projects/<project>/secrets/<secret_name>:addVersion", methods=["POST"])
def add_secret_version(project: str, secret_name: str):
    """
    Add a new version to a secret.
    Body example:
      {"payload": {"data": "<base64-or-raw-json>"}}
    For simplicity the emulator expects data as a JSON string.
    """
    body = request.get_json(silent=True) or {}
    payload = body.get("payload", {})
    data = payload.get("data", "")

    store = _load_store()
    path = _secret_path(project, secret_name)
    if path not in store:
        # auto-create the secret (lenient emulator behaviour)
        store[path] = {"versions": {}}

    versions = store[path]["versions"]
    next_version = str(len(versions) + 1)
    versions[next_version] = data
    _save_store(store)

    return jsonify({"name": f"{path}/versions/{next_version}", "version": next_version}), 201


# ---------------------------------------------------------------------------
# Access secret version
# ---------------------------------------------------------------------------
@app.route(
    "/v1/projects/<project>/secrets/<secret_name>/versions/<version>",
    methods=["GET"],
)
def access_secret_version(project: str, secret_name: str, version: str):
    """Return the payload data for a secret version."""
    store = _load_store()
    path = _secret_path(project, secret_name)
    if path not in store:
        return jsonify({"error": "secret not found"}), 404

    versions = store[path]["versions"]
    if version == "latest":
        if not versions:
            return jsonify({"error": "no versions exist"}), 404
        # dict keys are insertion-ordered in Python 3.7+
        version = list(versions.keys())[-1]

    data = versions.get(version)
    if data is None:
        return jsonify({"error": f"version {version} not found"}), 404

    return jsonify({"payload": {"data": data}}), 200


# ---------------------------------------------------------------------------
# List secrets (convenience for debugging)
# ---------------------------------------------------------------------------
@app.route("/v1/projects/<project>/secrets", methods=["GET"])
def list_secrets(project: str):
    """List all secret names in the project (debug helper)."""
    store = _load_store()
    prefix = f"projects/{project}/secrets/"
    names = [k for k in store if k.startswith(prefix)]
    return jsonify({"secrets": [{"name": n} for n in names]}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8083))
    app.run(host="0.0.0.0", port=port, debug=True)
