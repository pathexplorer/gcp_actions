#!/usr/bin/env python3
"""
Seed the local Secret Manager emulator with secrets from keys.env.

Usage:
    # Explicit path:
    python -m gcp_actions.emulators.secret_manager.seed --keys-env /path/to/keys.env

    # Via env var:
    KEYS_ENV_PATH=/path/to/keys.env python -m gcp_actions.emulators.secret_manager.seed

    # Auto-detect (tries common locations):
    python -m gcp_actions.emulators.secret_manager.seed

The script reads variables from keys.env and creates/updates the
corresponding secrets in the emulator so that the application can
use SecretManagerClient in emulator mode exactly as in production.
"""

import argparse
import json
import os
import re
import sys
import urllib.request
import urllib.error
from pathlib import Path


def _load_env_file(path: str) -> None:
    """Parse a KEY=VALUE env file and inject into os.environ (stdlib only)."""
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", line)
            if match:
                key, value = match.group(1), match.group(2).strip()
                # strip optional surrounding quotes
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                os.environ[key] = value


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _find_keys_env() -> str | None:
    """Try to locate keys.env from common locations."""
    candidates = [
        # Explicit env var
        os.environ.get("KEYS_ENV_PATH"),
        # Relative to this file:  gcp_actions/gcp_actions/emulators/secret_manager/seed.py
        # Going up 5 levels gets us to the monorepo root (main/)
        Path(__file__).resolve().parents[5] / "BigBikeData" / "power_core" / "project_env" / "keys.env",
        # Legacy hard-coded path
        Path.home() / "mega" / "projects" / "BigBikeData" / "keys.env",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(candidate)
    return None


def emulator_request(emulator_base: str, method: str, path: str, body: dict | None = None) -> dict:
    """Send an HTTP request to the emulator and return parsed JSON."""
    url = f"{emulator_base}{path}"
    data = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        if e.code == 409:  # AlreadyExists — non-fatal
            print(f"   ⚠️  Secret already exists (409), continuing.")
            return {}
        raise RuntimeError(f"Emulator HTTP {e.code}: {err_body}") from e
    except urllib.error.URLError as e:
        raise ConnectionError(
            f"Cannot reach Secret Manager emulator at {emulator_base}. "
            f"Is it running? Run: podman-compose up -d\nError: {e}"
        ) from e


def create_or_update_secret(emulator_base: str, project_id: str, secret_name: str, payload: dict) -> None:
    """Create a secret (if needed) and add a version with the given payload."""
    print(f"\n🔐 Secret: {secret_name}")
    emulator_request(
        emulator_base, "POST",
        f"/v1/projects/{project_id}/secrets",
        body={"secret_id": secret_name, "secret": {"replication": {"automatic": {}}}},
    )
    json_payload = json.dumps(payload, indent=2)
    emulator_request(
        emulator_base, "POST",
        f"/v1/projects/{project_id}/secrets/{secret_name}:addVersion",
        body={"payload": {"data": json_payload}},
    )
    print(f"   ✅ Seeded {len(payload)} keys: {list(payload.keys())}")


# ---------------------------------------------------------------------------
# Secret → payload mapping
# ---------------------------------------------------------------------------
# Mirrors what production Secret Manager stores.
#
# Format: env_var_holding_secret_name → list of env vars that make up the JSON payload
#
# Multiple env vars can point to the SAME secret name (e.g. SEC_DROPBOX and
# SEC_STRAVA both → "dropbox-secrets") — their payload keys are merged.
# ---------------------------------------------------------------------------
SECRET_CONFIG_MAP = {
    # --- fullstack-app-json-keys (all general config) ---
    "APP_JSON_KEYS": [
        "GCP_PROJECT_ID",
        "BREVO_API_KEY",
        "SMTP_PASSWORD",
        "SMTP_SENDER",
        "SMTP_SERVER",
        "SMTP_PORT",
        "SMTP_USER",
        "EVENTARC_SA",
        "EVENTARC_TRIGGER",
        "SEC_DROPBOX",
        "S_ACCOUNT_RUN",
        "S_ACCOUNT_DROPBOX",
        "CLOUD_RUN_SERVICE",
        "CLOUD_RUN_SERVICE_PUB",
        "GCS_BUCKET_NAME",
        "GCS_PUB_OUTPUT_BUCKET",
        "EMAIL_MODE",
        "STRAVA_UPLOAD",
        "GCP_TOPIC_NAME",
        "DROPBOX_TOPIC_NAME",
        "COOKIE_DOMAIN",
        "PRIVATE_ACCESS_TOKEN",
        "PRIVATE_UPLOAD_TOKEN",
        "FRONTEND_BASE_URL",
        "FLASK_SECRET_KEY",
        "DONATION_HTML_SNIPPET_MONO",
        "DONATION_HTML_SNIPPET_PRIVAT",
        "BACKEND_TAG",
        "FRONTEND_TAG",
    ],
    # --- dropbox-secrets (Dropbox + Strava + PG keys — combined secret) ---
    "SEC_DROPBOX": [
        "DROPBOX_APP_KEY",
        "DROPBOX_APP_SECRET",
        "DROPBOX_REFRESH_TOKEN",
        "STRAVA_APP_ID",
        "STRAVA_CLIENT_SECRET",
        "STRAVA_REFRESH_TOKEN",
        "STRAVA_ACCESS_TOKEN",
        "STRAVA_EXPIRES_AT",
    ],
}

# Single-key secrets: env_var_holding_secret_name → env_var_holding_the_value
# These are stored as a plain string (not wrapped in JSON).
SINGLE_KEY_SECRETS = {
    # Example: "FLASK_SECRET_KEY_NAME": "FLASK_SECRET_KEY_VALUE",
}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the Secret Manager emulator")
    parser.add_argument(
        "--keys-env",
        default=None,
        help="Path to keys.env file (default: auto-detect via KEYS_ENV_PATH or common locations)",
    )
    parser.add_argument(
        "--emulator-host",
        default=os.environ.get("SECRET_MANAGER_EMULATOR_HOST", "localhost:8083"),
        help="Secret Manager emulator host:port (default: localhost:8083 or $SECRET_MANAGER_EMULATOR_HOST)",
    )
    parser.add_argument(
        "--project",
        default=os.environ.get("GCP_PROJECT_ID", "local-test-project"),
        help="GCP project ID to use in the emulator (default: local-test-project or $GCP_PROJECT_ID)",
    )
    args = parser.parse_args()

    # Resolve keys.env path
    keys_env_path = args.keys_env or _find_keys_env()
    if not keys_env_path:
        print("❌ Could not find keys.env.")
        print("   Use --keys-env /path/to/keys.env or set KEYS_ENV_PATH.")
        print("   Tried common locations under the monorepo root.")
        sys.exit(1)

    print(f"✅ Loading keys from: {keys_env_path}")
    _load_env_file(keys_env_path)

    # Normalise emulator URL
    emulator_base = args.emulator_host
    if not emulator_base.startswith("http"):
        emulator_base = f"http://{emulator_base}"
    emulator_base = emulator_base.rstrip("/")
    project_id = args.project

    # Health check
    try:
        health = emulator_request(emulator_base, "GET", "/health")
        print(f"✅ Emulator reachable at {emulator_base}: {health}")
    except ConnectionError as e:
        print(f"❌ {e}")
        sys.exit(1)

    # Seed each secret
    for env_var_name, payload_keys in SECRET_CONFIG_MAP.items():
        secret_name = os.environ.get(env_var_name)
        if not secret_name:
            print(f"\n⏭️  Skipping {env_var_name}: env var not set in keys.env")
            continue

        payload = {}
        missing = []
        for key in payload_keys:
            value = os.environ.get(key)
            if value is not None:
                payload[key] = value
            else:
                missing.append(key)

        if missing:
            print(f"\n⚠️  {env_var_name} → secret '{secret_name}': missing keys: {missing}")
            if not payload:
                print(f"   ❌ No payload data; skipping.")
                continue

        create_or_update_secret(emulator_base, project_id, secret_name, payload)

    print("\n" + "=" * 60)
    print("🎉 Emulator seeding complete!")
    print(f"   Emulator: {emulator_base}")
    print(f"   Project:  {project_id}")
    print("=" * 60)


if __name__ == "__main__":
    main()
