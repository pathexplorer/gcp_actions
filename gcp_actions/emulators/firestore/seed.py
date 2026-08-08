#!/usr/bin/env python3
"""
Seed the local Firestore emulator with initial configuration documents.

Usage:
    # Built-in defaults:
    FIRESTORE_EMULATOR_HOST=localhost:8085 python seed.py

    # With custom defaults file:
    python seed.py --defaults-file /path/to/defaults.json

    # With inline JSON:
    python seed.py --defaults-json '{"KEY": "value"}'

    # Pull from real GCP Firestore (source of truth) into local emulator:
    python seed.py --from-project my-gcp-project-id

The script populates config/local/settings/data which is the document
used by InjectConfig._inject_firestore() in local-dev mode.
"""

import argparse
import json
import os
import subprocess
import sys

from google.cloud import firestore

# Sensible defaults for local development.
# These are base values — local_config.json takes precedence at runtime.
DEFAULT_CONFIG: dict[str, str] = {
    "CS_BUCKET_NAME": "local-dev-bucket",
    "EMAIL_MODE": "brevo",
    "GCS_PUB_INPUT_BUCKET": "local-input-bucket",
    "GCS_PUB_OUTPUT_BUCKET": "local-output-bucket",
    "DROPBOX_TOPIC_NAME": "dropbox-handler-testing",
    "LOGGING_LEVEL": "DEBUG",
}

# Firestore document path used by InjectConfig._inject_firestore()
CONFIG_COLLECTION = "config"
CONFIG_DOC_PATH = "local/settings/data"


def _resolve_path(collection: str, doc_path: str):
    """
    Resolve a Firestore document reference from collection + dotted/segmented
    path string, e.g. collection="config", doc_path="local/settings/data".
    Returns a DocumentReference.
    """
    db = firestore.Client()
    segments = doc_path.split("/")
    doc_ref = db.collection(collection).document(segments[0])
    for i in range(1, len(segments), 2):
        sub_col = segments[i]
        sub_doc = segments[i + 1] if i + 1 < len(segments) else None
        if sub_doc:
            doc_ref = doc_ref.collection(sub_col).document(sub_doc)
        else:
            doc_ref = doc_ref.collection(sub_col).document()
    return doc_ref


def _gcloud_project() -> str:
    """Return the currently configured gcloud project ID."""
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True, text=True, check=True, timeout=5,
        )
        project_id = result.stdout.strip()
        if not project_id:
            print("❌ No gcloud project is set. Run: gcloud config set project YOUR_PROJECT_ID")
            sys.exit(1)
        return project_id
    except FileNotFoundError:
        print("❌ gcloud CLI not found. Install: https://cloud.google.com/sdk/gcloud")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"❌ gcloud command failed: {e.stderr.strip()}")
        sys.exit(1)


def pull_from_gcp(project_id: str) -> dict[str, str]:
    """
    Read config/local/settings/data from a real GCP Firestore project.

    Temporarily clears FIRESTORE_EMULATOR_HOST so the Firestore client
    connects to real GCP instead of the local emulator.
    """
    saved_emulator_host = os.environ.pop("FIRESTORE_EMULATOR_HOST", None)

    try:
        print(f"📡 Connecting to real GCP Firestore (project: {project_id}) ...")
        db = firestore.Client(project=project_id)

        doc_ref = (
            db.collection(CONFIG_COLLECTION)
            .document("local")
            .collection("settings")
            .document("data")
        )
        doc = doc_ref.get()

        if not doc.exists:
            print(
                f"❌ Document {CONFIG_COLLECTION}/{CONFIG_DOC_PATH} not found "
                f"in project '{project_id}'."
            )
            sys.exit(1)

        config = dict(doc.to_dict())
        print(f"✅ Pulled {len(config)} keys from GCP Firestore.")
        for k, v in sorted(config.items()):
            # Truncate long values for display
            display = v if len(str(v)) <= 50 else str(v)[:47] + "..."
            print(f"   {k} = {display}")
        return config

    except Exception as e:
        print(f"❌ Failed to read from GCP Firestore: {e}")
        print()
        print("   Make sure you are authenticated to GCP:")
        print("     gcloud auth application-default login")
        print("   Or set GOOGLE_APPLICATION_CREDENTIALS to a service account key file.")
        sys.exit(1)

    finally:
        # Restore emulator host so subsequent writes go to the emulator
        if saved_emulator_host:
            os.environ["FIRESTORE_EMULATOR_HOST"] = saved_emulator_host


def seed_firestore(config: dict, emulator_host: str | None = None):
    """Seed the Firestore emulator with the given config."""
    if emulator_host:
        os.environ["FIRESTORE_EMULATOR_HOST"] = emulator_host

    host = os.environ.get("FIRESTORE_EMULATOR_HOST", "localhost:8085")
    print(f"Connecting to Firestore emulator at {host} ...")

    db = firestore.Client()

    # config/local/settings/data  (as used by InjectConfig._inject_firestore)
    doc_ref = db.collection("config").document("local") \
               .collection("settings").document("data")

    existing = doc_ref.get()
    if existing.exists:
        print(f"⚠  Document config/local/settings/data already exists.")
        print(f"   Existing keys: {sorted(existing.to_dict().keys())}")
        doc_ref.set(config, merge=True)
        print(f"✅ Merged {len(config)} keys into existing document.")
    else:
        doc_ref.set(config)
        print(f"✅ Created document config/local/settings/data with {len(config)} keys.")

    # Print final state
    final = doc_ref.get().to_dict()
    print(f"📋 Final document ({len(final)} keys):")
    for k, v in sorted(final.items()):
        print(f"   {k} = {v}")


def main():
    parser = argparse.ArgumentParser(description="Seed Firestore emulator")
    parser.add_argument(
        "--defaults-file",
        help="JSON file with default config values (merged on top of built-in defaults)",
    )
    parser.add_argument(
        "--defaults-json",
        help="Inline JSON with default config values (merged on top of built-in defaults)",
    )
    parser.add_argument(
        "--from-project",
        nargs="?",
        const="__auto__",   # --from-project alone → auto-detect from gcloud
        default=None,        # not provided → use built-in defaults
        metavar="GCP_PROJECT_ID",
        help=(
            "Pull config/local/settings/data from a real GCP Firestore project "
            "and seed it into the local emulator. Requires GCP authentication "
            "(gcloud auth application-default login). "
            "If GCP_PROJECT_ID is omitted, auto-detects from 'gcloud config get-value project'."
        ),
    )
    parser.add_argument(
        "--emulator-host",
        default=os.environ.get("FIRESTORE_EMULATOR_HOST", "localhost:8085"),
        help="Firestore emulator host:port",
    )
    args = parser.parse_args()

    # --- Resolve config source ---
    if args.from_project is not None:
        # GCP is source of truth — pull from real Firestore
        project_id = args.from_project if args.from_project != "__auto__" else _gcloud_project()
        config = pull_from_gcp(project_id)

        # Merge any local overrides on top
        if args.defaults_file:
            with open(args.defaults_file) as f:
                config.update(json.load(f))
            print(f"📄 Merged {args.defaults_file} overrides.")

        if args.defaults_json:
            config.update(json.loads(args.defaults_json))
            print(f"📄 Merged inline JSON overrides.")

    else:
        # Offline / local-only mode — use built-in defaults
        config = dict(DEFAULT_CONFIG)

        if args.defaults_file:
            with open(args.defaults_file) as f:
                config.update(json.load(f))
            print(f"📄 Loaded {len(config)} keys from {args.defaults_file}")

        if args.defaults_json:
            config.update(json.loads(args.defaults_json))
            print(f"📄 Merged inline JSON overrides.")

    seed_firestore(config, args.emulator_host)


if __name__ == "__main__":
    main()
