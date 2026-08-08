#!/usr/bin/env python3
"""
Seed the local Firestore emulator with initial configuration documents.

Usage:
    FIRESTORE_EMULATOR_HOST=localhost:8085 python seed.py

    # With custom defaults file:
    python seed.py --defaults-file /path/to/defaults.json

    # With inline JSON:
    python seed.py --defaults-json '{"KEY": "value"}'

The script populates config/local/settings/data which is the document
used by InjectConfig._inject_firestore() in local-dev mode.
"""

import argparse
import json
import os
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
        "--emulator-host",
        default=os.environ.get("FIRESTORE_EMULATOR_HOST", "localhost:8085"),
        help="Firestore emulator host:port",
    )
    args = parser.parse_args()

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
