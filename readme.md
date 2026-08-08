# gcp-actions

Reusable Google Cloud helper functions.

## Requirements

- Python >= 3.8
- [uv](https://docs.astral.sh/uv/) – fast Python package manager.

---

## Local installation with `uv`

### 1. Create and activate a virtual environment

```bash
uv venv
source .venv/bin/activate
```

> On Windows use `.venv\Scripts\activate` instead.

### 2. Install with core dependencies only

```bash
uv pip install -e .
```

### 3. Install with specific extras

```bash
# With Storage support
uv pip install -e ".[storage]"

# With Pub/Sub support
uv pip install -e ".[pubsub]"

# With Firestore support
uv pip install -e ".[firestore]"

# With Secret Manager support
uv pip install -e ".[secretmanager]"
```

### 4. Install all extras at once (recommended)

```bash
uv pip install -e ".[processing-worker]"
```

### Quick start (one-liner)

```bash
uv venv && uv pip install -e ".[processing-worker]"
```

---

## Verify installation

```bash
uv pip show gcp-actions
```

---

## Emulators (local development)

For local development without real GCP credentials, the following emulators are available:

### Secret Manager emulator

A lightweight Flask server that mimics the Secret Manager REST API.
Located at `gcp_actions/emulators/secret_manager/`.

```bash
cd gcp_actions/emulators/secret_manager
podman build -t sm-emulator .
podman run -p 8083:8083 sm-emulator
```

Secrets are stored in a JSON file. See `SETUP_ISSUES.md` for known quirks.

### Firestore emulator

Uses the official Google Cloud SDK Firestore emulator (`gcloud beta emulators firestore`).
A seed script (`emulators/firestore/seed.py`) populates the initial
`config/local/settings/data` document used by `InjectConfig`.

```bash
# Start the emulator (requires google/cloud-sdk:emulators image)
podman run -d --name fs-emulator -p 8085:8085 \
    google/cloud-sdk:emulators \
    gcloud beta emulators firestore start --host-port=0.0.0.0:8085

# Seed configuration
FIRESTORE_EMULATOR_HOST=localhost:8085 python gcp_actions/emulators/firestore/seed.py
```

For a turnkey setup of both emulators, use `local_dev.sh` from the `power_core` project.

---

## Available extras

| Extra              | Install command                                    |
|--------------------|----------------------------------------------------|
| `storage`          | `uv pip install -e ".[storage]"`                   |
| `pubsub`           | `uv pip install -e ".[pubsub]"`                    |
| `firestore`        | `uv pip install -e ".[firestore]"`                 |
| `secretmanager`    | `uv pip install -e ".[secretmanager]"`             |
| `processing-worker`| `uv pip install -e ".[processing-worker]"`         |

The `processing-worker` extra installs all the dependencies listed above.
