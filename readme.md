# gcp-actions

Reusable Google Cloud helper functions. It serve two projects: [BigBikeData](https://github.com/pathexplorer/BigBikeData) and [telegram_parcer](https://github.com/pathexplorer/telegram_parcer)

## Requirements

- Python >= 3.12
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

### 3. Install all extras at once (recommended)

```bash
uv pip install -e ".[processing-worker]"
```

### 4. Install with specific extras

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

Two local emulators are included for development without real GCP credentials.
Source: `gcp_actions/emulators/`.

| Emulator | Source | Default port |
|----------|--------|---------------|
| Secret Manager | `emulators/secret_manager/` | `8083` |
| Firestore | `emulators/firestore/` | `8085` |

### Prerequisites

- **Podman** (or Docker) installed and running
- **Python 3.12+**
- **Ports 8083 and 8085** free on localhost

### Quick standalone start

**Secret Manager emulator:**
```bash
cd gcp_actions/emulators/secret_manager
podman build -t sm-emulator .
podman run -d --name sm-emulator -p 8083:8083 sm-emulator
curl http://localhost:8083/health           # verify
```

**Firestore emulator:**
```bash
podman pull docker.io/google/cloud-sdk:emulators
podman run -d --name fs-emulator -p 8085:8085 \
    docker.io/google/cloud-sdk:emulators \
    gcloud beta emulators firestore start --host-port=0.0.0.0:8085
```

### Seeding data

Each emulator has a `seed.py` script to populate initial data:

```bash
# Secret Manager — seed secrets from a KEY=VALUE env file
python gcp_actions/emulators/secret_manager/seed.py --keys-env /path/to/keys.env

# Firestore — pull from real GCP (standard workflow, GCP is source of truth)
# Auto-detects project from gcloud config, or pass it explicitly:
gcloud auth application-default login   # one-time
FIRESTORE_EMULATOR_HOST=localhost:8085 \
    python gcp_actions/emulators/firestore/seed.py --from-project

# Or with explicit project ID:
# FIRESTORE_EMULATOR_HOST=localhost:8085 \
#     python gcp_actions/emulators/firestore/seed.py \
#     --from-project my-real-gcp-project

# Firestore — with local overrides on top of GCP config
FIRESTORE_EMULATOR_HOST=localhost:8085 \
    python gcp_actions/emulators/firestore/seed.py \
    --from-project my-real-gcp-project \
    --defaults-json '{"LOGGING_LEVEL":"DEBUG"}'

# Firestore — placeholder defaults (what host projects seed before pulling from GCP)
FIRESTORE_EMULATOR_HOST=localhost:8085 \
    python gcp_actions/emulators/firestore/seed.py
```

### Integrating into your project

How you wire the emulators into your app's startup depends on your project.
For a complete integration example — pod management, encrypted secrets volume,
ngrok tunnel for webhook testing, and a single-command `local_dev.sh` workflow —
see the **[power_core README](https://github.com/pathexplorer/BigBikeData/blob/main/power_core/README.md#local-development)**.

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
