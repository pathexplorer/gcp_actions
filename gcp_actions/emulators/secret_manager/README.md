# Secret Manager Emulator — Local Development

Replaces `keys.env` for local secret management, mirroring production's use
of Google Cloud Secret Manager.

Package location: `gcp_actions.emulators.secret_manager`

## Quick Start

```bash
cd BigBikeData/power_core

# 1. Start the emulator + seed it with secrets from keys.env
./local_dev.sh start

# 2. Export the emulator env vars into your shell
eval $(./local_dev.sh env)

# 3. Run the app — it will read secrets from the emulator automatically
python power_core/main.py
```

## What Happens Under the Hood

1. `compose.yaml` starts a lightweight Flask Secret Manager emulator in a
   Podman container (port 8083). Secrets are persisted in a Podman volume.
2. `seed.py` reads your existing `keys.env` and pushes the relevant
   secrets into the emulator using its REST API.
3. `local_dev.sh env` prints `export` commands for:
   - `SECRET_MANAGER_EMULATOR_HOST=localhost:8083`
   - `GCP_PROJECT_ID=local-test-project`
4. When `SecretManagerClient` (in `gcp_actions`) detects
   `SECRET_MANAGER_EMULATOR_HOST`, it switches from gRPC (real GCP) to
   simple HTTP calls against the emulator — **zero code changes needed**
   in your application.

## Files

| File | Purpose |
|------|---------|
| `emulator.py` | Flask REST API emulating Secret Manager |
| `seed.py` | Seeds emulator from `keys.env` (supports `--keys-env`, `--emulator-host`, `--project`) |
| `Dockerfile` | Container image for the emulator |
| `../../../power_core/compose.yaml` | Podman Compose definition (build context points here) |
| `../../../power_core/local_dev.sh` | Helper: start/stop/seed/env |
| `../../secret_manager.py` | `SecretManagerClient` (auto-detects emulator via `SECRET_MANAGER_EMULATOR_HOST`) |

## Running the emulator standalone (without Compose)

```bash
# Directly:
SM_EMULATOR_DATA_FILE=/tmp/secrets.json python -m gcp_actions.emulators.secret_manager.emulator

# Seed it:
python -m gcp_actions.emulators.secret_manager.seed --keys-env /path/to/keys.env
```

## API Surface (Emulator)

The emulator implements exactly the three operations used by the app:

| Operation | HTTP |
|-----------|------|
| Create secret | `POST /v1/projects/{p}/secrets` |
| Add version | `POST /v1/projects/{p}/secrets/{s}:addVersion` |
| Access version | `GET /v1/projects/{p}/secrets/{s}/versions/{v}` |

## Managing Secrets

- **Initial seed**: `cd BigBikeData/power_core && ./local_dev.sh seed` (reads `keys.env`)
- **Add/update a secret**: Use the `update_secret_json` method in Python
  (same as production) — it works transparently against the emulator.
- **View secrets**: `curl http://localhost:8083/v1/projects/local-test-project/secrets`
- **Reset data**: `podman-compose -f compose.yaml down -v` (destroys volume)

## Migration Path

1. ✅ Keep `keys.env` as the source of truth for now.
2. ✅ Run `seed.py` to populate the emulator.
3. The app uses `SecretManagerClient` → emulator (same API as production).
4. Eventually, stop relying on `keys.env` and manage secrets via the emulator
   (or a real dev GCP project) directly.
