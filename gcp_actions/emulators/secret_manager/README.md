# Secret Manager Emulator — Local Development

Replaces `keys.env`-based local secret management with a lightweight Secret
Manager emulator, mirroring production's use of Google Cloud Secret Manager.

Package location: `gcp_actions.emulators.secret_manager`

## Quick Start

```bash
cd gcp_actions/gcp_actions/emulators/secret_manager

# 1. Build the emulator image
podman build -t sm-emulator .

# 2. Start the emulator in the background
podman run -d --name bigbikedata-sm-emulator --network host -e PORT=8083 sm-emulator

# 3. Seed secrets from keys.env (one-time)
python seed.py --keys-env keys.env

# 4. Verify
curl http://localhost:8083/v1/projects/local-test-project/secrets
```

**⚠️  `--network host` is required** — Podman's rootless port forwarding
(pasta/slirp4netns) repeatedly dropped connections on POST requests.
Host networking avoids this entirely.

## What Happens Under the Hood

1. The emulator is a simple Flask app that stores secrets as JSON on disk
   (persisted via a Podman volume or bind mount).
2. `seed.py` reads your existing `keys.env` flat file and pushes each
   secret into the emulator as the correct JSON payload.
3. Set `SECRET_MANAGER_EMULATOR_HOST=localhost:8083` so the app uses the
   emulator instead of real GCP Secret Manager.
4. `SecretManagerClient` (in `gcp_actions`) auto-detects the env var and
   switches from gRPC to simple HTTP calls — **zero application code changes**.

## Files

| File | Purpose |
|------|---------|
| `emulator.py` | Flask REST API emulating Secret Manager (3 endpoints) |
| `seed.py` | Seeds emulator from `keys.env` (zero external deps) |
| `Dockerfile` | Container image (Flask dev server, no gunicorn) |
| `../../secret_manager.py` | `SecretManagerClient` — auto-detects emulator via `SECRET_MANAGER_EMULATOR_HOST` |

## Emulator API

| Operation | HTTP |
|-----------|------|
| Create secret | `POST /v1/projects/{p}/secrets` |
| Add version | `POST /v1/projects/{p}/secrets/{s}:addVersion` |
| Access version | `GET /v1/projects/{p}/secrets/{s}/versions/{v}` |
| Health check | `GET /health` |
| List secrets | `GET /v1/projects/{p}/secrets` |

## Managing Secrets

- **Initial seed**: `python seed.py --keys-env keys.env`
- **Update a secret**: `curl -X POST .../addVersion` or use app's `update_secret_json()`
- **View secrets**: `curl http://localhost:8083/v1/projects/local-test-project/secrets`
- **Reset everything**: `podman rm -f bigbikedata-sm-emulator && podman volume rm sm-emulator-data`

## Migration Path

1. ✅ `keys.env` is used **once** to seed the emulator.
2. ✅ `local_config.json` holds the `APP_JSON_KEYS` pointer (secret name).
3. ✅ App reads all other config from the emulator, matching production.
4. ✅ After seeding, `keys.env` is no longer needed — archive or delete it.
