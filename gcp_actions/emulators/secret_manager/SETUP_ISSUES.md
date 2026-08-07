# Setup Issues — Secret Manager Emulator

This documents every problem encountered while setting up the Secret Manager
emulator and why a single `podman-compose up` didn't work out of the box.

---

## 1. Podman Compose not installed

`podman-compose` was not available on the system. Workaround: use bare
`podman build / podman run` commands instead.

**Why it matters:** Compose would be the ideal one-command setup, but
requiring an extra package defeats the purpose of a lightweight emulator.

---

## 2. Port forwarding broken — `Connection reset by peer` on POST

```
✅ GET  /health        → works
❌ POST /secrets       → curl: (56) Recv failure: Connection reset by peer
```

**Root cause:** Podman's rootless networking (pasta) was dropping TCP
connections on POST requests. Gunicorn was binding to `0.0.0.0:8083`
(IPv4 only), but `localhost` resolved to `::1` (IPv6) first, causing
a mismatch. Even when forced to IPv4, the connection was unreliable.

**Fix:** Use `--network host` so the container shares the host's network
stack — no port forwarding needed.

```bash
podman run -d --name bigbikedata-sm-emulator --network host -e PORT=8083 sm-emulator
```

**Trade-off:** Host networking means the container can access all host ports.
Acceptable for a local dev tool; not for production.

---

## 3. Gunicorn replaced with Flask dev server

The original Dockerfile used gunicorn for production-style serving. After
switching to `--network host`, gunicorn's IPv4-only binding (`0.0.0.0`)
caused connection issues. Flask's built-in dev server handles both IPv4
and IPv6 transparently.

**Fix:** Replaced `CMD ["sh", "-c", "gunicorn ..."]` with `CMD ["python", "emulator.py"]`.

**Trade-off:** Flask dev server is single-threaded. Fine for local dev
(one user, low traffic). If multiple concurrent requests are needed,
gunicorn can be re-added with `--bind [::]:$PORT`.

---

## 4. `python-dotenv` not available system-wide

`seed.py` originally imported `from dotenv import load_dotenv`, but the
system Python is externally managed (PEP 668) and didn't have it.

**Fix:** Replaced with a 12-line stdlib-only parser using `re.match`.
Zero external dependencies — works on any Python 3.8+.

---

## 5. Port 8083 in use by stale container

After the foreground container was stopped with Ctrl+C, a leftover
container (`adoring_bartik`) was still holding port 8083, preventing
new containers from starting. Status showed "Created" (never reached "Up").

**Fix:** `podman rm -f` all old sm-emulator containers before starting a new one.

---

## 6. Secret structure mismatch between `keys.env` and production

| Assumption | Reality |
|---|---|
| `APP_JSON_KEYS` contains only `APP_JSON_KEYS_VALUE` | Contains 15+ keys: `GCP_PROJECT_ID`, `BREVO_API_KEY`, SMTP settings, etc. |
| `SEC_DROPBOX` and `SEC_STRAVA` are separate secrets | Both point to the same `dropbox-secrets` (combined Dropbox + Strava + PG) |
| All secrets are JSON | `flask-secret-key` is a single string (not JSON) |

**Fix:** `SECRET_CONFIG_MAP` in `seed.py` now matches the real production
structure. Deduplication logic prevents overwriting when two env vars
point to the same secret name. Single-key secrets are handled separately
via `SINGLE_KEY_SECRETS`.

---

## 7. `APP_JSON_KEYS` pointer missing from `local_config.json`

Without `keys.env`, the app needs to know the **name** of the main config
secret (`APP_JSON_KEYS=fullstack-app-json-keys`). This pointer now lives in
`local_config.json`, which is already loaded by `InjectConfig` before secrets.

**Fix:** Added `"APP_JSON_KEYS": "fullstack-app-json-keys"` to `local_config.json`.

---

## Summary: Why not one action?

| Problem | Category |
|---------|----------|
| No `podman-compose` | Missing package |
| Pasta port forwarding broken | Podman rootless networking bug |
| Gunicorn IPv4 vs IPv6 mismatch | Container networking |
| `python-dotenv` not installed | System Python restrictions |
| Stale container port conflict | State management |
| Secret structure mismatch | Configuration discovery |
| Missing secret pointer | Configuration discovery |

These are all **environment-specific** issues (Podman version, system Python
policy, project-specific secret structure). A generic one-liner can't
account for all of them. The current `podman build && podman run --network host`
approach is the most reliable across environments.
