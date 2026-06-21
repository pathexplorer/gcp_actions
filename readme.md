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

## Available extras

| Extra              | Install command                                    |
|--------------------|----------------------------------------------------|
| `storage`          | `uv pip install -e ".[storage]"`                   |
| `pubsub`           | `uv pip install -e ".[pubsub]"`                    |
| `firestore`        | `uv pip install -e ".[firestore]"`                 |
| `secretmanager`    | `uv pip install -e ".[secretmanager]"`             |
| `processing-worker`| `uv pip install -e ".[processing-worker]"`         |

The `processing-worker` extra installs all the dependencies listed above.
