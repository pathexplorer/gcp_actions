# Codebase Structure

## Statistics
- Python files: `27`
- Classes: `6`
- Functions: `41`
- Files with parsing errors: `0`

## File: `gcp_actions/__init__.py`

**Module Description:**
>gcp_actions - Google Cloud Platform utility library.

Provides client factories, secret management, Firestore operations,
Pub/Sub publishing, and GCS blob manipulation with support for
local development via emulators and Cloud Run deployment.

---

## File: `gcp_actions/blob_manipulation.py`

**Module Description:**
>Google Cloud Storage blob manipulation utilities.

Provides filename generation, upload, download, and deletion operations
for GCS blobs with support for Requester Pays and local development.

### Classes
- **`class StorageManipulations`**: Handle GCS upload operations for files, filenames, and raw strings.
  - **`__init__`**: Initialize the storage manipulation handler.

Args:
    bucket_name: GCS bucket name or environment variable containing it.
    gcs_path: Destination path in GCS (e.g., 'folder/file.ext').
    local_path: Local file path or string content to upload.
    content_type_set: MIME type for string uploads (e.g., 'application/json').
  - **`_check_filename`**: Verify that the local file exists before upload.
  - **`_upload_from_file`**: Upload using file object (for large files).
  - **`_upload_from_filename`**: Upload using local file path.
  - **`_upload_from_string`**: Upload raw string content with explicit content type.
  - **`upload_to_gcp_bucket`**: Upload content to GCS based on the specified filetype mode.

### Standalone Functions
- **`generate_unique_filename`**: Generate a unique GCS object path using UUID4 while preserving the file extension.
- **`download_from_gcp_bucket`**: Download a blob from GCS, supporting both file and text (JSON) modes.

Args:
    bucket_name: Bucket name or environment variable name.
    blob_name: Full path to the blob in the bucket.
    local_path: Local destination path (required for filetype='blob').
    filetype: 'blob' to save as file, 'text' to parse as JSON string.
    user_project: Project ID to bill for Requester Pays buckets.

Returns:
    True for successful file download, parsed JSON dict for text mode,
    False if blob not found in blob mode, empty dict if not found in text mode.
- **`delete_blob`**: Delete a GCS blob, returning True on success or if already absent.

Handles permission errors, API failures, and missing blobs gracefully
without raising exceptions. Logs detailed error context for debugging.

Args:
    bucket_name: GCS bucket name or environment variable name.
    blob_name: Full path to the blob in the bucket.
    user_project: Project ID to bill for Requester Pays buckets.

Returns:
    True if deleted or already missing, False on permission/API errors.

---

## File: `gcp_actions/client.py`

**Module Description:**
>GCP client factory with credential caching and service account impersonation.

Provides memoized access to Firestore, Storage, and other Google Cloud clients
with support for workload identity federation via impersonation.

### Standalone Functions
- **`get_env_and_cashed_it`**: Load and cache a required environment variable.

Args:
    variable: Name of the environment variable to retrieve.

Returns:
    The environment variable value.

Raises:
    ValueError: If variable name is empty or not a string.
    EnvironmentError: If the variable is not set.
- **`get_any_client`**: Create and cache a GCP client with optional service account impersonation.

Args:
    client_name: Client type ("firestore" or "storage").
    target_principal: Service account email to impersonate.

Returns:
    Initialized Google Cloud client instance.

Raises:
    ValueError: If client_name is not supported.
    RuntimeError: If client creation fails due to auth/permission issues.
- **`get_bucket`**: Get a GCS bucket handle, resolving bucket name from env var if needed.

Args:
    bucket_name: Bucket name or environment variable name containing it.
    impersonate_sa: Optional service account to impersonate for access.

Returns:
    A storage.Bucket object (lazy, no network call until used).

Raises:
    ValueError: If bucket name is empty.
    RuntimeError: If storage client cannot be created.

---

## File: `gcp_actions/common_utils/__init__.py`

**Module Description:**
>Common utilities for logging, timing, environment detection, and configuration.

Includes:
- handle_logs: Cloud Run JSON / local colored logging setup
- timer: Function and pipeline stage timing utilities
- local_runner: Cloud Run vs local environment detection and .env loading
- validate_entity: Resource name validation
- generate: Signed URL generation for GCS
- init_config: Configuration merging from Firestore, Secret Manager, local overrides

---

## File: `gcp_actions/common_utils/generate.py`

**Module Description:**
>Generate signed download URLs for private GCS objects.

Supports custom filenames via Content-Disposition and service account
impersonation for cross-project access.

### Standalone Functions
- **`g_download_link`**: Generate a V4 signed URL for downloading a private GCS blob.

Args:
    bucket_name: Bucket name or environment variable name.
    blob_name: Full path to the blob in the bucket.
    expiration_minutes: URL validity period (default: 60).
    download_filename: Optional filename for Content-Disposition header.
    impersonate_sa: Service account to impersonate for signing.

Returns:
    Signed URL string.

Raises:
    google_exceptions.NotFound: If bucket or blob doesn't exist.
    google_exceptions.Forbidden: If permissions insufficient.

---

## File: `gcp_actions/common_utils/handle_logs.py`

**Module Description:**
>Logging configuration for Cloud Run (JSON) and local (colored) environments.

Auto-detects Cloud Run via K_SERVICE env var and configures appropriate
formatters and handlers. Silences noisy third-party loggers.

### Classes
- **`class CloudJSONFormatter`**: Format log records as JSON for Cloud Run structured logging.
  - **`format`**: No docstring.
- **`class CustomColorFormatter`**: Apply ANSI colors to log levels for readable local console output.
  - **`format`**: No docstring.

### Standalone Functions
- **`run_handle_logs`**: Configure root logger for Cloud Run (JSON) or local (colored + file).

---

## File: `gcp_actions/common_utils/init_config.py`

**Module Description:**
>Configuration loader merging Firestore, Secret Manager, and local overrides.

Precedence (highest to lowest): local_config.json > Secret Manager > Firestore.
Loads secret names from local config first, then fetches actual secret values.

### Classes
- **`class InjectConfig`**: Load and merge config from Firestore, Secret Manager, and local overrides.
  - **`__init__`**: Initialize config loader with secret sources and project ID resolution.

Args:
    list_of_secret_env_vars: Env var names holding Secret Manager secret IDs.
    list_of_sa_env_vars: Corresponding service accounts for secret access.
    from_firestore: Whether to load base config from Firestore.
  - **`_inject_firestore`**: Load base configuration from Firestore config/local/settings/data.
  - **`add_local_variables`**: Merge local_config.json overrides into the config dict.
  - **`_final_merge`**: Inject merged configuration into environment variables.
  - **`load_and_inject_config`**: Load and merge config: Firestore -> local -> secrets -> local (final).

Precedence: local_config.json > Secret Manager > Firestore.
Local config applied twice: first for secret names, then for final overrides.

### Standalone Functions
- **`_find_project_root`**: Find project root by searching upward for .git directory.
- **`_load_local_config`**: Load local_config.json from project root if it exists.

---

## File: `gcp_actions/common_utils/local_runner.py`

**Module Description:**
>Environment detection and .env loading for local vs Cloud Run execution.

Detects Cloud Run via K_REVISION, loads .env/keys.env from project root
for local development, and provides the application identifier.

### Standalone Functions
- **`_find_env_file_path`**: Search upward from script location for keys.env or .env file.
- **`check_cloud_or_local_run`**: Detect execution environment and load local .env if needed.

Returns:
    Service ID in Cloud Run, or 'local-dev-mode' locally.

---

## File: `gcp_actions/common_utils/timer.py`

**Module Description:**
>Timing utilities for profiling pipeline stages and function execution.

Provides context managers for stage timing, a decorator for function timing,
and a table formatter for logging duration breakdowns.

### Standalone Functions
- **`time_stage`**: Context manager to time a pipeline stage and record duration.
- **`log_duration_table`**: Log a formatted table of stage durations with percentages.
- **`run_timer`**: Decorator to log function execution time if > 0.01s.
  - **`wrapper`**: No docstring.

---

## File: `gcp_actions/common_utils/validate_entity.py`

**Module Description:**
>Validate that environment variable values are lowercase (GCS naming requirement).

All resource names in this project must be lowercase per cloud storage
naming conventions. This validates env var values, not the env var names.

### Standalone Functions
- **`validate_name`**: Validate that an environment variable's value is all lowercase.

Args:
    env_var_name: Name of the environment variable to check.

Returns:
    The validated lowercase value.

Raises:
    ValueError: If env var is not set or value contains uppercase.

---

## File: `gcp_actions/emulators/__init__.py`

**Module Description:**
>Local development emulators for Google Cloud services.

Contains standalone Flask-based emulators for Secret Manager and Firestore
with persistent storage, Dockerfiles, and seeding scripts for local
development parity with production.

---

## File: `gcp_actions/emulators/firestore/__init__.py`

**Module Description:**
>Firestore emulator for local development.

Uses the official Google Cloud Firestore emulator. Seed with initial
config using the included seed.py script.

---

## File: `gcp_actions/emulators/firestore/seed.py`

**Module Description:**
>Seed the local Firestore emulator with initial configuration documents.

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

### Standalone Functions
- **`_resolve_path`**: Resolve a Firestore document reference from collection + dotted/segmented
path string, e.g. collection="config", doc_path="local/settings/data".
Returns a DocumentReference.
- **`_gcloud_project`**: Return the currently configured gcloud project ID.
- **`pull_from_gcp`**: Read config/local/settings/data from a real GCP Firestore project.

Temporarily clears FIRESTORE_EMULATOR_HOST so the Firestore client
connects to real GCP instead of the local emulator.
- **`seed_firestore`**: Seed the Firestore emulator with the given config.
- **`main`**: No docstring.

---

## File: `gcp_actions/emulators/secret_manager/__init__.py`

**Module Description:**
>Secret Manager emulator for local development.

Provides a Flask-based HTTP API compatible with the Secret Manager v1 REST API.
Run via Docker (included Dockerfile) or directly:
    python -m gcp_actions.emulators.secret_manager.emulator

Persists secrets to JSON file for survival across container restarts.

---

## File: `gcp_actions/emulators/secret_manager/emulator.py`

**Module Description:**
>Secret Manager Emulator — lightweight Flask server for local development.

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

### Standalone Functions
- **`_load_store`**: Load the secrets store from disk, or return an empty dict.
- **`_save_store`**: Persist the secrets store to disk.
- **`_secret_path`**: No docstring.
- **`health`**: No docstring.
- **`create_secret`**: Minimal create-secret endpoint.
Body example (we mostly ignore it in emulator):
  {"secret_id": "my-secret", "secret": {"replication": {"automatic": {}}}}
- **`add_secret_version`**: Add a new version to a secret.
Body example:
  {"payload": {"data": "<base64-or-raw-json>"}}
For simplicity the emulator expects data as a JSON string.
- **`access_secret_version`**: Return the payload data for a secret version.
- **`list_secrets`**: List all secret names in the project (debug helper).

---

## File: `gcp_actions/emulators/secret_manager/seed.py`

**Module Description:**
>Seed the local Secret Manager emulator with secrets from keys.env.

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

### Standalone Functions
- **`_load_env_file`**: Parse a KEY=VALUE env file and inject into os.environ (stdlib only).
- **`_find_keys_env`**: Try to locate keys.env from common locations.
- **`emulator_request`**: Send an HTTP request to the emulator and return parsed JSON.
- **`create_or_update_secret`**: Create a secret (if needed) and add a version with the given payload.
- **`main`**: No docstring.

---

## File: `gcp_actions/firestore_box/__init__.py`

**Module Description:**
>Firestore document manipulation utilities.

Provides FirestoreMagic class for high-level document operations
with auto-creation, backup, and field management.

---

## File: `gcp_actions/firestore_box/json_manipulations.py`

**Module Description:**
>Firestore document manipulation utilities with auto-creation and backup support.

Provides high-level CRUD operations for Firestore documents, including
automatic creation from templates, field deletion, backup/prune workflows,
and array-to-CSV conversion.

### Classes
- **`class FirestoreMagic`**: High-level wrapper for Firestore document operations with auto-create and backup.
  - **`__init__`**: Initialize the Firestore document handler.

Args:
    collection_name: Name of the Firestore collection.
    doc_load_name: Document ID or path (e.g., "doc" or "doc/subcoll/subdoc").
    placeholder_full: Default data to create document with if missing.
  - **`_create_resource_if_not_exists`**: Create document from placeholder if it doesn't exist; return snapshot.
  - **`load_firejson`**: Load document as dict, creating from placeholder if missing.
  - **`create_firejson`**: Create a new document with the given data (overwrites if exists).
  - **`set_firejson`**: Set document data with optional merge behavior.

Args:
    data_name: Data to write.
    merge: None to overwrite, True to merge with existing fields.
  - **`backup_document`**: Copy current document to a timestamped backup in {collection}_backups/.
  - **`prune_old_backups`**: Delete old backups beyond max_backups, keeping only the most recent.
  - **`update_firejson`**: Update specific fields in the document (creates if missing).
  - **`delete_field_firejson`**: Delete a field from the document, supporting dot-notation paths.

Args:
    field_for_delete: Field name or dot-separated path (e.g., "nested.field").
    mode: "normal" for top-level field, "path" for nested FieldPath.
  - **`unpack_array_to_csv_string`**: Join array field elements into a separator-delimited string.

---

## File: `gcp_actions/firestore_box/local/manipulation.py`

**Module Description:**
>No module docstring provided.

### Standalone Functions
- **`create_in_firestore`**: No docstring.

---

## File: `gcp_actions/local_test/firestore_as_swith.py`

**Module Description:**
>No module docstring provided.

---

## File: `gcp_actions/local_test/update_json_secrets.py`

**Module Description:**
>Local script to initialize Cloud Secret Manager secrets from .env files.

Loads token_api.env and keys.env, then creates/updates secrets in
Secret Manager for Dropbox and Strava credentials.

### Standalone Functions
- **`get_gcloud_config`**: Get a value from gcloud config (e.g., project ID).

---

## File: `gcp_actions/pubsub.py`

**Module Description:**
>Pub/Sub publishing utilities with environment-aware client selection.

Automatically chooses between gRPC (Cloud Run) and HTTPS (local/emulator)
clients based on runtime environment variables.

### Standalone Functions
- **`publish_message_grpc`**: Publish a message using the high-performance gRPC client (Cloud Run).
- **`publish_message_https`**: Publish a message using the HTTPS REST client (local development).
- **`publish_to_pubsub`**: Publish to Pub/Sub, automatically selecting gRPC or HTTPS based on environment.

Priority:
1. gRPC client if running in Cloud Run (K_SERVICE set)
2. gRPC client if PUBSUB_EMULATOR_HOST is set (local emulator)
3. HTTPS client for local development without emulator

---

## File: `gcp_actions/secret/add_secret_to_JSON_dict.py`

**Module Description:**
>Utility to add or update a single key in a JSON secret in Secret Manager.

Fetches the secret, updates the specified key, and writes a new version.

### Standalone Functions
- **`add_or_update_secret_key`**: Add or update a key-value pair in a JSON secret in Secret Manager.

Args:
    project_id: GCP project ID.
    secret_id: Secret Manager secret ID.
    key: Dictionary key to add or update.
    value: New value for the key.

Returns:
    True if successful, False otherwise.

---

## File: `gcp_actions/secret_manager.py`

**Module Description:**
>Secret Manager client with support for GCP production and local emulator.

Provides typed access to secrets stored as JSON, plain text, or raw bytes.
Supports service account impersonation for cross-project secret access.

### Classes
- **`class SecretManagerClient`**: Client for managing and accessing GCP Secret Manager secrets.

Supports three payload formats:
- JSON (get_secret_json / update_secret_json)
- UTF-8 text (get_secret_string with utf_coding="yes")
- Raw bytes (get_secret_string with utf_coding="no")

Emulator mode is activated by setting SECRET_MANAGER_EMULATOR_HOST
environment variable (e.g., "localhost:8083").
  - **`__init__`**: Initialize the Secret Manager client.

Args:
    project_id: GCP project ID containing the secrets.
    target_sa_email: Optional service account to impersonate for access.
  - **`_create_impersonated_credentials`**: Create short-lived impersonated credentials for a target service account.
  - **`get_secret_json`**: Fetch the latest secret version and parse it as JSON.
  - **`update_secret_json`**: Add a new secret version from a Python dictionary (serialized as JSON).
  - **`get_secret_string`**: Fetch a secret version as UTF-8 string or raw bytes.

Args:
    secret_id: Secret name in Secret Manager.
    version_id: Version to fetch (default: "latest").
    utf_coding: "yes" to decode as UTF-8 string, "no" for raw bytes.

Returns:
    Decoded string or raw bytes depending on utf_coding.

Raises:
    ValueError: If utf_coding is not "yes" or "no", or decoding fails.
  - **`update_secret_string`**: Add a new version to an existing secret with the given string value.
  - **`create_secret`**: Create a new secret with automatic replication if it doesn't exist.
  - **`_emulator_request`**: Send an HTTP request to the emulator and return parsed JSON response.
  - **`_emulator_access_secret`**: Retrieve a secret version from the emulator via GET request.
  - **`_emulator_add_version`**: Add a new version to a secret in the emulator via POST request.
  - **`_emulator_create_secret`**: Create a new secret in the emulator via POST request.

---

## File: `tests/__init__.py`

**Module Description:**
>No module docstring provided.

---

## File: `tests/data_processor.py`

**Module Description:**
>No module docstring provided.

### Standalone Functions
- **`unpack_firestore_array_to_csv_string`**: Unpacks a specific array field from a Firestore document into a
single string with elements separated by the specified separator.

Args:
    document: The Firestore document dictionary (e.g., {"id": 1, "tags": ["a", "b"]}).
    array_field_key: The key of the array field to unpack (e.g., "tags").
    separator: The character to use for separation (default is comma ',').

Returns:
    A comma-separated string of the array elements.
    Returns an empty string if the key is not found or the value is not a list.

---

## File: `tests/main.py`

**Module Description:**
>No module docstring provided.

---
