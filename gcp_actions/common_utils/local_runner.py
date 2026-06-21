import os
import sys
import logging
from dotenv import load_dotenv
from functools import lru_cache
from pathlib import Path

# Get a module-specific logger
logger = logging.getLogger(__name__)

# --- CENTRALIZED CONSTANTS ---
CLOUD_RUN_SERVICE_VAR = 'K_SERVICE'
CLOUD_RUN_REVISION_VAR = 'K_REVISION'
CRITICAL_LOCAL_VAR = 'GCP_PROJECT_ID' # A key variable expected to be in the .env file
DEFAULT_APP_ID = 'local-dev-mode'

def _find_env_file_path() -> str | None:
    """
    Manually searches for 'keys.env' or '.env' by walking up the directory tree
    from the main script's location. This is a robust way to find the project root.
    """
    try:
        # Start searching from the directory of the script that was executed.
        current_dir = Path(sys.argv[0]).resolve().parent
    except (IndexError, AttributeError):
        # Fallback for environments where sys.argv is not available (e.g., some test runners)
        current_dir = Path.cwd()

    # Walk up the directory tree until the root is reached
    while current_dir != current_dir.parent:
        keys_env_path = current_dir / 'keys.env'
        if keys_env_path.is_file():
            return str(keys_env_path)
            
        dotenv_path = current_dir / '.env'
        if dotenv_path.is_file():
            return str(dotenv_path)
            
        current_dir = current_dir.parent
    
    return None # Return None if no file was found

@lru_cache(maxsize=1)
def check_cloud_or_local_run() -> str:
    """
    Determines the execution environment (Cloud Run or Local).

    - In Cloud Run, it returns the service ID.
    - Locally, it ensures environment variables are loaded by searching
      upwards from the main script's path for a 'keys.env' or '.env' file.
    """
    # 1. Primary Check: Cloud Run Environment
    if os.environ.get(CLOUD_RUN_REVISION_VAR):
        app_id = os.environ.get(CLOUD_RUN_SERVICE_VAR, "CR_ID_MISSING")
        logger.info(f"✅ Cloud Run environment detected. Service ID: {app_id}")
        return app_id

    # 2. Local Environment Logic
    logger.info("Local environment detected. Verifying environment variables...")

    if CRITICAL_LOCAL_VAR in os.environ:
        logger.info("✅ Environment variables already loaded from shell.")
        return DEFAULT_APP_ID

    # Manually find the .env file path
    env_path = _find_env_file_path()

    if not env_path:
        raise FileNotFoundError(
            "Running locally, but no environment variables were pre-loaded and "
            "a 'keys.env' or '.env' file could not be found by searching up from the execution path."
        )

    logger.info(f"Found .env file at: {env_path}. Loading variables...")
    success = load_dotenv(dotenv_path=env_path, override=False)

    if not success:
        raise IOError(f"Failed to load the .env file from: {env_path}")

    if CRITICAL_LOCAL_VAR not in os.environ:
        raise ValueError(
            f"Successfully loaded {os.path.basename(env_path)}, but the critical "
            f"variable '{CRITICAL_LOCAL_VAR}' was not found inside."
        )

    logger.info(f"✅ Successfully loaded environment variables from {os.path.basename(env_path)}.")
    return DEFAULT_APP_ID
