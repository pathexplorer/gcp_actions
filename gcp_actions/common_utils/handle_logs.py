import logging
import os
import sys
import json
import warnings
from functools import lru_cache


# We don't need CloudLoggingHandler anymore for Cloud Run
# from google.cloud.logging import Client as LogClient
# from google.cloud.logging.handlers import CloudLoggingHandler

# --- 1. Formatter for Cloud (JSON) ---
class CloudJSONFormatter(logging.Formatter):
    """
    Formats logs as JSON, so Google Cloud Run can capture them from stdout
    and parse severity correctly.
    """

    def format(self, record):
        # Create the dictionary for JSON output
        log_record = {
            "severity": record.levelname,  # Key for GCP to read level
            "message": record.getMessage(),
            "logger": record.name,
            "filename": record.filename,
            "lineno": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


# --- 2. Formatter for Local (Colors) ---
class CustomColorFormatter(logging.Formatter):
    """
    A custom formatter that applies ANSI colors to log messages based on level.
    """
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    BOLD_RED = "\033[31;1m"
    RESET = "\033[0m"

    DEFAULT_FORMAT = "%(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: DEFAULT_FORMAT,
        logging.INFO: GREEN + DEFAULT_FORMAT + RESET,
        logging.WARNING: YELLOW + DEFAULT_FORMAT + RESET,
        logging.ERROR: RED + DEFAULT_FORMAT + RESET,
        logging.CRITICAL: BOLD_RED + DEFAULT_FORMAT + RESET
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


@lru_cache(maxsize=1)
def run_handle_logs():
    """
    Configures the root logger.
    - Cloud: Logs JSON to stdout (best practice for Cloud Run).
    - Local: Logs with colors to console and file.
    """
    # K_SERVICE is a standard env var in Cloud Run
    IS_CLOUD = "K_SERVICE" in os.environ

    root_log = logging.getLogger()
    log_level = os.environ.get("LOGGING_LEVEL", "INFO").upper()

    try:
        root_log.setLevel(log_level)
    except ValueError:
        # Fallback if you accidentally typed a wrong name in the Env Var
        root_log.setLevel(logging.INFO)
        logging.error(f"Invalid LOGGING_LEVEL '{log_level}' specified. Defaulting to INFO.")

    # Clear existing handlers to prevent duplicates
    if root_log.hasHandlers():
        root_log.handlers.clear()

    if IS_CLOUD:
        # --- CLOUD CONFIGURATION ---
        # Use StreamHandler (stdout) with JSON Formatter.
        # Cloud Run agent picks this up automatically.
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(CloudJSONFormatter())
        root_log.addHandler(handler)
        logging.info("Cloud Logging configured: JSON to stdout.")

    else:
        # --- LOCAL CONFIGURATION ---
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(CustomColorFormatter())
        root_log.addHandler(console_handler)

        file_handler = logging.FileHandler('my_log_file.log', mode='a')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        root_log.addHandler(file_handler)
        logging.info("Local logging configured (Console + File).")

    # Silence noisy loggers
    for noisy_logger in ["telethon", "urllib3", "asyncio", "google.cloud", "google.auth"]:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)

    # Suppress warnings
    warnings.filterwarnings("ignore", category=UserWarning)