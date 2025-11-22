import os
from dotenv import load_dotenv

def check_cloud_or_local_run(path_to_env: str = None):
    """
    Checks if the environment is Google Cloud Run or local. If local, loads
    environment variables from a specified .env file.
    Args:
        path_to_env: The absolute path to the .env file to load for local runs.
    Returns:
        bool: True if running in Cloud Run, False if running locally.
    Raises:
        FileNotFoundError: If running locally and the specified .env file does not exist.
        ValueError: If running locally and a key variable (e.g., GCP_PROJECT_ID) is not found after loading.
    """
    # The K_SERVICE environment variable is a reliable indicator of a Cloud Run environment.
    if "K_SERVICE" in os.environ:
        print("✅ Cloud Run environment detected.")
        return True
    else:
        # --- Local Environment Logic ---
        print("   Local environment detected. Attempting to load .env file...")

        if not path_to_env:
            raise ValueError("Running locally, but no 'path_to_env' was provided.")

        if not os.path.isfile(path_to_env):
            raise FileNotFoundError(f"The specified .env file does not exist at: {path_to_env}")

        # load_dotenv returns True if it successfully loaded the file.
        success = load_dotenv(dotenv_path=path_to_env, override=True)

        if not success:
            # This case is rare but could happen if the file is empty or unreadable.
            raise IOError(f"Failed to load the .env file from: {path_to_env}")

        # Verify that a critical variable was loaded.
        if "GCP_PROJECT_ID" not in os.environ:
            raise ValueError(f"Successfully loaded {path_to_env}, but 'GCP_PROJECT_ID' was not found inside.")

        print(f"✅ Successfully loaded environment variables from {os.path.basename(path_to_env)}. LOCAL RUNNING.")
        return False


if __name__ == "__main__":
    # Example of how to use the function
    try:
        # Correct, absolute path to your keys.env file
        env_path = "/home/stas/Dropbox/projects/BigBikeData/power_core/project_env/keys.env"
        is_cloud = check_cloud_or_local_run(path_to_env=env_path)

        if not is_cloud:
            # Now you can safely access your loaded environment variables
            project_id = os.environ.get("GCP_PROJECT_ID")
            print(f"   -> Successfully running locally for project: {project_id}")

    except (FileNotFoundError, ValueError, IOError) as e:
        print(f"❌ Configuration Error: {e}")
