"""
Validate that environment variable values are lowercase (GCS naming requirement).

All resource names in this project must be lowercase per cloud storage
naming conventions. This validates env var values, not the env var names.
"""

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_name(env_var_name: str) -> str:
    """Validate that an environment variable's value is all lowercase.

    Args:
        env_var_name: Name of the environment variable to check.

    Returns:
        The validated lowercase value.

    Raises:
        ValueError: If env var is not set or value contains uppercase.
    """
    if env_var_name is None:
        # Error case: Variable not defined
        raise ValueError(
            f"Configuration Error: Environment variable '{env_var_name}' is not set. "
            f"Please ensure it is exported in your environment."
        )

    # 2. Validation Check: Content must be all lowercase
    # (as per your requirement, often due to cloud storage naming rules)
    if not env_var_name.islower():
        # Error case: Value contains uppercase/mixed characters
        incorrect_value = env_var_name

        raise ValueError(
            f"The current value is '{incorrect_value}'. "
            "This names MUST be all lowercase (e.g., 'my-project-bucket-123', 'project-data-lake-raw'). " 
            "Check your environment variable configuration and try again."
        )

    # 3. Validation successful, return the clean lowercase name
    logger.info(f"Validated '{env_var_name}' as lowercase.")
    return env_var_name

#
# # --- Example Usage Demonstration ---
# def main():
#     """
#     Demonstrates the usage of the validate_bucket_name function with
#     success and failure scenarios.
#     """
#     print("--- Bucket Name Validation Script Demonstration ---")
#
#     # Define the environment variable we will check
#     ENV_VAR = "GCS_RAW_BUCKET"
#
#     # --- Test Case 1: Success (All Lowercase) ---
#     print(f"\n[Test 1] Successful Case: '{ENV_VAR}' is set to a valid lowercase name.")
#
#     try:
#         # Simulate setting the environment variable with a correct value
#         os.environ[ENV_VAR] = "project-data-lake-raw"
#
#         # 1. Validate and retrieve the name
#         validated_bucket = validate_bucket_name(ENV_VAR)
#
#         print(f"   Success! The validated bucket name is: '{validated_bucket}'")
#
#         # 2. Simulate the original code usage:
#         print(f"   Action: Use bucket in API call: {validated_bucket}")
#         # bucket = get_bucket(validated_bucket, user_project=user_project)
#
#     except ValueError as e:
#         print(f"   FAILURE (Unexpected): {e}")
#
#     # Clean up environment variable for subsequent tests
#     del os.environ[ENV_VAR]
#
#     # --- Test Case 2: Failure (Uppercase/Mixed Case) ---
#     print(f"\n[Test 2] Failure Case: '{ENV_VAR}' is set with mixed case (an expected error).")
#
#     try:
#         # Simulate setting the variable with mixed case, which should fail validation
#         os.environ[ENV_VAR] = "Project-Data-Lake-V1"
#
#         # Validate and retrieve (this should raise the error)
#         validated_bucket = validate_bucket_name(ENV_VAR)
#
#         print(f"   FAILURE (Error was expected but not raised): Validated: '{validated_bucket}'")
#
#     except ValueError as e:
#         # This is the expected and desired outcome for this test
#         print(f"   SUCCESS (Expected Error Raised): {e}")
#
#     # Clean up environment variable
#     del os.environ[ENV_VAR]
#
#     # --- Test Case 3: Failure (Variable Not Set) ---
#     print(f"\n[Test 3] Failure Case: '{ENV_VAR}' is not set (an expected error).")
#
#     # Ensure the variable is not set
#     if ENV_VAR in os.environ:
#         del os.environ[ENV_VAR]
#
#     try:
#         # Validate and retrieve (this should raise the error)
#         validated_bucket = validate_bucket_name(ENV_VAR)
#
#         print(f"   FAILURE (Error was expected but not raised): Validated: '{validated_bucket}'")
#
#     except ValueError as e:
#         # This is the expected and desired outcome for this test
#         print(f"   SUCCESS (Expected Error Raised): {e}")
#

if __name__ == "__main__":
    validate_name("gbрbgbb")