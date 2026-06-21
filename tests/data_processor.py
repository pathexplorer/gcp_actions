# data_processor.py

from typing import Dict, Any, List


def unpack_firestore_array_to_csv_string(
        document: Dict[str, Any],
        array_field_key: str,
        separator: str = ','
) -> str:
    """
    Unpacks a specific array field from a Firestore document into a
    single string with elements separated by the specified separator.

    Args:
        document: The Firestore document dictionary (e.g., {"id": 1, "tags": ["a", "b"]}).
        array_field_key: The key of the array field to unpack (e.g., "tags").
        separator: The character to use for separation (default is comma ',').

    Returns:
        A comma-separated string of the array elements.
        Returns an empty string if the key is not found or the value is not a list.
    """

    # 1. Necessary checks and error handling (as requested)
    if not isinstance(document, dict):
        # Using a more specific exception for clarity
        raise TypeError("Input 'document' must be a dictionary.")

    # 2. Extract the array
    array_data: Any = document.get(array_field_key)

    # 3. Validation: Ensure the data is a list of strings
    if not isinstance(array_data, list):
        # Log a warning or handle as per project requirements
        print(f"Warning: Field '{array_field_key}' is missing or not a list. Returning empty string.")
        return ""

    # 4. Check if all elements are strings (optional, but good practice for join)
    if not all(isinstance(item, str) for item in array_data):
        # Convert non-string items to string defensively
        array_data = [str(item) for item in array_data]

    # 5. Core logic: Use str.join() for efficient flattening
    # The result is a single string line
    return separator.join(array_data)