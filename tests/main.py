# main.py

from data_processor import unpack_firestore_array_to_csv_string

# --- Example Firestore Document ---
# Assume this is the document fetched from Firestore
firestore_doc = {
    "doc_id": "product_123",
    "status": "active",
    "tags": ["electronics", "charger", "fast_charging", "usb_c"],
    "prices": [10.99, 12.99, 9.99] # Example of a non-string array to handle
}

# --- Usage 1: Unpack the 'tags' field (String Array) ---
FIELD_NAME = "tags"
tags_string = unpack_firestore_array_to_csv_string(firestore_doc, FIELD_NAME)

print(f"--- Unpacking '{FIELD_NAME}' ---")
print(f"Original Array: {firestore_doc[FIELD_NAME]}")
print(f"Result String: {tags_string}")

# Expected Output: electronics,charger,fast_charging,usb_c

print("\n" + "-"*20 + "\n")

# --- Usage 2: Unpack the 'prices' field (Non-String Array) ---
FIELD_NAME_2 = "prices"
# The function will defensively convert numbers to strings before joining
prices_string = unpack_firestore_array_to_csv_string(firestore_doc, FIELD_NAME_2, separator='|')

print(f"--- Unpacking '{FIELD_NAME_2}' with '|' separator ---")
print(f"Original Array: {firestore_doc[FIELD_NAME_2]}")
print(f"Result String: {prices_string}")

# Expected Output: 10.99|12.99|9.99