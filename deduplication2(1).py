import json
import hashlib
import os
import re
from pathlib import Path
import unicodedata
import string      # Needed for standard punctuation removal
# Corrected: Added Optional to the import list
from typing import Set, Dict, Any, Generator, List, Optional

# --- Updated Normalization Function (Language Preserving) ---
def normalize_text(text: str) -> str:
    """
    Normalize text gently: lowercase, replace digits with '0',
    remove standard ASCII punctuation, normalize whitespace.
    Leaves non-ASCII letters and accents intact.
    """
    if not isinstance(text, str):
        return ""

    # 1. Lowercase
    text = text.lower()

    # 2. Replace all consecutive digits with a single '0'
    text = re.sub(r'\d+', '0', text)

    # 3. Remove standard ASCII punctuation
    translator = str.maketrans('', '', string.punctuation)
    text = text.translate(translator)

    # 4. Normalize whitespace (replace multiple spaces/tabs/newlines with single space)
    text = re.sub(r'\s+', ' ', text).strip()

    return text

# --- Hashing Function ---
def calculate_hash(text: str) -> str:
    """
    Calculates the SHA-256 hash of the input text.
    """
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

# --- JSONL Handling Functions ---
def read_jsonl(file_path: Path) -> Generator[Dict[str, Any], None, None]:
    """
    Reads a JSONL file line by line and yields each JSON object.
    Handles potential JSON decoding errors.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            line_num = 0
            for line in f:
                line_num += 1
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        print(f"Warning: Skipping invalid JSON line #{line_num} in {file_path}.")
    except FileNotFoundError:
        print(f"Error: Input file not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

def write_jsonl(file_path: Path, data: Dict[str, Any]):
    """
    Appends a JSON object as a line to a JSONL file. Ensures parent dir exists.
    """
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'a', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
            f.write('\n')
    except Exception as e:
        print(f"Error writing to {file_path}: {e}")

# --- Main Function (Two-Pass Cross-Shard Deduplication) ---
# Corrected: Added Optional import, so this type hint is now valid
def main(input_folder: str, output_folder: str, text_field: str, key_to_remove: Optional[str]):
    """
    Main function to find JSONL files, collect all unique hashes across shards (Pass 1),
    and then write out the first occurrence of each unique record (Pass 2).
    Uses updated, language-preserving normalization.
    """
    input_dir = Path(input_folder)
    output_dir = Path(output_folder)

    if not input_dir.is_dir():
        print(f"Error: Input folder '{input_folder}' not found or is not a directory.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Input folder: {input_dir.resolve()}")
    print(f"Output folder: {output_dir.resolve()}")

    print(f"Normalizing based on field: '{text_field}'")
    if key_to_remove:
        print(f"Removing key from output: '{key_to_remove}'")

    jsonl_files = sorted(list(input_dir.glob('*.jsonl')))

    if not jsonl_files:
        print(f"No .jsonl files found in '{input_folder}'.")
        return

    print(f"Found {len(jsonl_files)} .jsonl files to process.")

    # --- Pass 1: Collect all unique hashes ---
    print("\n--- Starting Pass 1: Collecting unique hashes ---")
    global_unique_hashes: Set[str] = set()
    total_records_read_pass1 = 0
    skipped_missing_field_pass1 = 0
    for input_file_path in jsonl_files:
        print(f"Reading {input_file_path} for hashes...")
        records_in_file = 0
        for record in read_jsonl(input_file_path):
            total_records_read_pass1 += 1
            records_in_file += 1
            if text_field not in record:
                skipped_missing_field_pass1 += 1
                continue

            original_text = record[text_field]
            normalized_text = normalize_text(original_text)

            if normalized_text:
                content_hash = calculate_hash(normalized_text)
                global_unique_hashes.add(content_hash)

        print(f"  Processed {records_in_file} records in {input_file_path}.")

    print(f"--- Finished Pass 1 ---")
    print(f"Read {total_records_read_pass1} records total.")
    if skipped_missing_field_pass1 > 0:
         print(f"Skipped {skipped_missing_field_pass1} records missing the '{text_field}' field.")
    print(f"Collected {len(global_unique_hashes)} unique text hashes across all shards.")
    estimated_entry_size_bytes = 32 + 24 # Rough estimate (hash + overhead)
    estimated_memory_mb = (len(global_unique_hashes) * estimated_entry_size_bytes) / (1024 * 1024)
    print(f"Estimated memory usage for storing hashes: ~{estimated_memory_mb:.2f} MB")
    print("\nNote: If the number of unique hashes is very large, this might consume significant memory.")

    # --- Pass 2: Write unique items ---
    print("\n--- Starting Pass 2: Writing unique records ---")
    hashes_written: Set[str] = set()
    total_records_written_pass2 = 0
    skipped_missing_field_pass2 = 0
    skipped_empty_normalized_pass2 = 0

    for input_file_path in jsonl_files:
         output_file_path = output_dir / input_file_path.name
         if output_file_path.exists():
            try:
                output_file_path.unlink()
            except OSError as e:
                print(f"Warning: Could not clear existing output file {output_file_path}. {e}")


    for input_file_path in jsonl_files:
        output_file_path = output_dir / input_file_path.name
        records_written_this_file = 0
        print(f"Processing {input_file_path} for writing...")

        for record in read_jsonl(input_file_path):
            if text_field not in record:
                skipped_missing_field_pass2 += 1
                continue

            original_text = record[text_field]
            normalized_text = normalize_text(original_text)

            if normalized_text:
                content_hash = calculate_hash(normalized_text)
                if content_hash in global_unique_hashes and content_hash not in hashes_written:
                    if key_to_remove:
                        record.pop(key_to_remove, None)
                    write_jsonl(output_file_path, record)
                    hashes_written.add(content_hash)
                    records_written_this_file += 1
                    total_records_written_pass2 += 1
            else:
                skipped_empty_normalized_pass2 +=1

        print(f"  Wrote {records_written_this_file} unique records to {output_file_path}.")

    print(f"--- Finished Pass 2 ---")
    if skipped_missing_field_pass2 > 0:
         print(f"Skipped {skipped_missing_field_pass2} records missing the '{text_field}' field during writing pass.")
    if skipped_empty_normalized_pass2 > 0:
         print(f"Skipped {skipped_empty_normalized_pass2} records with empty normalized text during writing pass.")
    print(f"Wrote {total_records_written_pass2} unique records in total across all shards.")
    print("\nProcessing complete.")


# --- Example Usage ---
if __name__ == "__main__":
    # ==================================================
    # --- Configuration ---
    # V V V V V V V V V V V V V V V V V V V V V V V V V V

    INPUT_FOLDER_PATH = "/home/blu-bridge002/Desktop/pipe/saturdaytest"
    OUTPUT_FOLDER_PATH = "/home/blu-bridge002/Desktop/pipe/y"

    # Ensure this matches the key in your JSONL files
    TEXT_FIELD_TO_NORMALIZE = "raw_content"

    # Set to the key you want removed, or None to remove nothing
    KEY_TO_REMOVE_FROM_OUTPUT = "cc_segment"
    # Example: KEY_TO_REMOVE_FROM_OUTPUT = None

    # ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^
    # --- END OF CONFIGURATION ---
    # ==================================================

    print("Starting deduplication process...")
    main(INPUT_FOLDER_PATH, OUTPUT_FOLDER_PATH, TEXT_FIELD_TO_NORMALIZE, KEY_TO_REMOVE_FROM_OUTPUT)
    print("Deduplication process finished.")