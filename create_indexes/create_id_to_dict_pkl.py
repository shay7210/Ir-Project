import pandas as pd
import pickle
import gcsfs
import random
from google.cloud import storage

# --- CONFIGURATION ---
BUCKET_NAME = 'wikipidia_ir_project'
KEY_FILE_PATH = '../my_gcp_key.json'
OUTPUT_FILE = '../inverted_indexes_pkls/id_to_title.pkl'


def create_dict_debug():
    print("🚀 Authenticating...")
    fs = gcsfs.GCSFileSystem(project='ir-project-2025', token=KEY_FILE_PATH)

    # Get files
    files = fs.glob(f"{BUCKET_NAME}/*.parquet")
    print(f"✅ Found {len(files)} parquet files.")

    full_dict = {}

    for i, file_path in enumerate(files):
        try:
            print(f"\n[{i + 1}/{len(files)}] Reading {file_path}...")

            # Read Data
            df = pd.read_parquet(f"gs://{file_path}",
                                 columns=['id', 'title'],
                                 storage_options={'token': KEY_FILE_PATH})

            # --- DEBUGGING PRINTS ---
            if i == 0:
                print(f"   📊 Data Types Detected:")
                print(f"      ID: {df['id'].dtype}")
                print(f"      Title: {df['title'].dtype}")
                print(f"      First 3 rows:\n{df.head(3)}")

            # FORCE INTEGER TYPE for ID (Crucial step!)
            df['id'] = df['id'].astype(int)

            # Convert to dict
            chunk_dict = dict(zip(df['id'], df['title']))
            full_dict.update(chunk_dict)

            # Verify one random entry from this batch
            random_key = random.choice(list(chunk_dict.keys()))
            print(f"   ✅ Added {len(chunk_dict)} entries. Sample: ID {random_key} -> '{chunk_dict[random_key]}'")

        except Exception as e:
            print(f"   ❌ Error reading {file_path}: {e}")

    # --- FINAL VERIFICATION ---
    print("\n" + "=" * 40)
    print(f"💾 Final Dictionary Size: {len(full_dict)}")

    print("🔍 Inspecting 5 Random Entries from Final Dict:")
    keys = list(full_dict.keys())
    for _ in range(5):
        k = random.choice(keys)
        print(f"   Key: {k} (Type: {type(k)}) --> Value: '{full_dict[k]}'")

    # Save to disk
    print(f"\n💾 Saving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'wb') as f:
        pickle.dump(full_dict, f)

    print("🎉 DONE! Now, you MUST upload this specific file to the bucket.")


if __name__ == "__main__":
    create_dict_debug()