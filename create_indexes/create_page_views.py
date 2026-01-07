import pickle
import os
from google.cloud import storage

# ==========================================
# CONFIGURATION
# ==========================================
BUCKET_NAME = 'wikipidia_ir_project'
KEY_FILE_PATH = '../my_gcp_key.json'

# The path where your server expects the file
DESTINATION_BLOB_NAME = 'postings_gcp/pageviews/pageviews.pkl'

# Path to your raw text file containing "doc_id count"
# If you downloaded the course file, it might be named 'pageviews-202108-user.txt'
RAW_DATA_PATH = 'pageviews.txt'


def get_bucket():
    if not os.path.exists(KEY_FILE_PATH):
        raise FileNotFoundError(f"Key file '{KEY_FILE_PATH}' not found!")
    storage_client = storage.Client.from_service_account_json(KEY_FILE_PATH)
    return storage_client.bucket(BUCKET_NAME)


def create_and_upload_pageviews():
    pageviews_dict = {}

    # 1. READ RAW DATA
    if os.path.exists(RAW_DATA_PATH):
        print(f"📖 Reading raw data from {RAW_DATA_PATH}...")
        with open(RAW_DATA_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        # Assumption: format is "doc_id count"
                        doc_id = int(parts[0])
                        count = int(parts[1])
                        pageviews_dict[doc_id] = count
                    except ValueError:
                        continue
        print(f"✅ Loaded {len(pageviews_dict)} documents into dictionary.")
    else:
        print(f"⚠️ Raw file '{RAW_DATA_PATH}' not found.")
        print("   Creating an EMPTY dictionary so the server can start.")
        # If you have specific IDs you want to test, add them manually here:
        # pageviews_dict = {123: 1000, 456: 500}

    # 2. PICKLE THE DICTIONARY
    temp_filename = 'pageviews.pkl'
    print(f"🥒 Pickling data to {temp_filename}...")
    with open(temp_filename, 'wb') as f:
        pickle.dump(pageviews_dict, f)

    # 3. UPLOAD TO GCP
    print(f"☁️ Uploading to gs://{BUCKET_NAME}/{DESTINATION_BLOB_NAME}...")
    bucket = get_bucket()
    blob = bucket.blob(DESTINATION_BLOB_NAME)
    blob.upload_from_filename(temp_filename)

    print("🎉 Success! The file is in the bucket.")

    # Cleanup local file
    if os.path.exists(temp_filename):
        os.remove(temp_filename)


if __name__ == "__main__":
    create_and_upload_pageviews()