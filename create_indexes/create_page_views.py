import bz2
import pickle
import os
import sys
import time

# --- CONFIGURATION ---
# 1. Path to your ID -> Title map (Change this to your actual file)
map_path = "../inverted_indexes_pkls/id_to_title.pkl"

# 2. Path to the pageviews dump
pv_path = "../create_indexes/pageviews-202108-user.bz2"

# 3. Output path for the new PageView index
output_path = "../inverted_indexes_pkls/pageviews_index.pkl"

# --- MAIN LOGIC ---

# Step 1: Load the ID Map
print(f"Loading ID map from: {map_path}...")
try:
    with open(map_path, 'rb') as f:
        id_to_title = pickle.load(f)
    print(f"Map loaded. Total items: {len(id_to_title)}")
except FileNotFoundError:
    print(f"Error: Could not find map file at {map_path}")
    sys.exit(1)

# Step 2: Process the Pageviews Dump
wid2pv = {}
print(f"Processing pageviews from: {pv_path}...")
start_time = time.time()
count = 0

with bz2.open(pv_path, "rt", encoding="utf-8") as f:
    for line in f:
        parts = line.split()

        # Safety check for column count
        if len(parts) < 5:
            continue

        # FILTER 1: Only look at English Wikipedia
        # parts[0] is domain. We want 'en.wikipedia'
        if parts[0] != 'en.wikipedia':
            continue

        # FILTER 2: Skip null IDs
        # parts[2] is Page ID
        if parts[2] == 'null':
            continue

        try:
            page_id = int(parts[2])

            # CHECK: Is this ID in our map?
            if page_id in id_to_title:
                # parts[4] is the view count
                views = int(parts[4])
                wid2pv[page_id] = views

        except ValueError:
            continue

        # Progress Indicator (so you know it's not stuck)
        count += 1
        if count % 1_000_000 == 0:
            print(f"Processed {count} relevant lines... (Found {len(wid2pv)} matches)")

# Step 3: Save the Result
print(f"Finished. Total matching pages with views: {len(wid2pv)}")
print(f"Saving to {output_path}...")

with open(output_path, 'wb') as f:
    pickle.dump(wid2pv, f)

print(f"Done! Took {(time.time() - start_time) / 60:.2f} minutes.")