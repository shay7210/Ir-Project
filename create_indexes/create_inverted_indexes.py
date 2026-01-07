import sys
import os
import re
import pickle
import math
from collections import Counter, defaultdict
from pathlib import Path
from google.cloud import storage
import nltk
from nltk.corpus import stopwords
from inverted_index_gcp import InvertedIndex
from pyspark.sql import SparkSession

# ====================================================
# 1. CONFIGURATION & SETUP
# ====================================================
BUCKET_NAME = 'wikipidia_ir_project'
NUM_BUCKETS = 124

# Initialize Spark (if running as a standalone script)
spark = SparkSession.builder \
    .appName("IR_Index_Creation") \
    .master("local[*]") \
    .getOrCreate()

# Initialize GCS Client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# NLTK Setup
nltk.download('stopwords', quiet=True)
english_stopwords = frozenset(stopwords.words('english'))
corpus_stopwords = ["category", "references", "also", "external", "links",
                    "may", "first", "see", "history", "people", "one", "two",
                    "part", "thumb", "including", "second", "following",
                    "many", "however", "would", "became"]
all_stopwords = english_stopwords.union(corpus_stopwords)
RE_WORD = re.compile(r"""[\#\@\w](['\-]?\w){2,24}""", re.UNICODE)

# ====================================================
# 2. HELPER FUNCTIONS
# ====================================================

def _hash(s):
    import hashlib
    return hashlib.blake2b(bytes(s, encoding='utf8'), digest_size=5).hexdigest()

def token2bucket_id(token):
    return int(_hash(token), 16) % NUM_BUCKETS

def word_count(text, doc_id):
    tokens = [token.group() for token in RE_WORD.finditer(text.lower())]
    tokens = [token for token in tokens if token not in all_stopwords]
    dict_res = Counter(tokens)
    return [(token, (doc_id, tf)) for token, tf in dict_res.items()]

def reduce_word_counts(unsorted_pl):
    return sorted(unsorted_pl, key=lambda x: x[0])

def calculate_df(postings):
    return postings.map(lambda x: (x[0], len(x[1])))

def partition_postings_and_write(postings, folder_name):
    map_to_buckets = postings.map(lambda x: (token2bucket_id(x[0]), x))
    grouped = map_to_buckets.groupByKey()
    return grouped.map(lambda x: InvertedIndex.write_a_posting_list(x, BUCKET_NAME, folder_name))

def upload_file(local_path, remote_path):
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(local_path)
    print(f"   Uploaded {local_path} to gs://{BUCKET_NAME}/{remote_path}")

# ====================================================
# 3. DATA LOADING
# ====================================================
print("🔍 Scanning bucket for parquet files...")
all_blobs = list(client.list_blobs(BUCKET_NAME))
parquet_paths = [f"gs://{BUCKET_NAME}/{b.name}" for b in all_blobs if b.name.endswith('.parquet')]

if not parquet_paths:
    raise Exception(f"❌ No .parquet files found in bucket {BUCKET_NAME}")

print(f"✅ Found {len(parquet_paths)} parquet files. Loading...")
parquetFile = spark.read.parquet(*parquet_paths)

# ====================================================
# 4. CREATE ID-TO-TITLE DICTIONARY (What you missed)
# ====================================================
print("🚀 Creating ID-to-Title Dictionary...")

# Select only ID and Title columns
id_title_pairs = parquetFile.select("id", "title").rdd

# Collect into a local dictionary (Ensure Driver has enough RAM)
id_to_title_dict = id_title_pairs.collectAsMap()

# Save to Pickle
local_dict_file = "../id_to_title.pkl"
with open(local_dict_file, 'wb') as f:
    pickle.dump(id_to_title_dict, f)

# Upload to GCS
upload_file(local_dict_file, "postings_gcp/id_to_title/id_to_title.pkl")
print("✅ ID-to-Title Dictionary Done!")

# ====================================================
# 5. CREATE BODY INDEX
# ====================================================
print("🚀 Creating Body Index...")
doc_text_pairs = parquetFile.select("text", "id").rdd
word_counts_rdd = doc_text_pairs.flatMap(lambda x: word_count(x[0], x[1]))
postings = word_counts_rdd.groupByKey().mapValues(reduce_word_counts)

# Filter low frequency terms
postings_filtered = postings.filter(lambda x: len(x[1]) > 50)

# Calculate DF
w2df_body = calculate_df(postings_filtered)
w2df_dict_body = w2df_body.collectAsMap()

# Write Posting Lists
_ = partition_postings_and_write(postings_filtered, "postings_gcp/postings_body").collect()

# Save Global Index
super_posting_locs = defaultdict(list)
blobs = client.list_blobs(BUCKET_NAME, prefix='postings_gcp/postings_body')
for blob in blobs:
    if not blob.name.endswith("pickle"): continue
    with blob.open("rb") as f:
        posting_locs = pickle.load(f)
        for k, v in posting_locs.items():
            super_posting_locs[k].extend(v)

inverted = InvertedIndex()
inverted.posting_locs = super_posting_locs
inverted.df = w2df_dict_body
inverted.write_index('.', 'index_body')
upload_file('../index_body.pkl', 'postings_gcp/postings_body/index.pkl')
print("✅ Body Index Done!")

# ====================================================
# 6. CREATE TITLE INDEX
# ====================================================
print("🚀 Creating Title Index...")
doc_title_pairs = parquetFile.select("title", "id").rdd
word_counts_title = doc_title_pairs.flatMap(lambda x: word_count(x[0], x[1]))
postings_title = word_counts_title.groupByKey().mapValues(reduce_word_counts)

w2df_title = calculate_df(postings_title)
w2df_dict_title = w2df_title.collectAsMap()

_ = partition_postings_and_write(postings_title, "postings_gcp/postings_title").collect()

super_posting_locs_title = defaultdict(list)
blobs = client.list_blobs(BUCKET_NAME, prefix='postings_gcp/postings_title')
for blob in blobs:
    if not blob.name.endswith("pickle"): continue
    with blob.open("rb") as f:
        posting_locs = pickle.load(f)
        for k, v in posting_locs.items():
            super_posting_locs_title[k].extend(v)

inverted_title = InvertedIndex()
inverted_title.posting_locs = super_posting_locs_title
inverted_title.df = w2df_dict_title
inverted_title.write_index('.', 'index_title')
upload_file('../index_title.pkl', 'postings_gcp/postings_title/index.pkl')
print("✅ Title Index Done!")

# ====================================================
# 7. CREATE ANCHOR INDEX
# ====================================================
print("🚀 Creating Anchor Index...")
pages_links = parquetFile.select("id", "anchor_text").rdd
anchor_pairs = pages_links.flatMap(lambda x: [(row.id, row.text) for row in x.anchor_text])

def anchor_to_tokens(pid, text):
    tokens = [token.group() for token in RE_WORD.finditer(text.lower()) if token.group() not in all_stopwords]
    return [(t, pid) for t in tokens]

word_counts_anchor = anchor_pairs.flatMap(lambda x: anchor_to_tokens(x[0], x[1]))
postings_anchor = word_counts_anchor.map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .groupByKey()

w2df_anchor = calculate_df(postings_anchor)
w2df_dict_anchor = w2df_anchor.collectAsMap()

_ = partition_postings_and_write(postings_anchor, "postings_gcp/postings_anchor").collect()

super_posting_locs_anchor = defaultdict(list)
blobs = client.list_blobs(BUCKET_NAME, prefix='postings_gcp/postings_anchor')
for blob in blobs:
    if not blob.name.endswith("pickle"): continue
    with blob.open("rb") as f:
        posting_locs = pickle.load(f)
        for k, v in posting_locs.items():
            super_posting_locs_anchor[k].extend(v)

inverted_anchor = InvertedIndex()
inverted_anchor.posting_locs = super_posting_locs_anchor
inverted_anchor.df = w2df_dict_anchor
inverted_anchor.write_index('.', 'index_anchor')
upload_file('../index_anchor.pkl', 'postings_gcp/postings_anchor/index.pkl')
print("✅ Anchor Index Done!")

print("\n🎉 ALL TASKS COMPLETE.")