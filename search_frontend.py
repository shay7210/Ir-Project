from flask import Flask, request, jsonify, render_template
from google.cloud import storage
import pickle
import os
import re
import collections
import math
import struct
import gzip
import csv
import nltk
from nltk.corpus import stopwords

# ==============================================================================
# 1. SETUP & CONFIGURATION
# ==============================================================================

# GLOBAL DATA HOLDERS
index_body = None
index_title = None
index_anchor = None
page_rank = {}
id_to_title = {}
page_views = {}
# CONFIGURATION
BUCKET_NAME = 'wikipidia_ir_project'
KEY_FILE_PATH = 'my_gcp_key.json'

# GCS CLIENT (Global)
storage_client = None
bucket = None

# ==============================================================================
# 2. TOKENIZER
# ==============================================================================

RE_WORD = re.compile(r"""[\#\@\w](['\-]?\w){2,24}""", re.UNICODE)

nltk.download('stopwords', quiet=True)
english_stopwords = frozenset(stopwords.words('english'))
corpus_stopwords = ["category", "references", "also", "external", "links",
                    "may", "first", "see", "history", "people", "one", "two",
                    "part", "thumb", "including", "second", "following",
                    "many", "however", "would", "became"]
all_stopwords = english_stopwords.union(corpus_stopwords)


def tokenize(text):
    tokens = [token.group() for token in RE_WORD.finditer(text.lower())]
    return [token for token in tokens if token not in all_stopwords]


# ==============================================================================
# 3. GCS & DATA LOADING LOGIC
# ==============================================================================

def init_gcp():
    global storage_client, bucket
    if not os.path.exists(KEY_FILE_PATH):
        raise FileNotFoundError(f"Key file '{KEY_FILE_PATH}' missing! Please move it to this folder.")

    storage_client = storage.Client.from_service_account_json(KEY_FILE_PATH)
    bucket = storage_client.bucket(BUCKET_NAME)
    print(f"✅ Authenticated with GCS bucket: {BUCKET_NAME}")


def download_all_bin_files(local_dir="postings_gcp"):
    """Downloads all .bin files from the bucket to local disk."""
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # List all blobs in the bucket that end with .bin
    blobs = bucket.list_blobs(prefix="postings_gcp/")

    for blob in blobs:
        if blob.name.endswith(".bin"):
            local_path = blob.name  # e.g., postings_gcp/postings_body/0_0.bin

            # Create subdirectories if needed
            local_folder = os.path.dirname(local_path)
            if not os.path.exists(local_folder):
                os.makedirs(local_folder)

            # Download if not exists
            if not os.path.exists(local_path):
                print(f"⬇️ Downloading {blob.name}...")
                blob.download_to_filename(local_path)
            else:
                print(f"✅ Found {blob.name}, skipping.")

def download_blob(remote_path, local_filename):
    if os.path.exists(local_filename):
        print(f"   -> Found local {local_filename}, skipping download.")
        return
    print(f"   -> Downloading {remote_path} to {local_filename}...")
    try:
        blob = bucket.blob(remote_path)
        blob.download_to_filename(local_filename)
    except Exception as e:
        print(f"   ❌ Failed to download {remote_path}: {e}")


def load_index(index_name, remote_folder):
    local_name = f"inverted_indexes_pkls/{index_name}.pkl"
    remote_path = f"{remote_folder}/index.pkl"
    download_blob(remote_path, local_name)
    if not os.path.exists(local_name): return None
    print(f"   -> Loading {local_name}...")
    with open(local_name, 'rb') as f:
        return pickle.load(f)


def load_pagerank():
    """Loads PageRank from the CSV.GZ file."""
    local_name = "inverted_indexes_pkls/pagerank.csv.gz"
    # Update this path if it changes in your bucket
    remote_path = "pr/part-00000-c5e092f9-9241-410d-955d-ec78de539def-c000.csv.gz"

    download_blob(remote_path, local_name)

    pr_dict = {}
    if os.path.exists(local_name):
        print("   -> Processing PageRank CSV...")
        try:
            # gzip.open automatically handles the compression!
            with gzip.open(local_name, 'rt') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        pr_dict[int(row[0])] = float(row[1])
        except Exception as e:
            print(f"   ❌ Error reading PageRank: {e}")
    return pr_dict

def load_id_map():
    local_name = "inverted_indexes_pkls/id_to_title.pkl"
    remote_path = "postings_gcp/id_to_title/id_to_title.pkl"
    download_blob(remote_path, local_name)
    if os.path.exists(local_name):
        print(f"   -> Loading {local_name}...")
        with open(local_name, 'rb') as f:
            return pickle.load(f)
    return {}

def load_pageviews():
    """Loads PageViews from a Pickle file."""
    local_name = "inverted_indexes_pkls/pageviews_index.pkl"
    # Adjust this remote path to match where you eventually put the file in your bucket
    remote_path = "postings_gcp/pageviews/pageviews_index.pkl"

    download_blob(remote_path, local_name)

    if os.path.exists(local_name):
        print(f"   -> Loading {local_name}...")
        try:
            with open(local_name, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"   ❌ Error reading PageViews pkl: {e}")
            return {}
    else:
        print("   ⚠️ PageViews file not found. Returning empty dictionary.")
        return {}
# ==============================================================================
# 4. REMOTE POSTING LIST READER (FIXED)
# ==============================================================================

class MultiFileReader:
    def __init__(self, base_dir):
        self.base_dir = base_dir  # e.g., "postings_gcp"

    def read(self, posting_locs, df):
        if not posting_locs: return []
        b = []

        # OPEN THE FILE LOCALLY
        for filename, offset in posting_locs:
            # Construct local path, e.g., "postings_gcp/postings_body/0_0.bin"
            file_path = os.path.join(self.base_dir, filename)

            try:
                with open(file_path, 'rb') as f:
                    f.seek(offset)
                    # 6 bytes per posting (4 for doc_id, 2 for tf)
                    n_bytes = df * 6
                    data_bytes = f.read(n_bytes)

                    # Process bytes
                    for i in range(0, len(data_bytes), 6):
                        doc_id = struct.unpack("!I", data_bytes[i:i + 4])[0]
                        tf = struct.unpack("!H", data_bytes[i + 4:i + 6])[0]
                        b.append((doc_id, tf))
            except FileNotFoundError:
                print(f"❌ File not found: {file_path}")
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")

        return b

# Updated get_posting_list
def get_posting_list(inverted_index, token, remote_folder):
    if not inverted_index: return []
    posting_locs = inverted_index.posting_locs.get(token, [])
    if not posting_locs: return []

    df = inverted_index.df.get(token, 0)

    # --- PRUNING ---
    MAX_DOCS_TO_READ = 15000
    if df > MAX_DOCS_TO_READ:
        df = MAX_DOCS_TO_READ

    # --- FIX 2: USE remote_folder AS THE BASE DIR ---
    # The files are downloaded to paths like 'postings_gcp/postings_body'
    reader = MultiFileReader(base_dir=remote_folder)
    return reader.read(posting_locs, df)

# ==============================================================================
# 5. FLASK APP
# ==============================================================================

class MyFlaskApp(Flask):
    # Inside class MyFlaskApp(Flask):

    def run(self, host=None, port=None, debug=None, **options):
        print("🚀 Initializing Server...")
        init_gcp()

        global index_body, index_title, index_anchor, page_rank, id_to_title, page_views

        # --- FIX 1: ADD THIS LINE ---
        print("⬇️ Downloading Postings to Local Disk...")
     #   download_all_bin_files()
        # ----------------------------

        print("LOADING DATA...")
        index_body = load_index("index_body", "postings_gcp/postings_body")
        index_title = load_index("index_title", "postings_gcp/postings_title")
        index_anchor = load_index("index_anchor", "postings_gcp/postings_anchor")
        page_rank = load_pagerank()
        page_views = load_pageviews()
        id_to_title = load_id_map()
        print("✅ Data Loaded. Server Ready!")
        super(MyFlaskApp, self).run(host=host, port=port, debug=debug, **options)

app = MyFlaskApp(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False



@app.route("/search")
def search():
    ''' Returns list of Wiki IDs (Strings) using BM25 for Body, and simple weights for Title/Anchor '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0: return jsonify(res)

    print(f"\n--- SEARCHING: '{query}' ---")
    query_tokens = tokenize(query)
    scores = collections.Counter()

    # --- CONFIGURATION ---
    # N: Total number of documents in corpus (approximate from PageRank)
    N = len(page_rank) if page_rank else 6348910

    # Weights (Adjusted W_BODY up because BM25 scores are smaller than raw TF)
    W_TITLE = 0.1

    W_ANCHOR = 0.1

    W_BODY = 25.0

    W_PR = 0.01

    # BM25 Constants
    k1 = 1.2
    b = 0  # No length normalization (we don't have doc lengths loaded)

    def calc_idf(doc_freq, total_docs):
        return math.log(1 + (total_docs - doc_freq + 0.5) / (doc_freq + 0.5))

    def bm25_saturation(tf):
        # Simplified BM25 with b=0: (TF * (k1 + 1)) / (TF + k1)
        return (tf * (k1 + 1)) / (tf + k1)

    # 1. Title (Simple Weight - As requested)
    for token in query_tokens:
        for doc_id, tf in get_posting_list(index_title, token, "postings_gcp/postings_title"):
            scores[doc_id] += (1 * W_TITLE)

    # 2. Anchor (Simple Weight - As requested)
    for token in query_tokens:
        for doc_id, tf in get_posting_list(index_anchor, token, "postings_gcp/postings_anchor"):
            scores[doc_id] += (tf * W_ANCHOR)

    # 3. Body (BM25)
    for token in query_tokens:
        # Get Document Frequency (DF) for IDF calculation
        df = index_body.df.get(token, 0)
        if df == 0: continue

        idf = calc_idf(df, N)

        for doc_id, tf in get_posting_list(index_body, token, "postings_gcp/postings_body"):
            # BM25 Score = IDF * (TF saturation)
            bm25_score = idf * bm25_saturation(tf)
            scores[doc_id] += (bm25_score * W_BODY)

    # 4. PageRank Boost
    for doc_id in scores:
        raw_pr = page_rank.get(doc_id, 0)
        pr_boost = math.log10(raw_pr + 1) if raw_pr > 0 else 0
        scores[doc_id] += (pr_boost * W_PR)

    # Final Result
    top_docs = scores.most_common(100)
    res = [(str(doc_id), id_to_title.get(doc_id, "N/A")) for doc_id, score in top_docs]
    print(res[0])
    print(f"   ➡️ Returning {len(res)} results.")
    return jsonify(res)


import math  # Make sure to import math


@app.route("/search_body")
def search_body():
    ''' Returns list of Wiki IDs (Strings) ordered by TF-IDF '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0: return jsonify(res)

    query_tokens = tokenize(query)
    scores = collections.Counter()

    # 1. Get total number of documents (N)
    # If index_body doesn't have a total_docs attribute, hardcode the corpus size (e.g., ~6.3M for English Wiki)
    # or use len(index_body.df) as a proxy if it covers all docs.
    N = len(index_body.ds) if hasattr(index_body, 'ds') else 6348910

    for token in query_tokens:
        # Skip tokens that don't exist in the index to avoid errors
        if token not in index_body.df:
            continue

        # 2. Calculate IDF for the term
        df = index_body.df[token]
        idf = math.log(N / df, 10)  # Log base 10 is standard

        postings = get_posting_list(index_body, token, "postings_gcp/postings_body")
        for doc_id, tf in postings:
            # 3. Accumulate score: TF * IDF
            scores[doc_id] += (tf * idf)

    top_docs = scores.most_common(100)
    res = [(str(doc_id), id_to_title.get(doc_id, "N/A")) for doc_id, score in top_docs]
    return jsonify(res)

@app.route("/search_title")
def search_title():
    ''' Returns list of Wiki IDs (Strings) '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0: return jsonify(res)

    query_tokens = tokenize(query)
    scores = collections.Counter()

    for token in query_tokens:
        postings = get_posting_list(index_title, token, "postings_gcp/postings_title")
        for doc_id, tf in postings:
            scores[doc_id] += 1

    top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    res = [(str(doc_id), id_to_title.get(int(doc_id), "N/A")) for doc_id, score in top_docs]
    return jsonify(res)


@app.route("/search_anchor")
def search_anchor():
    ''' Returns list of Wiki IDs (Strings) '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0: return jsonify(res)

    query_tokens = tokenize(query)
    scores = collections.Counter()

    for token in query_tokens:
        postings = get_posting_list(index_anchor, token, "postings_gcp/postings_anchor")
        for doc_id, tf in postings:
            scores[doc_id] += 1

    top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    res = [(str(doc_id), id_to_title.get(doc_id, "N/A")) for doc_id, score in top_docs]
    return jsonify(res)


@app.route("/get_pagerank", methods=['POST'])
def get_pagerank():
    wiki_ids = request.get_json() or []
    res = [page_rank.get(doc_id, 0.0) for doc_id in wiki_ids]
    return jsonify(res)


@app.route("/get_pageview", methods=['POST'])
def get_pageview():
    ''' Returns the number of page views that each of the provide wiki articles
        had in August 2021.
    '''
    # 1. Parse JSON input (expecting a list of IDs)
    wiki_ids = request.get_json() or []

    # 2. Retrieve counts
    res = []
    for doc_id in wiki_ids:
        try:
            # The dictionary keys are likely integers.
            # We try to convert the input ID to int just in case.
            count = page_views.get(int(doc_id), 0)
        except (ValueError, TypeError):
            # If conversion fails, try using the key as is (e.g. string)
            count = page_views.get(doc_id, 0)

        res.append(count)

    return jsonify(res)

@app.route("/")
def home():
    return render_template("index.html")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000, debug=True)