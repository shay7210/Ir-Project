import requests
import json
import os
import time  # <--- Added for timing

# ==========================================
# 1. CONFIGURATION
# ==========================================
SEARCH_URL = "http://localhost:9000/search"
QUERIES_FILE = "queries_train.json"


def load_queries(file_path):
    print(f"📂 Loading queries from {file_path}...")
    if not os.path.exists(file_path):
        print(f"❌ Error: File '{file_path}' not found!")
        exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


# Load the data dynamically
TEST_DATA = load_queries(QUERIES_FILE)


def evaluate_engine():
    total_p5 = 0
    total_p10 = 0
    total_f1_30 = 0
    total_quality = 0
    total_time = 0  # <--- Accumulator for time
    queries_count = len(TEST_DATA)

    print(f"🚀 Starting Evaluation for {queries_count} queries...\n")
    # Updated Header to include Time
    print(f"{'QUERY':<30} | {'P@5':<8} | {'P@10':<8} | {'F1@30':<8} | {'TIME (ms)':<9} | {'SCORE'}")
    print("-" * 95)

    for query, expected_ids in TEST_DATA.items():
        try:
            # 1. Send Query & Measure Time
            start_time = time.time()  # <--- Start Timer
            response = requests.get(SEARCH_URL, params={'query': query})
            end_time = time.time()  # <--- Stop Timer

            duration_ms = (end_time - start_time) * 1000  # Convert to milliseconds
            total_time += duration_ms

            if response.status_code != 200:
                print(f"❌ Error: {query} returned status {response.status_code}")
                continue

            # 2. Extract IDs
            results_with_titles = response.json()
            if not results_with_titles:
                actual_ids = []
            else:
                actual_ids = [str(item[0]) for item in results_with_titles]

            expected_set = set(expected_ids)

            # ==========================
            # 3. METRIC: Precision@5
            # ==========================
            top_5 = actual_ids[:5]
            if len(top_5) > 0:
                rel_5 = len(set(top_5).intersection(expected_set))
                p_5 = rel_5 / 5.0
            else:
                p_5 = 0.0

            # ==========================
            # 4. METRIC: Precision@10
            # ==========================
            top_10 = actual_ids[:10]
            if len(top_10) > 0:
                rel_10 = len(set(top_10).intersection(expected_set))
                p_10 = rel_10 / 10.0
            else:
                p_10 = 0.0

            # ==========================
            # 5. METRIC: F1@30
            # ==========================
            top_30 = actual_ids[:30]
            if len(top_30) > 0:
                rel_30 = len(set(top_30).intersection(expected_set))

                # Precision @ 30
                p_30 = rel_30 / 30.0

                # Recall @ 30 (Based on Total Expected)
                total_expected = len(expected_set)
                r_30 = rel_30 / total_expected if total_expected > 0 else 0

                # F1 Score calculation
                if (p_30 + r_30) > 0:
                    f1_30 = (2 * p_30 * r_30) / (p_30 + r_30)
                else:
                    f1_30 = 0.0
            else:
                f1_30 = 0.0

            # ==========================
            # 6. ASSIGNMENT METRIC
            # ==========================
            if (p_5 + f1_30) > 0:
                score = (2 * p_5 * f1_30) / (p_5 + f1_30)
            else:
                score = 0.0

            # Print (Truncate query for display)
            display_query = (query[:27] + '..') if len(query) > 27 else query
            print(
                f"{display_query:<30} | {p_5:.2%}   | {p_10:.2%}   | {f1_30:.2%}   | {duration_ms:>6.0f} ms | {score:.4f}")

            total_p5 += p_5
            total_p10 += p_10
            total_f1_30 += f1_30
            total_quality += score

        except Exception as e:
            print(f"⚠️ Exception for '{query}': {e}")

    # ==========================================
    # 7. SUMMARY
    # ==========================================
    if queries_count > 0:
        avg_p5 = total_p5 / queries_count
        avg_p10 = total_p10 / queries_count
        avg_f1_30 = total_f1_30 / queries_count
        avg_score = total_quality / queries_count
        avg_time = total_time / queries_count

        print("-" * 95)
        print(f"📊 Average Precision@5:  {avg_p5:.2%}")
        print(f"📊 Average Precision@10: {avg_p10:.2%}")
        print(f"📊 Average F1@30:        {avg_f1_30:.2%}")
        print(f"⏱️  Average Query Time:   {avg_time:.2f} ms")
        print(f"🏆 Average Final Score:  {avg_score:.4f}")
    else:
        print("\n⚠️ No queries were processed.")


if __name__ == "__main__":
    evaluate_engine()