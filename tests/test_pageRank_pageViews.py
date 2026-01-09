import requests
import json

# ==========================================
# CONFIGURATION
# ==========================================
BASE_URL = "http://localhost:9000"
TEST_IDS = [12, 25, 1000, 999999999]  # Includes valid IDs and a  non-existent one


def test_pagerank():
    url = f"{BASE_URL}/get_pagerank"
    print(f"\n🧪 Testing PageRank Endpoint: {url}")

    try:
        # Send POST request with JSON list of IDs
        response = requests.post(url, json=TEST_IDS)

        if response.status_code == 200:
            results = response.json()
            print("   ✅ Status 200 OK")
            print(f"   📤 Input IDs: {TEST_IDS}")
            print(f"   📥 Received:  {results}")

            # Simple validation
            if len(results) == len(TEST_IDS):
                print("   ✅ Count matches input length.")
            else:
                print("   ❌ Count mismatch!")
        else:
            print(f"   ❌ Error: Status {response.status_code}")
            print(f"   Response: {response.text}")

    except Exception as e:
        print(f"   ❌ Exception: {e}")


def test_pageviews():
    url = f"{BASE_URL}/get_pageview"
    print(f"\n🧪 Testing PageViews Endpoint: {url}")

    try:
        # Send POST request with JSON list of IDs
        response = requests.post(url, json=TEST_IDS)

        if response.status_code == 200:
            results = response.json()
            print("   ✅ Status 200 OK")
            print(f"   📤 Input IDs: {TEST_IDS}")
            print(f"   📥 Received:  {results}")

            # Simple validation
            if len(results) == len(TEST_IDS):
                print("   ✅ Count matches input length.")
            else:
                print("   ❌ Count mismatch!")
        else:
            print(f"   ❌ Error: Status {response.status_code}")
            print(f"   Response: {response.text}")

    except Exception as e:
        print(f"   ❌ Exception: {e}")


if __name__ == "__main__":
    print(f"🚀 Starting Auxiliary API Tests on {BASE_URL}...")
    test_pagerank()
    test_pageviews()