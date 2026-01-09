# Information Retrieval Project (Search Engine)

A Python-based search engine built as part of an Information Retrieval academic course. This system implements an Inverted Index and uses ranking algorithms (BM25) to retrieve and rank relevant documents based on user queries. The engine is exposed via a RESTful API using Flask and is designed to be deployed on Google Cloud Platform (GCP).

## 🚀 Features

* **Inverted Index:** Efficient indexing of document corpus.
* **Ranking Algorithm:** Implementation of BM25 for relevance scoring.
* **REST API:** Flask-based backend serving search results in JSON format.
* **Cloud Ready:** Configured for deployment on GCP Compute Engine.
* **Query Processing:** Text preprocessing, tokenization, and stop-word removal.

## 🛠️ Tech Stack

* **Language:** Python 3.8+
* **Web Framework:** Flask
* **Libraries:** NumPy, Pandas, Scikit-learn, NLTK (for NLP tasks)
* **Deployment:** Google Cloud Platform (Compute Engine / VM)

## 📂 Project Structure

```bash
ir_proj_20251213/
│
├── create_indexes/            # Scripts to generate indices
│   ├── create_id_to_dict_pkl.py
│   ├── create_inverted_indexes.py
│   ├── create_page_views.py
│   └── create_pagerank.py
│
├── deploy_scripts/            # Cloud deployment helpers
│   ├── run_frontend_in_colab.ipynb
│   ├── run_frontend_in_gcp.sh
│   └── startup_script_gcp.sh
│
├── inverted_indexes_pkls/     # Serialized index data & PageRank
│   ├── id_to_title.pkl
│   ├── index_anchor.pkl
│   ├── index_body.pkl
│   ├── index_title.pkl
│   ├── pagerank.csv.gz
│   ├── pageviews.pkl
│   └── pageviews_index.pkl
│
├── plots/                     # Evaluation plots and graphs
│
├── postings_gcp/              # Binary posting files
│   ├── postings_anchor/
│   ├── postings_body/
│   └── postings_title/
│
├── templates/                 # Flask HTML templates
│   └── index.html
│
├── tests/                     # Unit tests
│   ├── test_engine.py
│   └── test_pagerank_pageViews.py
│
├── .gitignore
├── inverted_index_gcp.py      # Main Inverted Index class and logic
├── queries_train.json         # Training queries for evaluation
├── README.md                  # Project documentation
└── search_frontend.py         # Main Flask application entry point