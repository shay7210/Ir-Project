#!/bin/bash
set -e

APP_USER="shay7210"
APP_HOME="/home/${APP_USER}"
VENV_DIR="${APP_HOME}/venv"

apt-get update
apt-get install -y python3-venv python3-pip

# Create venv as the normal user (not root)
sudo -u "${APP_USER}" bash -lc "
python3 -m venv '${VENV_DIR}'
source '${VENV_DIR}/bin/activate'
pip install --upgrade pip
pip install \
  'Flask==2.0.2' \
  'Werkzeug==2.3.8' \
  'flask-restful==0.3.9' \
  'nltk==3.6.3' \
  'pandas' \
  'google-cloud-storage' \
  'numpy>=1.23.2,<3'
"
# 1. Create a directory for your data
sudo -u "${APP_USER}" mkdir -p "${APP_HOME}/data"

# 2. Download data from your bucket (REPLACE WITH YOUR BUCKET NAME)
# NOTE: The -r flag is for directories (like your postings folder)
sudo -u "${APP_USER}" gsutil -m cp -r gs://YOUR_BUCKET_NAME_HERE/* "${APP_HOME}/data/"

echo "Startup script finished successfully!"