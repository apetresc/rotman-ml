#!/bin/bash -ex

ENDPOINT_ID="1584435838043815936"
PROJECT_ID="anthos-poc-315613"
INPUT_DATA_FILE="./predict_banking_request.json"


curl \
  -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  https://us-central1-aiplatform.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/endpoints/${ENDPOINT_ID}:predict \
  -d "@${INPUT_DATA_FILE}"
