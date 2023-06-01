import os
import json
import requests
from requests.auth import HTTPBasicAuth

es_url = os.environ['ES_URL']
es_username = os.environ['ES_USERNAME']
es_password = os.environ['ES_PASSWORD']

# The document id you want to update
doc_id = 'engineno9_7276230938'

# The new google_image_links
new_image_link = 'https://lh5.googleusercontent.com/p/AF1QipMVwsktY5mYiv4FKyqj4iNjD6Ug-tMzUZbNxu06'

# The name of the index
index_name = 'results_st_pete'

# The update command
update_command = {
    "doc": {
        "google_image_links": new_image_link
    }
}

# Send the update request
response = requests.post(
    f"{es_url}{index_name}/_update/{doc_id}",
    auth=HTTPBasicAuth(es_username, es_password),
    headers={"Content-Type": "application/json"},
    data=json.dumps(update_command)
)

# Check if the update operation was successful
if response.status_code == 200:
    print(f"Updated document with ID: {doc_id}")
else:
    print(f"Failed to update document. Response: {response.text}")
