# Load Spacy Model
# Load Levenshtein Dictionary
# Query ElasticSearch for Documents
# Consider partial document updates...

import argparse


query = {
    "query" : {
        "range" : {
            "updated_at": {
                "gte": "SOME_VALUE"
            }
        },
        "exists": {
            "field": "SOME_FIELD"
        }
    },
    "size" : 5000
}

parser = argparse.ArgumentParser(
    prog = "Some Insightful Name",
    description = "What this script does"
)

parser.add_argument("-s", "--start_date", required=True, help="Date and time to from which to start processing documents MM-DD-YYYY HH:mm")
# parser.add_argument("-e", "--es_url", required=True, help="The URL of ElasticSearch, should also contain the port")
# parser.add_argument("-i", "--index", required=True, help="The Index to Crawl inside of ElasticSearch")

es_url = "http://localhost:9200/"

alphanumeric_spacy = spacy.load("Filename")
levenshtein_dictionary = [] # File.load("Something")


def query_elasticsearch(url, search_endpoint, request = ""):
    # Make Request to ElasticSearch
    r = requests.get(
        url + search_endpoint,
        headers = {"Content-Type": "application/json"},
        data=request
    )
    return r

def create_scroll_elasticsearch(url, index, request, scroll_duration = 10):
    # Make a Scrolling request to ElasticSearch
    return query_elasticsearch(
        url,
        index + "/search?scroll=" + str(scroll_duration) + "m",
        request
    )

def push_to_elasticsearch(url, index, documents):
    # Use Partial document update to push to ElasticSearch
    es_payload = []
    for document in documents:
        es_payload.append(json.dumps{"index" : {"_index": index, "_id": document["id"]}})
        es_payload.append(json.dumps{"doc": {"alphanumeric_values": document["alphanumeric_vals"]}})
    requests.post(
        url + index + "/_bulk",
        headers = {"Content-Type": "application/json"}
        data="\n".join(es_payload)
    )

def crawl_es_index(es_url, index):
    # Do the actual crawling
    # Call scroll
    modified_query = json.dumps(query).replace("SOME_VALUE", "01-01-2023").replace("SOME_FIELD", "updated_at")
    results = create_scroll_elasticsearch(es_url, index, modified_query)
    scroll_id = results["_scroll_id"]
    while True:
        # Check that there are documents to process
        if len(results["hits"]["hits"]) == 0:
            break
        
        # Process documents
        modified_documents = []
        for document in results["hits"]["hits"]:
            modified_documents.append(
                {
                    "id": document["_id"],
                    "alphanumeric_vals": alphanumeric_spacy(document["content_en"])
                }
            )
        
        # Write Documents to ES
        push_to_elasticsearch(es_url, index, modified_documents)
        # Query ElasticSearch
        results = query_elasticsearch(es_url, "_search/scroll"
            json.dumps({
                "scroll": "10m",
                "scroll_id": scroll_id
            })
        )

# Setup Process