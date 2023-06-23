# Load the alphanumeric data
# Create Training data
# Generate Rules

# Test the model

import requests
import json
from spacy.lang.en import English
from spacy.pipeline import EntityRuler


query1 = {
    "aggs": {
        "regex_count": {
            "terms": {
                "field": "REPLACE_THIS.keyword",
                "size": 20
            }
        }
    },
    "size": 0
}

query2 = {
    "aggs": {
        "regex_patterns" : {
            "terms": {
                "field": "regex_patterns.keyword",
                "size": 500
            }
        }
    },
    "query" : {
        "term": {
            "REPLACE_THIS.keyword": {
                "value": "THIS_AS_WELL"
            }
        }
    },
    "size" : 0
}

es_url = "http://localhost:9200/"

i14y_list = []

def query_elasticsearch(search_endpoint, body = ""):
    r = requests.get(
        es_url + search_endpoint, 
        headers = {"Content-Type": "application/json"},
        data=body
        )
    return r

def generate_list_from_i14y():
    search_endpoint = "production-i14y-documents-searchgov-v6-reindex_keyword_regex/_search"
    r = query_elasticsearch(search_endpoint, json.dumps(query1).replace("REPLACE_THIS", "domain_name"))
    response = json.loads(r.text)
    print(response['aggregations']['regex_count']['buckets'])
    # Iterate over keys:
    for domain in response['aggregations']['regex_count']['buckets']:
        print( domain['key'])
        payload = json.dumps(query2).replace("THIS_AS_WELL", domain['key']).replace("REPLACE_THIS", "domain_name")
        print(payload)
        es_query_val = json.loads(query_elasticsearch(search_endpoint, payload).text)
        for regex_pattern in es_query_val["aggregations"]["regex_patterns"]["buckets"]:
            # print(regex_pattern["key"])
            i14y_list.append(regex_pattern["key"])

def generate_rules(patterns):
    nlp = English()
    # ruler = EntityRuler(nlp)
    ruler = nlp.add_pipe("entity_ruler")
    ruler.add_patterns(patterns)
    # nlp.add_pipe(ruler)
    nlp.to_disk("alpha_numeric")

def create_training_data(data, data_type):
    patterns = []
    for item in data:
        patterns.append(
            { 
                "label": data_type,
                "pattern": item
            }
        )
    return (patterns)

generate_list_from_i14y()
# print(i14y_list)
generate_rules(create_training_data(i14y_list, "ALPHANUMERIC"))