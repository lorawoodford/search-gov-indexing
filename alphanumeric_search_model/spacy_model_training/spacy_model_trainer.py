# Load the alphanumeric data
# Create Training data
# Generate Rules

# Test the model

import requests
import json
import spacy
from spacy.lang.en import English
from spacy.pipeline import EntityRuler


query1 = {
    "aggs": {
        "regex_count": {
            "terms": {
                "field": "REPLACE_THIS.keyword",
                "size": 5
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
                "size": 5
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

query3 = {
    "query": {
        "query_string": {
            "query": "REPLACE_THIS",
            "default_field": "content_en"
        }
    },
    "_source": "content_en",
    "size": 1
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

def get_test_document_from_elasticsearch(query_string):
    search_endpoint = "production-i14y-documents-searchgov-v6/_search"
    r = query_elasticsearch(search_endpoint, json.dumps(query3).replace("REPLACE_THIS", query_string))
    response = json.loads(r.text)
    # print(response)
    if r.status_code == 200:
        return(response["hits"]["hits"][0]["_source"]["content_en"])
    return None

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

def test_model(model, text):
    doc = nlp(text)
    results = []
    entities = []
    print(doc.ents)
    for ent in doc.ents:
        entities.append((ent.start_char, ent.end_char, ent.label_))
    print (entities)
    if len(entities) > 0:
        results = [text, {"entities": entities}]
    return results

generate_list_from_i14y()
print(i14y_list)
print(len(i14y_list))
i14y_list = list(set(i14y_list))
test_docs = []
for item in i14y_list[0]:
    test_docs.append(get_test_document_from_elasticsearch(item))
# print(get_test_document_from_elasticsearch(i14y_list[0]))
# print(test_docs)
generate_rules(create_training_data(i14y_list, "ALPHANUMERIC"))

nlp = spacy.load("alpha_numeric")
TRAINING_DATA = []

for doc in test_docs:
    if doc != None:
        doc = doc.strip()
        doc = doc.replace("\n", " ")
    results = test_model(nlp, doc)
    print(results)
    if results != None:
        TRAINING_DATA.append(results)

print(TRAINING_DATA)