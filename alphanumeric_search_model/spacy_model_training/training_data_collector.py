import requests
import json
import spacy
import sys
import signal
import datetime
import time
import random
import os
from spacy.lang.en import English
from spacy.pipeline import EntityRuler
from spacy.training.example import Example

from threading import Thread
import queue

# import ray

query1 = {
    "aggs": {
        "regex_count": {
            "terms": {
                "field": "doc.REPLACE_THIS",
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
                "field": "doc.regex_patterns",
                "size": 500
            }
        }
    },
    "query" : {
        "bool": {
            "filter": [
                {
                    "term": {
                        "doc.REPLACE_THIS": {
                            "value": "THIS_AS_WELL"
                        }
                    }
                }
            ]
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

es_url = "http://es717x3:9200/"

i14y_list = []

def query_elasticsearch(search_endpoint, body = ""):
    r = requests.get(
        es_url + search_endpoint, 
        headers = {"Content-Type": "application/json"},
        data=body
        )
    return r

def generate_list_from_i14y():
    print(str(datetime.datetime.now()) + " Collecting list of Search Gov Alpha Numeric Strings")    
    search_endpoint = "test_index/_search"
    r = query_elasticsearch(search_endpoint, json.dumps(query1).replace("REPLACE_THIS", "domain_name"))
    response = json.loads(r.text)
    # print(response)
    # print(response['aggregations']['regex_count']['buckets'])
    # for domain in response['aggregations']['regex_count']['buckets']:
    #     print(domain["key"] + " " + str(domain["doc_count"]))
    # Iterate over keys:
    domains = ["www.gsa.gov", "www.goarmy.com", "static.e-publishing.af.mil"]
    for domain in response['aggregations']['regex_count']['buckets']:
        domains.append(domain['key'])
    print(str(datetime.datetime.now()) + " Iterating over " + str(len(domains)) + " domains")
    for domain in domains: #response['aggregations']['regex_count']['buckets']:
        print(domain)
        payload = json.dumps(query2).replace("THIS_AS_WELL", domain).replace("REPLACE_THIS", "domain_name")
        # print(payload)
        es_query_val = json.loads(query_elasticsearch(search_endpoint, payload).text)
        for regex_pattern in es_query_val["aggregations"]["regex_patterns"]["buckets"]:
            # print(str(regex_pattern["value"]) + "\t\t" + regex_pattern["key"])
            i14y_list.append(regex_pattern["key"])

def get_test_document_from_elasticsearch(query_string):
    search_endpoint = "production-i14y-documents-searchgov-v6/_search"
    r = query_elasticsearch(search_endpoint, json.dumps(query3).replace("REPLACE_THIS", query_string))
    response = json.loads(r.text)
    # print(r.status_code)
    # print(response)
    if r.status_code == 200 and len(response["hits"]["hits"]) > 0:
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
    # print(doc.ents)
    for ent in doc.ents:
        entities.append((ent.start_char, ent.end_char, ent.label_))
    # print (entities)
    if len(entities) > 0:
        results = [text, {"entities": entities}]
    return results

def remove_english_words_from_list(list_of_words, english_word_path = "data/english_words.txt"):
    print(str(datetime.datetime.now()) + " Starting English Word Removal")    
    f = open(english_word_path, "r")
    english_words = f.read().lower().split("\n")
    for word in list_of_words:
        if word in english_words:
            list_of_words.remove(word)
    return list_of_words

def save_nlp(filename, nlp):
    print(str(datetime.datetime.now()) + " Saving " + filename + " to disk")
    nlp.to_disk(filename)

def load_nlp(filename):
    print(str(datetime.datetime.now()) + " Loading " + filename + " from disk")
    return spacy.blank("en").from_disk(filename)

if __name__ == "__main__":
    generate_list_from_i14y()
    # i14y_list.sort()
    # print(i14y_list)
    print(len(i14y_list))
    sys.exit(0)
    i14y_list = list(set(i14y_list))
    i14y_list.sort()
    i14y_list = remove_english_words_from_list(i14y_list)
    print(len(i14y_list))
    with open ("data/i14y_list.json", "w", encoding="utf-8") as f:
        json.dump(i14y_list, f, indent=4)

    # with open("data/i14y_list.json", "r", encoding="utf-8") as f:
    #     i14y_list = json.load(f)

    # print(i14y_list)
    # sys.exit(0)
    test_docs = []
    # for item in i14y_list:
    #     # print(item)
    #     test_docs.append(get_test_document_from_elasticsearch(item))
    # print(get_test_document_from_elasticsearch(i14y_list[0]))
    print(len(test_docs))
    generate_rules(create_training_data(i14y_list, "ALPHANUMERIC"))

    # sys.exit(0)
    print(str(datetime.datetime.now()) + " Starting Training")
    # print(spacy.info)
    # ray.init(num_cpus=14, num_gpus=0)

    nlp = spacy.load("alpha_numeric")
    TRAINING_DATA = []
    # thread_array = create_ray_threads("/mnt/scratch/ksummers/temp_model")

    # gpu = spacy.require_gpu()
    # print(str(datetime.datetime.now()) + " spaCy using GPU: " + str(gpu))


    for item in i14y_list:
        doc = get_test_document_from_elasticsearch(item)
        # Something to consider for later, maybe, is to extract sentences with the alpha numeric strings out
        if doc != None:
            doc = doc.strip()
            doc = doc.replace("\n", " ")
            doc_segments = doc.split(". ")
            print(str(len(doc_segments)))
            for sentence in doc_segments:
                if not item in sentence:
                    doc_segments.remove(sentence)
            print(str(len(doc_segments)))
            for sentence in doc_segments[:5]:
                results = test_model(nlp, sentence[:500000])
                # print(results)
                if results != None:
                    TRAINING_DATA.append(results)

    TRAINING_DATA = [ele for ele in TRAINING_DATA if ele != []]
    # print(TRAINING_DATA)

    print(str(datetime.datetime.now()) + " Finished processing, saving file")

    with open ("/mnt/trainingdata/ksummers/training_model_sm/training_data_md.json", "w", encoding="utf-8") as f:
        json.dump(TRAINING_DATA, f, indent=4)
