# Load Spacy Model
# Load Levenshtein Dictionary
# Query ElasticSearch for Documents
# Consider partial document updates...

import argparse
import spacy
import json
# import requests
import sys
import datetime
import signal
import memory_profiler
from elasticsearch import Elasticsearch
from guppy import hpy
from memory_profiler import profile

query = {
    "query" : {
        "bool": {
            "must": [
                {
                    "range" : {
                        "updated_at": {
                            "gte": "SOME_VALUE",
                            "lte": "now"
                        }
                    }
                # },
                # {
                #     "query_string": {
                #         "query": "18th",
                #         "default_field": "content_en"
                #     }
                # },
                # {
                #     "query_string": {
                #         "query": "www.e-publishing.af.mil",
                #         "default_field": "domain_name"
                #     }
                }
            ],
            "filter": [
                {
                    "exists": {
                        "field": "content_en"
                    }
                },
                {
                    "bool": {
                        "should" : [
                            {
                                "term": {
                                    "domain_name": "static.e-publishing.af.mil"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.e-publishing.af.mil"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.goarmy.com"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.gsa.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.uscis.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.va.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "apps.dtic.mil"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.nrc.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.fs.usda.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.justice.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.sec.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.army.mil"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "founders.archives.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.cdc.gov"
                                }
                            },
                            {
                                "term": {
                                    "domain_name": "www.osha.gov"
                                }
                            },
                        ]
                    }
                }
            ],
            "should": [

            ],
            "must_not": [

            ]
        }
        # {},
        # {"query_string": {
        #     "query": "community",
        #     "default_field": "content_en"
        # }}
        # #,
        # # "exists": {
        # #     "field": "SOME_FIELD"
        # # }
    },
    "size" : 10
}

parser = argparse.ArgumentParser(
    prog = "Some Insightful Name",
    description = "What this script does"
)

h = hpy()

# parser.add_argument("-s", "--start_date", required=True, help="Date and time to from which to start processing documents MM-DD-YYYY HH:mm")
# parser.add_argument("-e", "--es_url", required=True, help="The URL of ElasticSearch, should also contain the port")
# parser.add_argument("-i", "--index", required=True, help="The Index to Crawl inside of ElasticSearch")
# Username
# Password
# Consider Ability to load different spacy models
# Consider ability to load different Levenshtein dictionaries


def query_elasticsearch(es_client, scroll_id, scroll_duration = "10m"):
    # Make Request to ElasticSearch
    # print(str(datetime.datetime.now()),"\t",request)
    return es_client.scroll(scroll_id = scroll_id, scroll=scroll_duration)

def create_scroll_elasticsearch(es_client, index, request, scroll_duration = 10):
    # Make a Scrolling request to ElasticSearch
    return es_client.search(
        index = index,
        body = request,
        scroll = str(scroll_duration) + "m"
    )

def push_to_elasticsearch(es_client, index, documents):
    # Use Partial document update to push to ElasticSearch
    es_payload = []
    for document in documents:
        es_payload.append({"update" : {"_index": index, "_id": document["id"]}})
        es_payload.append({"doc" : {"alphanumeric_values": document["alphanumeric_vals"]}})
    es_client.bulk(body=es_payload)

def load_levenshtein_dictionary(file_name):
    levenshtein_dictionary = {}
    dict_file = open(file_name, "r")
    for line in dict_file:
        split_line = line.strip().split(",")
        levenshtein_dictionary[split_line[0]] = split_line[1:]
    dict_file.close()
    return levenshtein_dictionary

def process_alphanumeric_document(document):
    working_doc = alphanumeric_spacy(document)
    # print(working_doc)
    additional_alphanumeric_vals = []
    for token in working_doc:
        if token.ent_type_ == 'ALPHANUMERIC':
            # print(dir(token))
            # print(token, token.is_digit, token.is_alpha)
            # print("18th", token, "18th" == token.text)
            # sys.exit(0)
            if token.text in levenshtein_dictionary:
                additional_alphanumeric_vals.append(levenshtein_dictionary[token.text])
        # print(token.ent_type_)
        # print(dir(token))
    additional_alphanumeric_vals = list(set([item for row in additional_alphanumeric_vals for item in row]))
    # print(additional_alphanumeric_vals)
    # sys.exit(0)
    return additional_alphanumeric_vals

def collect_memory_stats():
    file.write(str(datetime.datetime.now()))
    file.write(str("\n"))
    print(h.heap())
    file.write(str(h.heap()))
    file.write(str("\n"))

@profile
def crawl_es_index(es_client, index, start_date):
    # Do the actual crawling
    # Call scroll
    num_docs_more_than_3m = 0
    modified_query = json.dumps(query).replace("SOME_VALUE", start_date).replace("SOME_FIELD", "updated_at")
    json_result = create_scroll_elasticsearch(es_client, index, modified_query, 60)
    # json_result = results.json()
    num_runs = 0
    # print(json_result)
    scroll_id = json_result["_scroll_id"]
    try:
        while True:
            # Check that there are documents to process
            if len(json_result["hits"]["hits"]) == 0:
                print("Number of documents more than 3MB: ", num_docs_more_than_3m)
                break

            if num_runs == 10:
                num_runs = 0
                collect_memory_stats()
            
            # Process documents
            modified_documents = []
            for document in json_result["hits"]["hits"]:
                # print(document)
                # print(document["_id"])
                if "content_en" in document["_source"]:
                    try:
                        doc = {
                                "id": document["_id"],
                                "alphanumeric_vals": process_alphanumeric_document(document["_source"]["content_en"])
                            }
                        modified_documents.append(doc)
                        doc = None
                        # print(len(modified_documents[-1]["alphanumeric_vals"]))
                    except ValueError as why:
                        num_docs_more_than_3m = num_docs_more_than_3m + 1
            
            # Write Documents to ES
            # print(modified_documents)
            # sys.exit(0)
            if len(modified_documents) > 0:
                push_to_elasticsearch(es_client, index, modified_documents)
            modified_documents = None
            # Query ElasticSearch
            json_result = query_elasticsearch(es_client, scroll_id)
            num_runs = num_runs + 1
    except KeyboardInterrupt:
        print("Exiting")
    file.close()

# Setup Process
args = parser.parse_args()
# start_date = args.start_date
# es_url = args.es_url
# index = args.index

# alphanumeric_spacy = spacy.load("../spacy_model_training/alpha_numeric_ner_model/")
# levenshtein_dictionary = load_levenshtein_dictionary("../levenshtein_final.csv")
alphanumeric_spacy = spacy.load("/mnt/trainingdata/ksummers/alpha_numeric_ner_model/")
alphanumeric_spacy.max_length = 1500000
levenshtein_dictionary = load_levenshtein_dictionary("/mnt/trainingdata/ksummers/levenshtein_final.csv")

# print(levenshtein_dictionary["18th"])
# sys.exit(0)

current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
str_current_datetime = str(current_datetime)
file_name = "/mnt/trainingdata/ksummers/heap_usage_" + str_current_datetime + ".txt"
# file_name = "heap_usage_" + str_current_datetime + ".txt"
file = open(file_name, 'w')


# es_url = "http://localhost:9200/"
es_url = "http://es717x3:9200/"
index = "production-i14y-documents-searchgov-v6"
start_date = "2023-01-01"

elasticsearch_client = Elasticsearch([es_url])

# print(elasticsearch_client.search(index=index))

crawl_es_index(elasticsearch_client, index, start_date)