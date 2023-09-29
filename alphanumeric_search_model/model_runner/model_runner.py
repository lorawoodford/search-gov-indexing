# Load Spacy Model
# Load Levenshtein Dictionary
# Query ElasticSearch for Documents
# Consider partial document updates...

import argparse
import spacy
import json
import requests
import sys

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
                "bool": {
                    "should" : [
                        {
                            "match_phrase": {
                                "domain_name.keyword": "static.e-publishing.af.mil"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.e-publishing.af.mil"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.goarmy.com"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.gsa.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.uscis.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.va.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "apps.dtic.mil"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.nrc.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.fs.usda.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.justice.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.sec.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.army.mil"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "founders.archives.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.cdc.gov"
                            }
                        },
                        {
                            "match_phrase": {
                                "domain_name.keyword": "www.osha.gov"
                            }
                        },
                    ]
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
    "size" : 5000
}

parser = argparse.ArgumentParser(
    prog = "Some Insightful Name",
    description = "What this script does"
)

# parser.add_argument("-s", "--start_date", required=True, help="Date and time to from which to start processing documents MM-DD-YYYY HH:mm")
# parser.add_argument("-e", "--es_url", required=True, help="The URL of ElasticSearch, should also contain the port")
# parser.add_argument("-i", "--index", required=True, help="The Index to Crawl inside of ElasticSearch")
# Username
# Password
# Consider Ability to load different spacy models
# Consider ability to load different Levenshtein dictionaries


def query_elasticsearch(url, search_endpoint, request = ""):
    # Make Request to ElasticSearch
    print(request)
    r = requests.get(
        url + search_endpoint,
        headers = {"Content-Type": "application/json"},
        data=request
    )
    # print(r.json())
    return r

def create_scroll_elasticsearch(url, index, request, scroll_duration = 10):
    # Make a Scrolling request to ElasticSearch
    return query_elasticsearch(
        url,
        index + "/_search?scroll=" + str(scroll_duration) + "m",
        request
    )

def push_to_elasticsearch(url, index, documents):
    # Use Partial document update to push to ElasticSearch
    es_payload = []
    for document in documents:
        es_payload.append(json.dumps({"index" : {"_index": index, "_id": document["id"]}}))
        es_payload.append(json.dumps({"doc": {"alphanumeric_values": document["alphanumeric_vals"]}}))
    requests.post(
        url + index + "/_bulk",
        headers = {"Content-Type": "application/json"},
        data="\n".join(es_payload)
    )

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

def crawl_es_index(es_url, index, start_date):
    # Do the actual crawling
    # Call scroll
    num_docs_more_than_3m = 0
    modified_query = json.dumps(query).replace("SOME_VALUE", start_date).replace("SOME_FIELD", "updated_at")
    results = create_scroll_elasticsearch(es_url, index, modified_query, 60)
    json_result = results.json()
    # print(json_result)
    scroll_id = json_result["_scroll_id"]
    while True:
        # Check that there are documents to process
        if len(json_result["hits"]["hits"]) == 0:
            print("Number of documents more than 3MB: ", num_docs_more_than_3m)
            break
        
        # Process documents
        modified_documents = []
        for document in json_result["hits"]["hits"]:
            # print(document)
            if "content_en" in document["_source"]:
                try:
                    modified_documents.append(
                        {
                            "id": document["_id"],
                            "alphanumeric_vals": process_alphanumeric_document(document["_source"]["content_en"])
                        }
                    )
                except ValueError as why:
                    num_docs_more_than_3m = num_docs_more_than_3m + 1
        
        # Write Documents to ES
        # print(modified_documents)
        # sys.exit(0)
        push_to_elasticsearch(es_url, index, modified_documents)
        # Query ElasticSearch
        results = query_elasticsearch(es_url, "_search/scroll",
            json.dumps({
                "scroll": "10m",
                "scroll_id": scroll_id
            })
        )
        json_result = results.json()

# Setup Process
args = parser.parse_args()
# start_date = args.start_date
# es_url = args.es_url
# index = args.index

alphanumeric_spacy = spacy.load("/mnt/trainingdata/ksummers/alpha_numeric_ner_model/")
alphanumeric_spacy.max_length = 3000000
levenshtein_dictionary = load_levenshtein_dictionary("/mnt/trainingdata/ksummers/levenshtein_final.csv")

# print(levenshtein_dictionary["18th"])
# sys.exit(0)

es_url = "http://es717x3:9200/"
index = "production-i14y-documents-searchgov-v6"
start_date = "2023-01-01"

crawl_es_index(es_url, index, start_date)