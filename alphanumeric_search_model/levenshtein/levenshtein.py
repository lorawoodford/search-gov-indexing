# Pull all RegEx Expressions From i14y Together
#   From ElasticSearch (this needs to be removed not the tool for the job)
#       Query on Terms for Domain_name
#       For Each domain name, query ES for regexes
#   Put all expressions in Array/List
#   Create Unique List
# Pull all RegEx From Logstash Documents
#   From ElasticSearch (This needs to be removed, not the tool for the job)
#       Query on Terms for Affiliate
#       For Each Affiliate query ES for RegEx
# Sort each list? This will get first characters to be the same
# Sort each list by length? This will group together similar length
# Do Levenshtein Compare between lists (currently an n^n operation, would there be a faster way?)

import requests
from Levenshtein import ratio
import json

query1 = {
    "aggs": {
        "regex_count": {
            "terms": {
                "field": "REPLACE_THIS.keyword",
                "size": 10
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
                "size": 8000
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

i14y_list = []
logstash_list = []

distance_touple = []

es_url = "http://es717x3:9200/"

def query_elasticsearch(search_endpoint, body = ""):
    r = requests.get(
        es_url + search_endpoint, 
        headers = {"Content-Type": "application/json"},
        data=body
        )
    return r

def get_levenshtein_distance(word_one, word_two):
    lratio = ratio(word_one, word_two, score_cutoff=0.7)
    if lratio > 0.0:
        distance_touple.append([word_one, word_two, str(lratio)])

def generate_list_from_i14y():
    search_endpoint = "production-i14y-documents-searchgov-v6-reindex_keyword_regex/_search"
    r = query_elasticsearch(search_endpoint, json.dumps(query1).replace("REPLACE_THIS", "domain_name"))
    response = json.loads(r.text)
    print(response['aggregations']['regex_count']['buckets'])
    # domains = response['aggregations']['regex_count']['buckets']
    domains = ["static.e-publishing.af.mil", "www.e-publishing.af.mil"]
    # Iterate over keys:
    for domain in domains:
        print( domain)
        payload = json.dumps(query2).replace("THIS_AS_WELL", domain).replace("REPLACE_THIS", "domain_name")
        print(payload)
        es_query_val = json.loads(query_elasticsearch(search_endpoint, payload).text)
        for regex_pattern in es_query_val["aggregations"]["regex_patterns"]["buckets"]:
            # print(regex_pattern["key"])
            i14y_list.append(regex_pattern["key"])

def generate_list_from_logstash_requests():
    search_endpoint = "human-logstash-_regex/_search"
    r = query_elasticsearch(search_endpoint, json.dumps(query1).replace("REPLACE_THIS", "affiliate"))
    response = json.loads(r.text)
    print(response)
    for affiliate in response['aggregations']['regex_count']['buckets']:
        print(affiliate["key"])
        es_query_val = json.loads(query_elasticsearch(search_endpoint, json.dumps(query2).replace("REPLACE_THIS", "affiliate").replace("THIS_AS_WELL", affiliate['key'])).text)
        for regex_pattern in es_query_val["aggregations"]["regex_patterns"]["buckets"]:
            logstash_list.append(regex_pattern["key"])


#query_elasticsearch("production-i14y-documents-searchgov-v6-reindex_keyword_regex/_search", json.dumps(query1))
generate_list_from_i14y()
# some_list = i14y_list.sort()
print(i14y_list)
print(len(set(i14y_list)))
generate_list_from_logstash_requests()
print(logstash_list)
print(len(set(logstash_list)))
for i14y in set(i14y_list):
    for logstash_word in set(i14y_list):
        get_levenshtein_distance(i14y, logstash_word)
# print(*distance_touple, sep='\n')

with open ("/mnt/trainingdata/ksummers/levenshtein_raw_epubs.txt", "w", encoding="utf-8") as f:
    for line in distance_touple:
        f.write(",".join(line)+"\n")
