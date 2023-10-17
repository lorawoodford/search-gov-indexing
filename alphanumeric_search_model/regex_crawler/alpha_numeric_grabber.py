import requests
import json
import spacy
import re

from spacy.lang.en import English
from spacy.pipeline import EntityRuler
from spacy.training.example import Example

query = {
    "sort": [
        {
            "_score" :{ "order" : "desc"}
        }
    ],
    "size" : 1000,
    "query" : {
        "bool" :{
            "filter": [
                {
                    "regexp": {
                        "SOME_IMPORTANT_FIELD": "SOME_REGEX_PATTERN"
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
            ]
        }
    }
}

indices = {
    "production-i14y-documents-searchgov-v6-reindex_keyword" : [
        "title_en.raw",
        "title_en.raw.keyword",
        "content_en.raw",
        "content_en.raw.keyword"
    ],
    # "human_logstash-*": [
    #     "params.query.raw"
    # ]
}

es_urls = [
    # "http://localhost:9200"
    "http://es717x1:9200",
    "http://es717x2:9200",
    "http://es717x3:9200",
    "http://es717x4:9200",
]

regex_expressions = [
    '[0-9]+[a-z]+',
    "[a-z]+[0-9]+",
    "([a-z]+[0-9]*-[a-z]+[0-9]*)",
    "([a-z]+[0-9]*-[0-9]+[a-z]*)",
    "([0-9]+[a-z]*-[a-z]+[0-9]*)",
    "([0-9]+[a-z]*-[0-9]+[a-z]*)",
    # "([a-z]+[0-9]*§[a-z]+[0-9]*)",
    # "([a-z]+[0-9]*§[0-9]+[a-z]*)",
    # "([0-9]+[a-z]*§[a-z]+[0-9]*)",
    # "([0-9]+[a-z]*§[0-9]+[a-z]*)",
    # "([a-z]+[0-9]*.[a-z]+[0-9]*)",
    "([a-z]+[0-9]*\\\.[a-z]+[0-9]+)",
    "([a-z]+[0-9]+\\\.[a-z]+[0-9]*)",
    "([a-z]+[0-9]+\\\.[a-z]+[0-9]+)",
    "([a-z]+[0-9]*\\\.[0-9]+[a-z]*)",
    "([0-9]+[a-z]*\\\.[a-z]+[0-9]*)",
    "([0-9]+[a-z]*\\\.[0-9]+[a-z]*)",
    "([a-z]+[0-9]*\\\s[a-z]+[0-9]+)",
    "([a-z]+[0-9]+\\\s[a-z]+[0-9]*)",
    "([a-z]+[0-9]*\\\s[0-9]+[a-z]*)",
    "([0-9]+[a-z]*\\\s[a-z]+[0-9]*)",
    "([0-9]+[a-z]+\\\s[0-9]+[a-z]*)",
    "([0-9]+[a-z]*\\\s[0-9]+[a-z]+)",
]

def query_elasticsearch(es_url, search_endpoint, body = ""):
    r = requests.get(
        es_url + search_endpoint, 
        headers = {"Content-Type": "application/json"},
        data=body
        )
    return r

def create_scroll_elasticsearch(url, index, body, scroll_duration):
    return query_elasticsearch(
        url,
        "/" + index + "/_search?scroll=" + str(scroll_duration) + "m",
        body
    )

def push_to_elasticsearch(url, index, documents):
    # Use Partial document update to push to ElasticSearch
    es_payload = []
    for document in documents:
        es_payload.append(json.dumps({"index" : {"_index": index}}))
        es_payload.append(json.dumps({"doc" : document}))
    tmp = requests.post(
        url + "/_bulk",
        headers = {"Content-Type": "application/json"},
        data="\n".join(es_payload) + "\n"
    )
    return tmp

def create_i14y_doc(doc, regex, field):
    # print(doc)
    # print(re.findall(regex.replace("\\\\", "\\"), doc["_source"][field]))
    return {
        "domain_name" : doc["_source"]["domain_name"],
        "regex_patterns" : re.findall(regex.replace("\\\\", "\\"), doc["_source"][field])
    }
    # return

def crawl_es_index_with_field(es_url, index, field, regex_pattern):
    modified_query = json.dumps(query).replace("SOME_REGEX_PATTERN", regex_pattern).replace("SOME_IMPORTANT_FIELD", field)
    results = create_scroll_elasticsearch(es_url[0], index, modified_query, 60)
    json_result = results.json()
    es_node = 0

    # print(json_result)

    if(results.status_code == 400):
        print(json_result)
        return

    scroll_id = json_result["_scroll_id"]
    while True:
        if len(json_result["hits"]["hits"]) == 0:
           break
        
        temp_doc_list = []

        for document in json_result["hits"]["hits"]:
            scan_field = field.replace(".raw", "").replace(".keyword", "")
            temp_doc_list.append(create_i14y_doc(document, regex_pattern, scan_field))
        
        response = push_to_elasticsearch(es_url[es_node], index + "_regex_py", temp_doc_list)
        if(response.status_code == 400):
            print(response.json())
            # return
            
        results = query_elasticsearch(es_url[es_node], "/_search/scroll", {
            "scroll" : "10m",
            "scroll_id": scroll_id
        })
        es_node = (es_node + 1) % len(es_urls)
    # return

for index in list(indices.keys()):
    for field in indices[index]:
        for regexp in regex_expressions:
            print(" ****************** ", regexp, " ****************** ")
            crawl_es_index_with_field(es_urls, index, field, regexp)