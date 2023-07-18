# Load the alphanumeric data
# Create Training data
# Generate Rules

# Test the model

import requests
import json
import spacy
import sys
import datetime
import time
import random
import os
from spacy.lang.en import English
from spacy.pipeline import EntityRuler
from spacy.training.example import Example

from threading import Thread
import queue

import ray

query1 = {
    "aggs": {
        "regex_count": {
            "terms": {
                "field": "REPLACE_THIS.keyword",
                "size": 1000
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
        "bool": {
            "filter": [
                {
                    "term": {
                        "REPLACE_THIS.keyword": {
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

training_creation_queue = queue.Queue(maxsize=100000)
ray_object_id_queue = queue.Queue(maxsize=4500)
processed_queue = queue.Queue(maxsize=100000)

class TrainingDataProcessor(Thread):
    def __init__(self, training_dataset):
        Thread.__init__(self)
        self.training_dataset = training_dataset
    
    def run(self):
        print(str(datetime.datetime.now()) + " Starting Training Data Processor")
        for text, annotations in self.training_dataset:
            training_creation_queue.put([text,annotations])

# End TrainingDataProcessor


class ExampleCreator(Thread):
    def __init__(self, nlp):
        Thread.__init__(self)
        self.nlp = nlp
    
    def run(self):
        # nlp_ref = ray.put(self.nlp)
        print(str(datetime.datetime.now()) + " Starting Example Creator")
        remote_container = ray.remote(ExampleContainer)
        actor_handle = remote_container.remote(self.nlp) #ExampleContainer.remote(self.nlp))
        while(True):
            example = training_creation_queue.get()
            # ray_obj = create_example.remote(example[0], example[1], self.nlp)
            # print(example)
            ray_obj = actor_handle.create_example.remote(example)
            # print(ray_obj)
            ray_object_id_queue.put(ray_obj)
    
    # @ray.remote
    # def create_example(text, annotations, nlp):
    #     return [Example.from_dict(nlp.make_doc(text), annotations)]

# End ExampleCreator

# @ray.remote
class ExampleContainer:
    def __init__(self, nlp):
        self.nlp = nlp

    def create_example(self, example_array):
        return [Example.from_dict(self.nlp.make_doc(example_array[0]), example_array[1])]

# End ExampleContainer

class ExamplePusher(Thread):
    def __init__(self):
        Thread.__init__(self)
    
    def run(self):
        print(str(datetime.datetime.now()) + " Starting Example Pusher")
        while(True):
            processed_queue.put(ray.get(ray_object_id_queue.get()))

# End ExamplePusher

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
    # print(response)
    # print(response['aggregations']['regex_count']['buckets'])
    # Iterate over keys:
    for domain in response['aggregations']['regex_count']['buckets']:
        print( domain['key'])
        payload = json.dumps(query2).replace("THIS_AS_WELL", domain['key']).replace("REPLACE_THIS", "domain_name")
        # print(payload)
        es_query_val = json.loads(query_elasticsearch(search_endpoint, payload).text)
        for regex_pattern in es_query_val["aggregations"]["regex_patterns"]["buckets"]:
            # print(regex_pattern["key"])
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

def remove_english_words_from_list(list_of_words):
    f = open("data/english_words.txt", "r")
    english_words = f.read().split("\n")
    for word in list_of_words:
        if word in english_words:
            list_of_words.remove(word)
    return list_of_words

def create_ray_threads(nlp):
    print(str(datetime.datetime.now()) + " Creating Ray Threads")    
    nlp_ref = ray.put(nlp)
    example_creator = ExampleCreator(nlp_ref)
    example_creator.daemon = True
    example_creator.name = "Example_Creator"
    example_creator.start()
    thread_array.append(example_creator)
    for n in range(2): #(os.cpu_count() - 2)):
        print(str(datetime.datetime.now()) + " Creating Ray Getting Thread: " + str(n))
        t = ExamplePusher()
        t.daemon = True
        t.name = "Example_Pusher_" + str(n)
        t.start()
        thread_array.append(t)
    print(str(datetime.datetime.now()) + " Finished creating Ray Threads")
    return(thread_array)

def train_spacy(data, iterations):
    print(str(datetime.datetime.now()) + " Starting Training")
    TRAIN_DATA = data
    nlp = spacy.blank("en")
    print(nlp.pipe_names)
    if "ner" not in nlp.pipe_names:
        ner = nlp.create_pipe("ner")
        nlp.add_pipe("ner", last=True)
    print(str(datetime.datetime.now()) + " Starting Entity processing")
    for _, annotations in TRAIN_DATA:
        for ent in annotations.get("entities"):
            ner.add_label(ent[2])
    print(str(datetime.datetime.now()) + " Finished Entity processing")
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]
    with nlp.disable_pipes(*other_pipes):
        example_pusher_threads = []
        thread_array = create_ray_threads(nlp)
        optimizer = nlp.begin_training()
        for iteration in range(iterations):
            print(str(datetime.datetime.now()) + " Starting iteration: " + str(iteration))
            random.shuffle(TRAIN_DATA)
            print(str(datetime.datetime.now()) + " Finished shuffling data")
            losses = {}
            # Create Worker Threads
            trainer = TrainingDataProcessor(TRAIN_DATA)
            trainer.start()
            time.sleep(10)
            print(str(datetime.datetime.now()) + " Starting Actual Training")
            while(not processed_queue.empty() or trainer.is_alive()):
                nlp.update(
                    processed_queue.get(),
                    drop = 0.2,
                    sgd = optimizer,
                    losses = losses
                )
            # for text, annotations in TRAIN_DATA:
            #     example = Example.from_dict(nlp.make_doc(text), annotations) # This needs to be parallelized, not just multithreaded
            #     print(str(datetime.datetime.now()) + " Example")
            #     nlp.update(
            #         [example], # Processed_queue.get()
            #         drop = 0.2,
            #         sgd=optimizer,
            #         losses = losses
            #     )
            #     print(str(datetime.datetime.now()) + " NLP")

                # sys.exit(0)
            # Make sure all worker threads are finished
            print(losses)
        # Clean up threads
    print(str(datetime.datetime.now()) + " Completed Training")
    return(nlp)

# generate_list_from_i14y()
# i14y_list.sort()
# # print(i14y_list)
# print(len(i14y_list))
# i14y_list = list(set(i14y_list))
# i14y_list.sort()
# i14y_list = remove_english_words_from_list(i14y_list)
# print(len(i14y_list))
# with open ("data/i14y_list.json", "w", encoding="utf-8") as f:
#     json.dump(i14y_list, f, indent=4)

# with open("data/i14y_list.json", "r", encoding="utf-8") as f:
#     i14y_list = json.load(f)

# print(i14y_list)
# sys.exit(0)
# test_docs = []
# for item in i14y_list:
#     print(item)
#     test_docs.append(get_test_document_from_elasticsearch(item))
# # print(get_test_document_from_elasticsearch(i14y_list[0]))
# print(len(test_docs))
# generate_rules(create_training_data(i14y_list, "ALPHANUMERIC"))

# sys.exit(0)
print(str(datetime.datetime.now()) + " Starting Training")
gpu = spacy.require_gpu()
print(str(datetime.datetime.now()) + " spaCy using GPU: " + str(gpu))
print(spacy.info)
ray.init(num_cpus=14, num_gpus=0)

# nlp = spacy.load("alpha_numeric")
# TRAINING_DATA = []

# for item in i14y_list:
#     doc = get_test_document_from_elasticsearch(item)
#     if doc != None:
#         doc = doc.strip()
#         doc = doc.replace("\n", " ")
#         results = test_model(nlp, doc[:500000])
#         # print(results)
#         if results != None:
#             TRAINING_DATA.append(results)

# TRAINING_DATA = [ele for ele in TRAINING_DATA if ele != []]
# print(TRAINING_DATA)

# with open ("/mnt/trainingdata/ksummers/training_data.json", "w", encoding="utf-8") as f:
#     json.dump(TRAINING_DATA, f, indent=4)

print(str(datetime.datetime.now()) + " Reading in Training Dataset")
with open ("/mnt/trainingdata/ksummers/training_data.json", "r", encoding="utf-8") as f:
    TRAINING_DATA = json.load(f)
print(str(datetime.datetime.now()) + " Finished reading Training Dataset")

nlp = train_spacy(TRAINING_DATA, 30)
print(str(datetime.datetime.now()) + " Writing spaCy model to disk")
nlp.to_disk("/mnt/trainingdata/ksummers/alpha_numeric_ner_model")

print(str(datetime.datetime.now()) + " Finished training")