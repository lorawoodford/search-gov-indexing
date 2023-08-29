import pytest
import sys
import spacy
import requests

sys.path.append("../")

import training_data_collector

def test_query_elasticsearch():
    # assert False
    # Query elasticsearch
    es_query = training_data_collector.query_elasticsearch("", "")
    # Assert that Elasticsearch responds with a 200
    assert es_query.status_code == 200

def test_generate_list_from_i14y():
    # assert False
    # Probably best way to handle this is to generate a list from the top five domains and ensure they are the same
    assert len(training_data_collector.generate_list_from_i14y()) == 6500

def test_get_test_document_from_elasticsearch():
    # assert False
    # Use a value to search for a document
    document = training_data_collector.get_test_document_from_elasticsearch("SOME STRING")
    # Assert that a document is returned by ES 
    assert isinstance(document, str)

def test_generate_rules():
    assert False
    # This creates an entity ruler and save to disk...

def test_create_training_data():
    # assert False
    data_type = "NUMERICALALPHA"
    # Create a list of values and a data type to go with
    items = ["the", "quick", "brown", "fox"]
    # Assert that all values have the appropriate data type
    test_data = training_data_collector.create_training_data(items, data_type)
    for item, test_d in zip(items, test_data):
        assert test_d["pattern"] == item and test_d["label"] == data_type

def test_load_nlp():
    # assert False
    # Assert that loading a model returns a spacy model object
    assert isinstance(training_data_collector.load_nlp("../alpha_numeric_ner_model"), spacy.lang.en.English)

def test_test_model():
    assert False
    # Not sure how to test this, will need to look at the return values

def test_remove_english_words_from_list():
    # Create a list of english and non english words
    list_of_words = ["the", "brown", "fix", "abc123", "dd214", "i9", "1099R"]
    # Assert that the english words have been removed
    removed_list = training_data_collector.remove_english_words_from_list(list_of_words, "../data/english_words.txt")
    print(list_of_words)
    assert len(list_of_words) == 5
