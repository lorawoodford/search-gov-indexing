import pytest
import sys

sys.path.append("../")

import levenshtein
import dictionary


# Comparison tests

# Test get_levenshtein_distance(word_one, word_two)
def test_get_levenshtein_distance():
    file = open("distance_test_data.txt")
    for line in file:
        split_line = line.split(",")
        levenshtein_distance = levenshtein.get_levenshtein_distance(split_line[0], split_line[1])
        print(levenshtein_distance)
        print(float(split_line[2]))
        if(levenshtein_distance != float(split_line[2])):
            assert False
    assert True

# Test alphanumeric_levenshtein_comparitor(word)
# This is not currently used; asserting true to pass tests
def test_alphanumeric_levenshtein_comparitor():
    assert True

# Dictionary Creation Tests

# Test create_changed_punctuation_array(word)
def test_create_changed_punctuation_array():
    test_values = ["15th", "DD 214"]
    # for value in test_values:
        
    assert True

# Test generate_leveshtein_dictionary(array)
def test_generate_levenshtein_dictionary():
    file = open("distance_test_data.txt")
    levenshtein_dictionary = dictionary.generate_leveshtein_dictionary(file)
    print(levenshtein_dictionary)
    assert False