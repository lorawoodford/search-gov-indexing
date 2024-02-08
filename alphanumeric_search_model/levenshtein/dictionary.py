
# create a dictionary
# Read the file in line by line
#  each line 
#   if there's a source entry append to array
#   else create a new source entry and add search value
# Write out file

import re

# levenshtein_dictionary = {}

def create_changed_punctuation_array(word):
    punctuation_regex = "[-\s\.§]"
    punctuation_substitutions = ["-", " ", ".", "§"]
    word_array = [word, re.sub(punctuation_regex, "", word)]
    for letter in punctuation_substitutions:
        # print(letter)
        # print(word_array)
        word_array.append(re.sub(punctuation_regex, letter, word))
    
    return list(set(word_array))

# print(create_changed_punctuation_array("DD-214"))
# print(create_changed_punctuation_array("15th"))
# print(create_changed_punctuation_array("DD-214"))

def generate_leveshtein_dictionary(raw_lines):
    levenshtein_dictionary = {}
    for line in raw_lines:
        line_split = line.split(",")
        if line_split[0] in levenshtein_dictionary:
            levenshtein_dictionary[line_split[0]] = levenshtein_dictionary[line_split[0]] + create_changed_punctuation_array(line_split[1])
        else:
            levenshtein_dictionary[line_split[0]] = create_changed_punctuation_array(line_split[1])
    
    return levenshtein_dictionary

# for line in levenshtein_raw:
#     line_split = line.split(",")
#     if line_split[0] in levenshtein_dictionary:
#         levenshtein_dictionary[line_split[0]] = levenshtein_dictionary[line_split[0]] + create_changed_punctuation_array(line_split[1])
#     else:
#         levenshtein_dictionary[line_split[0]] = create_changed_punctuation_array(line_split[1])

# print(levenshtein_dictionary)

if __name__ == "__main__":
    levenshtein_raw = open("/mnt/trainingdata/ksummers/levenshtein_raw.txt", "r")

    levenshtein_dictionary = generate_leveshtein_dictionary(levenshtein_raw)

    levenshtein_raw.close()

    sorted_levenshtein = list(levenshtein_dictionary.keys())
    sorted_levenshtein.sort()

    # print (sorted_levenshtein)

    final_file = open("/mnt/trainingdata/ksummers/levenshtein_final.csv", "w", encoding="utf-8")
    for key in sorted_levenshtein:
        final_file.write(key + "," + ",".join(list(set(levenshtein_dictionary[key]))) + "\n")

    final_file.close()
