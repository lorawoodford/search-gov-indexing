
# create a dictionary
# Read the file in line by line
#  each line 
#   if there's a source entry append to array
#   else create a new source entry and add search value
# Write out file

levenshtein_dictionary = {}

levenshtein_raw = open("../levenshtein_raw.txt", "r")

for line in levenshtein_raw:
    line_split = line.split(",")
    if line_split[0] in levenshtein_dictionary:
        levenshtein_dictionary[line_split[0]].append(line_split[1])
    else:
        levenshtein_dictionary[line_split[0]] = [line_split[1]]

# print(levenshtein_dictionary)

levenshtein_raw.close()

sorted_levenshtein = list(levenshtein_dictionary.keys())
sorted_levenshtein.sort()

# print (sorted_levenshtein)

final_file = open("levenshtein_final.csv", "w", encoding="utf-8")
for key in sorted_levenshtein:
    final_file.write(key + "," + ",".join(levenshtein_dictionary[key]) + "\n")

final_file.close()
