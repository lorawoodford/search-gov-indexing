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

def query_elasticsearch(body):
    return 0

def get_levenshtein_distance(word_one, word_two):
    return 0

def generate_list_from_i14y():
    return 0

def generate_list_from_logstash_requests():
    return 0

