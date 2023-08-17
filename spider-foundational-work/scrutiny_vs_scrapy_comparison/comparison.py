import csv
import difflib
import urllib.parse
import re
from comparison_functions import *
import pandas as pd

# to run, enter at the prompt: `python3 comparison.py`

suf = '/scrapy_urls' # suf is short for scrapy_urls_folder 
scUf = '/scrutiny_urls/processed' # scUf is short for scrutiny_urls_folder

scrapy_paths = [suf + '/armymwr_urls.csv', 
               suf + '/james_webb_urls.csv',
               suf + '/travel_dod_mil_urls.csv',
               suf + '/veteran_affair_urls.csv']
scrutiny_paths = [scUf + '/armymwr.txt', 
                scUf + '/james_webb.txt',
                scUf + '/travel_dod_mil.txt',
                scUf + '/veteran_affairs.txt']
text_gaps = ['armymwr',
             'james webb',
             'travel.dod.mil',
             'veteran_affairs'
             ]

for i in range(0, len(scrapy_paths)):
    scrapy_urls = csv_to_set(scrapy_paths[i])
    scrutiny_urls = txt_to_set(scrutiny_paths[i])
    url_sets = make_comparisons(scrapy_urls, scrutiny_urls)
    with open('processed/' + str(text_gaps[i]) + '.csv', 'w', newline='') as csvfile:
        # Create a CSV writer object
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(pd.Series(list(url_sets[0])))
        writer.writerow(pd.Series(list(url_sets[1])))
        writer.writerow(pd.Series(list(url_sets[2])))
    csvfile.close()







