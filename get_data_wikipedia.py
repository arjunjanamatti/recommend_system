import requests

# Parsing HTML
from bs4 import BeautifulSoup

# File system management
import os

base_url = 'https://dumps.wikimedia.org/enwiki/'
index = requests.get(base_url).text
soup_index = BeautifulSoup(index, 'html.parser')

# Find the links that are dates of dumps
dumps = [a['href'] for a in soup_index.find_all('a') if
         a.has_attr('href')]

print(dumps)

dump_url = base_url + '20210319/'

# Retrieve the html
dump_html = requests.get(dump_url).text
print(dump_html[:10])