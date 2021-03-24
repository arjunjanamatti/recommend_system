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

dump_url = base_url + '20210101/'

# Retrieve the html
dump_html = requests.get(dump_url).text
print(dump_html)

# Convert to a soup
soup_dump = BeautifulSoup(dump_html, 'html.parser')

# Find list of elements with the class file
print(soup_dump.find_all('li', {'class': 'file'}, limit = 10)[:4])

files = []

# Search through all files
for file in soup_dump.find_all('li', {'class': 'file'}):
    text = file.text
    # Select the relevant files
    if 'pages-articles' in text:
        files.append((text.split()[0], text.split()[1:]))

print(files)

files_to_download = [file[0] for file in files if '.xml-p' in file[0]]

import sys
from keras.utils import get_file

keras_home = '/home/ubuntu/.keras/datasets/'