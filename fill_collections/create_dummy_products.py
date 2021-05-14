import json
import random
import time
from datetime import datetime
import pickle
from pymongo import MongoClient
import pandas as pd
from pymongo.errors import ConnectionFailure
from itertools import islice
from random import randint

def random_chunk(li, min_chunk=10, max_chunk=11):
    it = iter(li)
    while True:
        nxt = list(islice(it,randint(min_chunk,max_chunk)))
        if nxt:
            yield nxt
        else:
            break

with open('resourceId.pickle', 'rb') as f:
    resourceId = pickle.load(f)

review_id_tags = list(random_chunk(resourceId))
print(review_id_tags)
print(len(review_id_tags))

with open('products.json') as file:
    products = json.load(file)
products_list = []
print(len(products))
for index, pro in enumerate(products):
    pro['review_id_tags'] = review_id_tags[index]
    products_list.append(pro)
print(products_list)

print(len(products_list))

# with open("products_1.json", "w") as fp:
#     json.dump(products_list , fp, indent=4)
#
# def looping_json_files(files_list):
#     list_1 = []
#     for files in files_list:
#         with open(files) as file:
#             data = json.load(file)
#             list_1.append(data)
#     return list_1
#
# def SendJsonFilesToMongoDB(files_list):
#     'this function will send the json files to MongoDB'
#     try:
#         myclient = MongoClient()
#         mydb = myclient['real_reviews']
#         # collections is similar to tables in mongo dn
#         list_1 = looping_json_files(files_list)
#         for index, file in enumerate(files_list):
#             print(file)
#             print(file.split('.')[0])
#             my_collection = mydb[file.split('.')[0]]
#             my_collection.insert(doc_or_docs=list_1[index])
#
#         print('Data sent to the database')
#
#     except ConnectionFailure:
#         print('Failed to connect to mongoDB database')
#
# files_list = ['products_1.json']
#
# # only first time use the below function
# try:
#     SendJsonFilesToMongoDB(files_list)
# except Exception as e:
#     print(e)