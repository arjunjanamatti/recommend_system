import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json
import pandas as pd
from datetime import timedelta
from math import *
import random

def looping_json_files(files_list):
    list_1 = []
    for files in files_list:
        with open(files) as file:
            data = json.load(file)
            list_1.append(data)
    return list_1


def SendJsonFilesToMongoDB(files_list):
    'this function will send the json files to MongoDB'
    try:
        myclient = MongoClient()
        mydb = myclient['real_reviews']
        # collections is similar to tables in mongo dn
        list_1 = looping_json_files(files_list)
        for index, file in enumerate(files_list):
            print(file)
            print(file.split('.')[0])
            my_collection = mydb[file.split('.')[0]]
            my_collection.insert(doc_or_docs=list_1[index])

        print('Data sent to the database')

    except ConnectionFailure:
        print('Failed to connect to mongoDB database')

def GetTableDictionary(files_list):
    myclient = MongoClient()
    mydb = myclient['real_reviews']
    list_1 = looping_json_files(files_list)
    print(list_1)
    tables_dictionary = {}
    for index, file in enumerate(files_list):
        print(file)
        my_collection = mydb[file.split('.')[0]]

        #     ### Find all the data
        #     print()
        #     print('Entire dataset: ')
        list_data = my_collection.find()
        df = pd.DataFrame(list(list_data))
        tables_dictionary[file.split('.')[0]] = df
    return tables_dictionary
