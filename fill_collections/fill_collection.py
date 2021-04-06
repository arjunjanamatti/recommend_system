import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json

def looping_json_files(files_list):
    list_1 = []
    for files in files_list:
        with open(files) as file:
            data = json.load(file)
            list_1.append(data)
    return list_1


def main():
    'connect to the database'
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


        # my_collection = mydb['reviews']
        #
        # my_collection.insert(doc_or_docs = my_dictionary)
        #
        print('Data sent to the database')

    except ConnectionFailure:
        print('Failed to connect to mongoDB database')



files_list = ['reviews.json', 'likes.json']
main()