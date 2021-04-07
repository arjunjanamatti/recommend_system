import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json
import pandas as pd

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

def get_data():
    try:
        myclient = MongoClient()
        mydb = myclient['real_reviews']
        # collections is similar to tables in mongo dn
        list_1 = looping_json_files(files_list)
        tables_dictionary = {}
        for index, file in enumerate(files_list):

            my_collection = mydb[file.split('.')[0]]

            ### Find all the data
            print()
            print('Entire dataset: ')
            list_data = my_collection.find()
            for i in list_data:
                print(i)

            try_list = defaultdict(list)
            list_data = my_collection.find()

            for j in list_data:
                for k in j.keys():
                    try_list[k].append(j[k])
                # try_list[j] .append(list_data[j])

            # print(try_list)
            col_list = list(try_list.keys())
            print(len(col_list))
            # df = pd.DataFrame([(k, v[0][0], v[0][1]) for k, v in try_list.items()],
            #        columns=col_list)
            # print(df.head())
            df = pd.DataFrame(list(list_data))
            print(df.head())


    except ConnectionFailure:
        print('Failed to connect to mongo DB database')

files_list = ['reviews.json', 'likes.json']
# main()

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
print(tables_dictionary)
    # print(list_data)
#     for i in list_data:
#         print(i)
#
#     try_list = defaultdict(list)
#     list_data = my_collection.find()
#
#     for j in list_data:
#         for k in j.keys():
#             try_list[k].append(j[k])
#         # try_list[j] .append(list_data[j])
#
#     # print(try_list)
#     col_list = list(try_list.keys())
#     print(len(col_list))
#     # df = pd.DataFrame([(k, v[0][0], v[0][1]) for k, v in try_list.items()],
#     #        columns=col_list)
#     # print(df.head())
#     df = pd.DataFrame(list(list_data))
#     print(df.head())

df_1 = tables_dictionary['reviews']
df_1_approve = (df_1[df_1['isApprove']=='approved'])
df_2 = tables_dictionary['likes']
df_1_approve=df_1_approve.rename(columns={"_id": "resourceId"})
df_merge = pd.merge(df_1_approve,df_2, on='resourceId')
df_merge.to_csv('df_merge.csv')