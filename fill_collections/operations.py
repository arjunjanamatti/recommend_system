import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json
import pandas as pd
from datetime import timedelta
from math import *
import random

##### VARIABLES
files_list = ['reviews_1.json','likes_1.json']

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

def MergedDataframe(files_list):
    tables_dictionary = GetTableDictionary(files_list)
    # transform the reviews_1 table to df_1 dataframe
    df_1 = tables_dictionary['reviews_1']
    # select reviews which are approved
    df_1_approve = (df_1[df_1['isApprove'] == 'approved'])
    # transform the likes_1 table to df_1 dataframe
    df_2 = tables_dictionary['likes_1']
    # rename the column name in reviews_1 table to resourceId as per likes_1 table
    df_1_approve = df_1_approve.rename(columns={"_id": "resourceId"})
    # merge both the dataframes based on common column 'resourceId'
    df_merge = df_1_approve.merge(df_2, how='left', on='resourceId')

    # extract only required columns from the merged dataframe
    df_merge_1 = df_merge[['resourceId', 'loc', 'createdAt_x', 'updatedAt_x', 'fromUserId_x']]
    # longititude extraction from the loc
    longitude = [_['coordinates'][0] for _ in df_merge_1['loc']]

    latitude = [_['coordinates'][1] for _ in df_merge_1['loc']]
    df_merge_1['longitude'] = longitude
    df_merge_1['latitude'] = latitude
    df_merge_1.drop(labels='loc', inplace=True, axis=1)
    created_dates = ([_.split('T')[0] for _ in df_merge_1['createdAt_x']])
    updated_dates = ([_.split('T')[0] for _ in df_merge_1['updatedAt_x']])
    df_merge_1['created_dates'] = created_dates
    df_merge_1['updated_dates'] = updated_dates
    df_merge_1['created_dates'] = pd.to_datetime(df_merge_1['created_dates'])
    df_merge_1['updated_dates'] = pd.to_datetime(df_merge_1['updated_dates'])

    df_merge_1.drop(labels=['createdAt_x', 'updatedAt_x'], inplace=True, axis=1)
    return df_merge_1

