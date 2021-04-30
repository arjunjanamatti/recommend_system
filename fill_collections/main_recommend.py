from pymongo import MongoClient
import json
import pandas as pd
from datetime import timedelta
from math import *
from flask import Flask, request
import numpy as np
import time


class recommend_results:
    def GetBlockUsersData(self):
        myclient = MongoClient(host=None, port=None)
        mydb = myclient['real_reviews']
        coll = mydb['blockusers']
        cur = coll.find()
        block_users_dict_list = [doc for doc in cur]

        self.try_dict = {}
        for new in block_users_dict_list:
            self.try_dict[new['_id']] = []
            self.try_dict[new['_id']].append(new['blockUserId'])
            self.try_dict[new['_id']].append(new['fromUserId'])

    def GetTableDictionary(self):
        myclient = MongoClient(host='localhost', port=27017)
        mydb = myclient['real_reviews']
        # list_1 = self.looping_json_files()
        self.files_list = ['reviews.json', 'likes.json']
        # self.files_list = ['reviews_1.json', 'likes_1.json']
        self.tables_dictionary = {}
        for index, file in enumerate(self.files_list):
            my_collection = mydb[file.split('.')[0]]
            list_data = my_collection.find()
            df = pd.DataFrame(list(list_data))
            self.tables_dictionary[file.split('.')[0]] = df
        return self.tables_dictionary

    def MergedDataframe(self, user_id):
        self.GetTableDictionary()
        self.GetBlockUsersData()
        # transform the reviews_1 table to df_1 dataframe
        df_1 = self.tables_dictionary[self.files_list[0].split('.')[0]]
        # select reviews which are approved
        df_1_approve = (df_1[df_1['isApprove'] == 'approved'])
        # transform the likes_1 table to df_1 dataframe
        df_2 = self.tables_dictionary[self.files_list[-1].split('.')[0]]
        # rename the column name in reviews_1 table to resourceId as per likes_1 table
        df_1_approve = df_1_approve.rename(columns={"_id": "resourceId"})
        # merge both the dataframes based on common column 'resourceId'
        df_merge = df_1_approve.merge(df_2, how='left', on='resourceId')

        # extract only required columns from the merged dataframe
        self.df_merge_1 = df_merge[['resourceId', 'loc','rating', 'createdAt_x', 'updatedAt_x', 'fromUserId_y', 'categoryId']]
        # longititude extraction from the loc
        longitude = [_['coordinates'][0] for _ in self.df_merge_1['loc']]

        latitude = [_['coordinates'][1] for _ in self.df_merge_1['loc']]
        self.df_merge_1['longitude'] = longitude
        self.df_merge_1['latitude'] = latitude
        self.df_merge_1.drop(labels='loc', inplace=True, axis=1)
        created_dates = ([_.split('T')[0] for _ in self.df_merge_1['createdAt_x']])
        updated_dates = ([_.split('T')[0] for _ in self.df_merge_1['updatedAt_x']])
        self.df_merge_1['created_dates'] = created_dates
        self.df_merge_1['updated_dates'] = updated_dates
        self.df_merge_1['created_dates'] = pd.to_datetime(self.df_merge_1['created_dates'], dayfirst=True)
        self.df_merge_1['updated_dates'] = pd.to_datetime(self.df_merge_1['updated_dates'], dayfirst=True)

        self.df_merge_1.drop(labels=['createdAt_x', 'updatedAt_x'], inplace=True, axis=1)
        if len(user_id) > 0:
            print('INSIDE IF LOOP')
            for val in self.try_dict.values():
                if user_id in val:
                    remove_id = str([v for v in val if v != user_id][-1])
                    print(f'REMOVID: {remove_id}')
                    self.df_merge_1 = self.df_merge_1[~self.df_merge_1.fromUserId_x.str.contains(remove_id)]
            self.df_merge_1.reset_index(inplace=True)
        self.df_merge_1.to_csv('df_merge.csv')
        return self.df_merge_1

    pass
