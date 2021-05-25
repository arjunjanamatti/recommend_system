# import pymongo
from pymongo import MongoClient
import logging
import pandas as pd
from datetime import timedelta
from math import *
from flask import Flask, request
import numpy as np

app = Flask(__name__)

class trend_results:
    def __init__(self):
        self.top_review_last_week = {}
        self.top_user_last_week = {}
        self.popular_review_last_month = {}
        self.popular_user_last_month = {}
        self.myclient = MongoClient(host=None, port=None)
        self.mydb = self.myclient['real_reviews']
        pass

    def MergeDataframeUpdate(self, user_id, search_text, target_userid):
        reviews = self.mydb['reviews_2']
        likes = self.mydb['likes_2']
        blockusers = self.mydb['blockusers']

        ##### Blockusersdata
        cur = blockusers.find({}, {'blockUserId': 1, 'fromUserId': 1})
        block_users_dict_list = [doc for doc in cur]
        try_list = []

        def get_data_block_users(new):
            try_list.extend([new['blockUserId'], new['fromUserId']])
            return try_list

        block_list = list(map(get_data_block_users, block_users_dict_list))
        block_list = [(y) for x in block_list for y in x]

        ##### searchtext part
        reviews_filter = {"isApprove": 'approved', "isDeleted": False,
                          "title": {"$regex": f".*{search_text}.*"}} if search_text != None else {
            "isApprove": 'approved', "isDeleted": False}

        #### blockusers part
        reviews_filter['fromUserId'] = {'$nin': block_list} if (len(user_id) > 0) else reviews_filter

        # extract fields where review is approved and not deleted, also selecting only required fields
        reviews_filter = {"isApprove": 'approved', "isDeleted": False,
                          "title": {"$regex": f".*{search_text}.*"}} if search_text != None else {
            "isApprove": 'approved', "isDeleted": False}

        df_reviews = pd.DataFrame(list(reviews.find(reviews_filter, {'_id': 1, "loc": 1, "title": 1,
                                                                     'createdAt': 1, 'updatedAt': 1, 'fromUserId': 1,
                                                                     'categoryId': 1})))
        # from likes table only review _id and resourceId field
        df_likes = pd.DataFrame(list(likes.find({}, {'_id': 1, 'resourceId': 1})))
        df_reviews.set_index('_id', inplace=True)
        df_likes.set_index('resourceId', inplace=True)
        self.df_merge = df_reviews.join(df_likes, how='left')
        self.df_merge['longitude'] = self.df_merge['loc'].apply(lambda x: x['coordinates'][0])
        self.df_merge['latitude'] = self.df_merge['loc'].apply(lambda x: x['coordinates'][1])
        self.df_merge['created_dates'] = self.df_merge['createdAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge['updated_dates'] = self.df_merge['updatedAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge.drop(labels=['createdAt', 'updatedAt', 'loc', "_id"], inplace=True, axis=1)
        self.df_merge['resourceId'] = self.df_merge.index
        self.df_merge.reset_index(drop=True, inplace=True)
        targetuserid_reviewlist = list(reviews.find({'fromUserId': target_userid}, {'_id': 1}))
        self.targetuserid_reviewlist = [reviews['_id'] for reviews in targetuserid_reviewlist]

    def distance(self, lon1, lat1, lon2, lat2):
        x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
        y = (lat2 - lat1)
        return sqrt(x * x + y * y)

    def SubgroupCategoriesToDictionary(self,user_id, search_text, target_userid):
        self.MergeDataframeUpdate(user_id, search_text, target_userid)
        gb = (self.df_merge.groupby('categoryId'))
        self.cat_dict = {}
        for cat in gb.groups:
            self.cat_dict[cat] = gb.get_group(cat).reset_index()
        return self.cat_dict