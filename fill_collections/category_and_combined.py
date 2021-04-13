import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json
import pandas as pd
from datetime import timedelta
from math import *
import random


class trend_results:
    def __init__(self, files_list):
        self.files_list = files_list
        pass

    def looping_json_files(self):
        list_1 = []
        for files in self.files_list:
            with open(files) as file:
                data = json.load(file)
                list_1.append(data)
        return list_1

    def GetTableDictionary(self):
        myclient = MongoClient(host=None, port=None)
        mydb = myclient['real_reviews']
        list_1 = self.looping_json_files()
        tables_dictionary = {}
        for index, file in enumerate(self.files_list):
            my_collection = mydb[file.split('.')[0]]
            list_data = my_collection.find()
            df = pd.DataFrame(list(list_data))
            tables_dictionary[file.split('.')[0]] = df
        return tables_dictionary

    def MergedDataframe(self):
        self.tables_dictionary = self.GetTableDictionary()
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
        df_merge_1 = df_merge[['resourceId', 'loc', 'createdAt_x', 'updatedAt_x', 'fromUserId_x', 'categoryId']]
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
        df_merge_1['created_dates'] = pd.to_datetime(df_merge_1['created_dates'], dayfirst=True)
        df_merge_1['updated_dates'] = pd.to_datetime(df_merge_1['updated_dates'], dayfirst=True)

        df_merge_1.drop(labels=['createdAt_x', 'updatedAt_x'], inplace=True, axis=1)
        return df_merge_1

    def distance(self, lon1, lat1, lon2, lat2):
        x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
        y = (lat2 - lat1)
        return sqrt(x * x + y * y)

    def SubgroupCategoriesToDictionary(self):
        df_merge_1 = self.MergedDataframe()
        gb = (df_merge_1.groupby('categoryId'))
        cat_dict = {}
        for cat in gb.groups:
            cat_dict[cat] = gb.get_group(cat).reset_index()
        return cat_dict

    def TrendingNearReviews(self):
        df_merge_cat = self.MergedDataframe()
        index_list = list(df_merge_cat.index)
        random_index = random.choice(index_list)
        user_rand, review_rand = str(df_merge_cat.iloc[random_index]['fromUserId_x']), str(
            df_merge_cat.iloc[random_index]['resourceId'])
        long_rand, lat_rand = float(df_merge_cat.iloc[random_index]['longitude']), float(
            df_merge_cat.iloc[random_index]['latitude'])
        dist_list = []
        for index, lat in enumerate(df_merge_cat.loc[:, 'latitude']):
            dist_measured = self.distance(long_rand, lat_rand, df_merge_cat.loc[index, 'longitude'], lat)
            dist_list.append(dist_measured)

        df_merge_cat_1 = df_merge_cat.copy()
        df_merge_cat_1['dist_list'] = dist_list
        df_merge_cat_1 = df_merge_cat_1.sort_values('dist_list')
        groupby_like_count = (df_merge_cat_1.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
            columns={'updated_dates': 'ReviewViewCount'}))
        df_merge_cat_1_count_merge = (df_merge_cat_1.merge(groupby_like_count, on='resourceId'))
        df_merge_cat_1_count_merge = df_merge_cat_1_count_merge.sort_values(by=['ReviewViewCount', 'dist_list'],
                                                                            ascending=[False, True])
        return (df_merge_cat_1_count_merge['resourceId'].unique())

    pass