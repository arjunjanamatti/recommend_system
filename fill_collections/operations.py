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

def distance(lon1, lat1, lon2, lat2):
    x = (lon2 - lon1) * cos(0.5*(lat2+lat1))
    y = (lat2 - lat1)
    return sqrt( x*x + y*y )

def NearestNeighborReviewID(files_list):
    df_merge_1 = MergedDataframe(files_list)
    # get the list of unique review_id
    unique_review_list = list(df_merge_1['resourceId'])
    # select a random review_id from the unique review_id list
    random_review = random.choice(unique_review_list)
    # will drop all the duplicates and create a dataframe
    df_merge_1_unique = (df_merge_1[df_merge_1.duplicated()].drop_duplicates()).reset_index()
    # get the index number of the random review_id
    index_num = (df_merge_1_unique.index[df_merge_1_unique.resourceId == random_review])
    # get the latitude and longitude of the random review_id
    long_random_review, lat_random_review = float(df_merge_1_unique.iloc[index_num]['longitude']), (
    df_merge_1_unique.iloc[index_num]['latitude'])
    dist_list = []
    # append the distance of each review with respect to the random review_id
    for index, lat in enumerate(df_merge_1.loc[:, 'latitude']):
        dist_measured = distance(long_random_review, lat_random_review, df_merge_1.loc[index, 'longitude'], lat)
        dist_list.append(dist_measured)
    # create a copy of df_merge_1
    df_merge_2 = df_merge_1.copy()
    # add a new column for the distance
    df_merge_2['dist_list'] = dist_list
    # sort the dataframe based on the column = 'dist_list'
    df_merge_2 = df_merge_2.sort_values('dist_list')
    # get only the duplicates of the dataframe
    df_merge_2_unique = (df_merge_2[df_merge_2.duplicated()].drop_duplicates()).reset_index()
    # get the like counts by using the groupby
    groupby_like_count = (df_merge_2.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
        columns={'updated_dates': 'ReviewViewCount'}))
    # merge the dataframe of unique and like_count
    df_merge_2_unique_like_count_merge = (df_merge_2_unique.merge(groupby_like_count, on='resourceId'))
    # sort the merged dataframe in descending order based on 'like_count'
    df_merge_2_unique_like_count_merge = df_merge_2_unique_like_count_merge.sort_values(by='ReviewViewCount',
                                                                                        ascending=False)
    return list(df_merge_2_unique_like_count_merge[df_merge_2_unique_like_count_merge['dist_list'] <= 10]['resourceId'][:10])

def WeeklyResults(files_list, week_num = 1):
    ### DATAFRAME FOR LAST WEEK
    df_merge_1 = MergedDataframe(files_list)
    today = pd.to_datetime('today').floor('D')
    week_prior = today - timedelta(weeks=week_num)
    df_last_week = df_merge_1[(df_merge_1['updated_dates'] <= today) & (df_merge_1['updated_dates'] >= week_prior)]
    df_last_week.to_csv('df_last_week.csv')
    top_10_last_week_df = (df_last_week.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
        columns={'updated_dates': 'ReviewViewCount'}))
    top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
    print()
    print('LAST WEEK RESULTS')
    trending_reviews = list(top_10_reviews_last_week['resourceId'])
    top_10_last_week_df = (df_last_week.groupby(['fromUserId_x'])['updated_dates'].count().reset_index().rename(
        columns={'updated_dates': 'ReviewViewCount'}))
    top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
    trending_users = list(top_10_reviews_last_week['fromUserId_x'])
    while len(trending_reviews) < 10:
        week_num += 1
        week_prior = today - timedelta(weeks=week_num)
        df_last_week = df_merge_1[(df_merge_1['updated_dates'] <= today) & (df_merge_1['updated_dates'] >= week_prior)]
        df_last_week.to_csv('df_last_week.csv')
        top_10_last_week_df = (df_last_week.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
            columns={'updated_dates': 'ReviewViewCount'}))
        top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
        print()
        print('LAST WEEK RESULTS', week_num)
        trending_reviews = list(top_10_reviews_last_week['resourceId'])
        print('Length of trending reviews: ', len(trending_reviews))
    week_num = 1
    while  len(trending_users) < 10:
        week_num += 1
        week_prior = today - timedelta(weeks=week_num)
        df_last_week = df_merge_1[(df_merge_1['updated_dates'] <= today) & (df_merge_1['updated_dates'] >= week_prior)]
        df_last_week.to_csv('df_last_week.csv')
        top_10_last_week_df = (df_last_week.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
            columns={'updated_dates': 'ReviewViewCount'}))
        top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
        print()
        print('LAST WEEK RESULTS')
        trending_reviews = list(top_10_reviews_last_week['resourceId'])
    return trending_reviews, trending_users

    pass

# print(NearestNeighborReviewID(files_list))
print(WeeklyResults(files_list)[0])