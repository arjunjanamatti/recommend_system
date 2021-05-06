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

files_list = ['all_data_files/reviews_1.json','all_data_files/likes_1.json']
# main()
# get_data()
#


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
#

# transform the reviews_1 table to df_1 dataframe
df_1 = tables_dictionary['all_data_files/reviews_1']
# select reviews which are approved
df_1_approve = (df_1[df_1['isApprove']=='approved'])
# transform the likes_1 table to df_1 dataframe
df_2 = tables_dictionary['all_data_files/likes_1']
# rename the column name in reviews_1 table to resourceId as per likes_1 table
df_1_approve=df_1_approve.rename(columns={"_id": "resourceId"})
# merge both the dataframes based on common column 'resourceId'
df_merge = df_1_approve.merge(df_2,how='left', on='resourceId')

# extract only required columns from the merged dataframe
df_merge_1 = df_merge[['resourceId', 'loc', 'createdAt_x', 'updatedAt_x','fromUserId_x']]
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
df_merge.to_csv('df_merge.csv')
df_merge_1.to_csv('df_merge_1.csv')

# print(df_merge_1.groupby(['fromUserId_x','resourceId'])['resourceId'].count())
# df.value_counts(subset=['A', 'B'])
print(df_merge_1.groupby(['fromUserId_x'])['resourceId'].count())

print(df_merge_1.value_counts(subset=['fromUserId_x','resourceId']))

### DATAFRAME FOR NEAREST NEIGHBORS FOR REVIEW_ID
def distance(lon1, lat1, lon2, lat2):
    x = (lon2 - lon1) * cos(0.5*(lat2+lat1))
    y = (lat2 - lat1)
    return sqrt( x*x + y*y )

unique_user_list = list(df_merge_1['fromUserId_x'])
unique_review_list = list(df_merge_1['resourceId'])
random_user = random.choice(unique_user_list)
random_review = random.choice(unique_review_list)
df_merge_1_unique = (df_merge_1[df_merge_1.duplicated()].drop_duplicates()).reset_index()
index_num = (df_merge_1_unique.index[df_merge_1_unique.resourceId == random_review])
long_random_review, lat_random_review = float(df_merge_1_unique.iloc[index_num]['longitude']), (df_merge_1_unique.iloc[index_num]['latitude'])
dist_list = []
for index, lat in enumerate(df_merge_1.loc[:,'latitude']):
    dist_measured = distance(long_random_review, lat_random_review, df_merge_1.loc[index,'longitude'], lat)
    dist_list.append(dist_measured)

df_merge_2 = df_merge_1.copy()
df_merge_2['dist_list'] = dist_list
df_merge_2 = df_merge_2.sort_values('dist_list')
df_merge_2_unique = (df_merge_2[df_merge_2.duplicated()].drop_duplicates()).reset_index()
# print(df_merge_2_unique['resourceId'][:10])
groupby_like_count = (df_merge_2.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
df_merge_2_unique_like_count_merge = (df_merge_2_unique.merge(groupby_like_count,on='resourceId'))
df_merge_2_unique_like_count_merge = df_merge_2_unique_like_count_merge.sort_values(by='ReviewViewCount', ascending=False)
print()
print(df_merge_2_unique_like_count_merge)
print()
print(df_merge_2_unique_like_count_merge[df_merge_2_unique_like_count_merge['dist_list'] <= 10][:10])

### DATAFRAME FOR LAST WEEK
today = pd.to_datetime('today').floor('D')
week_prior =  today - timedelta(weeks=1)
df_last_week = df_merge_1[(df_merge_1['updated_dates'] <= today) & (df_merge_1['updated_dates'] >= week_prior)]
df_last_week.to_csv('df_last_week.csv')
top_10_last_week_df = (df_last_week.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
print()
print('LAST WEEK RESULTS')
print(top_10_reviews_last_week['resourceId'])
top_10_last_week_df = (df_last_week.groupby(['fromUserId_x'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
print(top_10_reviews_last_week['fromUserId_x'])

### DATAFRAME FOR LAST MONTH
last_month = today - timedelta(days=30)
df_last_month = df_merge_1[(df_merge_1['updated_dates'] <= today) & (df_merge_1['updated_dates'] >= last_month)]
df_last_month.to_csv('df_last_month.csv')
print()
print('LAST MONTH RESULTS')
# print(df_last_month.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_last_month_df = (df_last_month.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_month = (top_10_last_month_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
print(top_10_reviews_last_month['resourceId'])
top_10_last_month_df = (df_last_month.groupby(['fromUserId_x'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_month = (top_10_last_month_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
print(top_10_reviews_last_month['fromUserId_x'])