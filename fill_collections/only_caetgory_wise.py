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

try:
    files_list = ['reviews_1.json','likes_1.json']
    SendJsonFilesToMongoDB(files_list)
except Exception as e:
    print(e)
    pass

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
    df_merge_1 = df_merge[['resourceId', 'loc', 'createdAt_x', 'updatedAt_x','fromUserId_x', 'categoryId']]
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

def SubgroupCategoriesToDictionary(files_list):
    df_merge_1 = MergedDataframe(files_list)
    gb = (df_merge_1.groupby('categoryId'))
    cat_dict = {}
    # print([gb.get_group(x).reset_index() for x in gb.groups])
    # print([cat_dict[x] = gb.get_group(x).reset_index()  for x in gb.groups])
    for cat in gb.groups:
        cat_dict[cat] = gb.get_group(cat).reset_index()
    return cat_dict

def TrendingNearReviews(df_merge_cat):
    index_list = list(df_merge_cat.index)
    random_index = random.choice(index_list)
    user_rand, review_rand = str(df_merge_cat.iloc[random_index]['fromUserId_x']), str(
        df_merge_cat.iloc[random_index]['resourceId'])
    long_rand, lat_rand = float(df_merge_cat.iloc[random_index]['longitude']), float(
        df_merge_cat.iloc[random_index]['latitude'])
    dist_list = []
    for index, lat in enumerate(df_merge_cat.loc[:, 'latitude']):
        dist_measured = distance(long_rand, lat_rand, df_merge_cat.loc[index, 'longitude'], lat)
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
df_1 = tables_dictionary['reviews_1']
# select reviews which are approved
df_1_approve = (df_1[df_1['isApprove']=='approved'])
# transform the likes_1 table to df_1 dataframe
df_2 = tables_dictionary['likes_1']
# rename the column name in reviews_1 table to resourceId as per likes_1 table
df_1_approve=df_1_approve.rename(columns={"_id": "resourceId"})
# merge both the dataframes based on common column 'resourceId'
df_merge = df_1_approve.merge(df_2,how='left', on='resourceId')

# extract only required columns from the merged dataframe
df_merge_1 = df_merge[['resourceId', 'loc', 'createdAt_x', 'updatedAt_x','fromUserId_x', 'categoryId']]
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

cat_uniq_list = list(df_merge_1['categoryId'].unique())

gb = (df_merge_1.groupby('categoryId'))
cat_dict = {}
# print([gb.get_group(x).reset_index() for x in gb.groups])
# print([cat_dict[x] = gb.get_group(x).reset_index()  for x in gb.groups])
for cat in gb.groups:
    cat_dict[cat] = gb.get_group(cat).reset_index()
# print(cat_dict)

# for keys in cat_dict.keys():
#     print(keys)
df_merge_cat = cat_dict['602cb70978d3fda29f330606']
# print(df_merge_1)
#
# print(df_merge_1.groupby(['fromUserId_x'])['resourceId'].count())
#
# print(df_merge_1.value_counts(subset=['fromUserId_x','resourceId']))

### DATAFRAME FOR NEAREST NEIGHBORS FOR REVIEW_ID
def distance(lon1, lat1, lon2, lat2):
    x = (lon2 - lon1) * cos(0.5*(lat2+lat1))
    y = (lat2 - lat1)
    return sqrt( x*x + y*y )

# Number of views =
# likes
# dislikes
# comments
# shares
index_list = list(df_merge_cat.index)
random_index = random.choice(index_list)
user_rand, review_rand = str(df_merge_cat.iloc[random_index]['fromUserId_x']), str(df_merge_cat.iloc[random_index]['resourceId'])
long_rand, lat_rand = float(df_merge_cat.iloc[random_index]['longitude']), float(df_merge_cat.iloc[random_index]['latitude'])
dist_list = []
for index, lat in enumerate(df_merge_cat.loc[:,'latitude']):
    dist_measured = distance(long_rand, lat_rand, df_merge_cat.loc[index,'longitude'], lat)
    dist_list.append(dist_measured)
print(dist_list)

df_merge_cat_1 = df_merge_cat.copy()
df_merge_cat_1['dist_list'] = dist_list
df_merge_cat_1 = df_merge_cat_1.sort_values('dist_list')
groupby_like_count = (df_merge_cat_1.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
print(groupby_like_count)
df_merge_cat_1_count_merge = (df_merge_cat_1.merge(groupby_like_count,on='resourceId'))
df_merge_cat_1_count_merge = df_merge_cat_1_count_merge.sort_values(by=['ReviewViewCount', 'dist_list'], ascending=[False, True])
print(df_merge_cat_1_count_merge)
print(df_merge_cat_1_count_merge['resourceId'].unique())


### DATAFRAME FOR LAST WEEK
today = pd.to_datetime('today').floor('D')
week_num = 1
week_prior =  today - timedelta(weeks = week_num)
df_last_week = df_merge_cat[(df_merge_cat['updated_dates'] <= today) & (df_merge_cat['updated_dates'] >= week_prior)]
df_last_week.to_csv('df_last_week.csv')
top_10_last_week_df = (df_last_week.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
print()
print('LAST WEEK RESULTS')
print(df_merge_cat['updated_dates'].min())
print(week_prior)
while (len(top_10_reviews_last_week['resourceId'].unique()) < 10):
    week_num += 1
    # print(week_num)/
    week_prior = today - timedelta(weeks=week_num)
    if (week_prior < df_merge_cat['updated_dates'].min()):
        break
    df_last_week = df_merge_cat[
        (df_merge_cat['updated_dates'] <= today) & (df_merge_cat['updated_dates'] >= week_prior)]
    top_10_last_week_df = (df_last_week.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
        columns={'updated_dates': 'ReviewViewCount'}))
    top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))

#
print(top_10_reviews_last_week['resourceId'].unique())

week_num = 1
top_10_last_week_df = (df_last_week.groupby(['fromUserId_x'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
while (len(top_10_reviews_last_week['fromUserId_x'].unique()) < 10):
    week_num += 1
    # print(week_num)
    week_prior = today - timedelta(weeks=week_num)
    if (week_prior < df_merge_cat['updated_dates'].min()):
        break
    df_last_week = df_merge_cat[
        (df_merge_cat['updated_dates'] <= today) & (df_merge_cat['updated_dates'] >= week_prior)]
    top_10_last_week_df = (df_last_week.groupby(['fromUserId_x'])['updated_dates'].count().reset_index().rename(
        columns={'updated_dates': 'ReviewViewCount'}))
    top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))

print(top_10_reviews_last_week['fromUserId_x'].unique())

### DATAFRAME FOR LAST MONTH
last_month = today - timedelta(days=30)
df_last_month = df_merge_cat[(df_merge_cat['updated_dates'] <= today) & (df_merge_cat['updated_dates'] >= last_month)]
df_last_month.to_csv('df_last_month.csv')
print()
print('LAST MONTH RESULTS')
# print(df_last_month.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_last_month_df = (df_last_month.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_month = (top_10_last_month_df.sort_values(['ReviewViewCount'], ascending=False))
print(top_10_reviews_last_month['resourceId'].unique())
top_10_last_month_df = (df_last_month.groupby(['fromUserId_x'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_month = (top_10_last_month_df.sort_values(['ReviewViewCount'], ascending=False))
print(top_10_reviews_last_month['fromUserId_x'].unique())