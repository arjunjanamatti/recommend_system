import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json
import pandas as pd
from datetime import timedelta

def looping_json_files(files_list):
    list_1 = []
    for files in files_list:
        with open(files) as file:
            data = json.load(file)
            list_1.append(data)
    return list_1


def main():
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

files_list = ['reviews_1.json','likes_1.json']
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

df_1 = tables_dictionary['reviews_1']
df_1_approve = (df_1[df_1['isApprove']=='approved'])
df_2 = tables_dictionary['likes_1']
df_1_approve=df_1_approve.rename(columns={"_id": "resourceId"})
df_merge = df_1_approve.merge(df_2,how='left', on='resourceId')
print(df_merge.columns)
df_merge_1 = df_merge[['resourceId', 'loc', 'createdAt_x', 'updatedAt_x','fromUserId_x']]
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


### DATAFRAME FOR LAST WEEK
today = pd.to_datetime('today').floor('D')
week_prior =  today - timedelta(weeks=1)
df_last_week = df_merge_1[(df_merge_1['updated_dates'] <= today) & (df_merge_1['updated_dates'] >= week_prior)]
df_last_week.to_csv('df_last_week.csv')
top_10_last_week_df = (df_last_week.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
print(top_10_reviews_last_week['resourceId'])
top_10_last_week_df = (df_last_week.groupby(['fromUserId_x'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewViewCount'], ascending=False))[:10]
print(top_10_reviews_last_week['fromUserId_x'])
### DATAFRAME FOR LAST MONTH
last_month = today - timedelta(days=30)
df_last_month = df_merge_1[(df_merge_1['updated_dates'] <= today) & (df_merge_1['updated_dates'] >= last_month)]
df_last_month.to_csv('df_last_month.csv')
print()
# print(df_last_month.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(columns = {'updated_dates': 'ReviewViewCount'}))
