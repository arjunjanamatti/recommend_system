import json
import random
import time
from datetime import datetime
import pickle
from pymongo import MongoClient
import pandas as pd
from pymongo.errors import ConnectionFailure

with open('resourceId.pickle', 'rb') as f:
    resourceId = pickle.load(f)

nested_dict = []
sample_dict =   {
    "_id": "605e017098305a7a376ccd24",
    "resourceId": "604cf485c4e5fa0b7f699386",
    "resourceType": "REVIEW",
    "fromUserId": "5fdf6bbcfe08e8c0191a7805",
    "createdAt": "2021-03-26T15:44:48.209Z",
    "updatedAt": "2021-03-26T15:44:48.209Z",
    "__v": 0
  }

with open('resourceId.pickle', 'rb') as f:
    mynewlist = pickle.load(f)
print(mynewlist)

def randomDate(start, end):
    frmt = '%d-%m-%Y %H:%M:%S'

    stime = time.mktime(time.strptime(start, frmt))
    etime = time.mktime(time.strptime(end, frmt))

    ptime = stime + random.random() * (etime - stime)
    dt = datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    return dt

for i in range(100000):
    new_dict = sample_dict.copy()
    new_dict['_id'] = new_dict['_id'][:23]+str(24+i)
    new_dict['resourceId'] = random.choice(resourceId)
    new_dict['fromUserId'] = random.choice(mynewlist)
    new_dict['createdAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    new_dict['updatedAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    # new_dict = sample_dict.copy()
    nested_dict.append(new_dict)

with open("likes_2.json", "w") as fp:
    json.dump(nested_dict , fp, indent=4)

df = pd.read_json("likes_2.json")
print(df['resourceId'].value_counts())

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

files_list = ['likes_2.json']
SendJsonFilesToMongoDB(files_list)