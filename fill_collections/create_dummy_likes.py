import json
import random
import time
from datetime import datetime
import pickle
from create_dummy_reviews import randomDate
from pymongo import MongoClient
import pandas as pd

with open('user_id.pickle', 'rb') as f:
    user_id = pickle.load(f)

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

for i in range(10000):
    new_dict = sample_dict.copy()
    new_dict['_id'] = new_dict['_id'][:23]+str(24+i)
    new_dict['resourceId'] = random.choice(user_id)
    new_dict['createdAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    new_dict['updatedAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    # new_dict = sample_dict.copy()
    nested_dict.append(new_dict)

with open("likes_1.json", "w") as fp:
    json.dump(nested_dict , fp, indent=4)

df = pd.read_json("likes_1.json")
print(df['resourceId'].value_counts())
