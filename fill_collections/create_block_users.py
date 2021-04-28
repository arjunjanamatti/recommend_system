import json
import random
import time
from datetime import datetime
import pickle
from pymongo import MongoClient
import pandas as pd

### SEND A SAMPLE DICTIONARY TO MONGODB
# with open('resourceId.pickle', 'rb') as f:
#     resourceId = pickle.load(f)
#
# nested_dict = []
# sample_dict =   [
#     {"_id": "6086ff261f6ee05e62e109c9",
#     "blockUserId": "5fdf6bbcfe08e8c0191a7814",
#     "fromUserId": "5fdf6bbcfe08e8c0191a7805",
#     "createdAt": "2021-04-26T17:57:58.192Z",
#     "updatedAt": "2021-04-26T17:57:58.192Z",
#     "__v": 0
# }]
#
# try_dict = {}
# for new in sample_dict:
#     if new['_id'] not in try_dict.keys():
#         try_dict[new['_id']] = []
#         try_dict[new['_id']].append(new['blockUserId'])
#         try_dict[new['_id']].append(new['fromUserId'])
#     else:
#         try_dict[new['_id']].append(new['blockUserId'])
#         try_dict[new['_id']].append(new['fromUserId'])
#
# print(try_dict)
#
# blocked_id_list = []
# for val in try_dict.values():
#     if "5fdf6bbcfe08e8c0191a7805" in val:
#         a = [blocked_id_list.append(x) for x in val]
#
# print(blocked_id_list)
# with open("blockusers.json", "w") as fp:
#     json.dump(sample_dict , fp, indent=4)
#
# from fill_collection import *
#
# files_list = ['blockusers.json']
# SendJsonFilesToMongoDB(files_list)

