import json
import random
import time
from datetime import datetime
import pickle
from pymongo import MongoClient
import pandas as pd

with open('resourceId.pickle', 'rb') as f:
    resourceId = pickle.load(f)

nested_dict = []
sample_dict =   {
    "_id": "6086ff261f6ee05e62e109c9",
    "blockUserId": "5fe0d004fe08e8c0191a7806",
    "fromUserId": "5fdf6bbcfe08e8c0191a7805",
    "createdAt": "2021-04-26T17:57:58.192Z",
    "updatedAt": "2021-04-26T17:57:58.192Z",
    "__v": 0
}

new_dict = [{
    "_id": "6086ff261f6ee05e62e109c9",
    "blockUserId": "5fe0d004fe08e8c0191a7806",
    "fromUserId": "5fdf6bbcfe08e8c0191a7805",
    "createdAt": "2021-04-26T17:57:58.192Z",
    "updatedAt": "2021-04-26T17:57:58.192Z",
    "__v": 0
},
    {
        "_id": "6086ff261f6ee05e62e109c10",
        "blockUserId": "5fe0d004fe08e8c0191a7816",
        "fromUserId": "5fdf6bbcfe08e8c0191a7815",
        "createdAt": "2021-04-26T17:57:58.192Z",
        "updatedAt": "2021-04-26T17:57:58.192Z",
        "__v": 0
    },
    {
        "_id": "6086ff261f6ee05e62e109c10",
        "blockUserId": "5fe0d004fe08e8c0191a7816",
        "fromUserId": "5fdf6bbcfe08e8c0191a7817",
        "createdAt": "2021-04-26T17:57:58.192Z",
        "updatedAt": "2021-04-26T17:57:58.192Z",
        "__v": 0
    }
]
try_dict = {}
for new in new_dict:
    if new['_id'] not in try_dict.keys():
        try_dict[new['_id']] = []
        try_dict[new['_id']].append(new['blockUserId'])
        try_dict[new['_id']].append(new['fromUserId'])
    else:
        try_dict[new['_id']].append(new['blockUserId'])
        try_dict[new['_id']].append(new['fromUserId'])

print(try_dict)

blocked_id_list = []
for val in try_dict.values():
    if "5fdf6bbcfe08e8c0191a7805" in val:
        print(val)

    print([blocked_id_list.append(x) for x in val if x == '5fdf6bbcfe08e8c0191a7805'])

print(blocked_id_list)