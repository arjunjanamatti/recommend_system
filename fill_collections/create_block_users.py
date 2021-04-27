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