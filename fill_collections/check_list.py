from pymongo import MongoClient
import pandas as pd
import time
import numpy as np
from math import *

start_time = time.perf_counter()
myclient = MongoClient(host='localhost', port=27017)
mydb = myclient['real_reviews']
reviews = mydb['reviews_2']

reviews_filter = {"isApprove": 'approved', "isDeleted": False}
df_reviews = pd.DataFrame(list(reviews.find(reviews_filter, {'_id': 1, "loc": 1, "title": 1,
                           'createdAt': 1, 'updatedAt': 1, 'fromUserId': 1, 'categoryId': 1})))

combine_title = list(df_reviews["title"])
print(combine_title)
search_text = 'toshiba quality'
search_text = list(search_text.split())

if [s for s in combine_title if all(xs in s for xs in search_text)]:
    print(f'search_text: {search_text}')