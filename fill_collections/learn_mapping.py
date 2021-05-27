from pymongo import MongoClient
import pandas as pd
import time
import numpy as np
from math import *
import logging, os

try:
    os.makedirs('LearnMappingLogs')
except:
    pass
logname = 'LearnMappingLogs/loging.log'
logging.basicConfig(filename=logname,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

logger = logging.getLogger('urbanGUI')

start_time = time.perf_counter()
myclient = MongoClient(host='localhost', port=27017)
mydb = myclient['real_reviews']
blockusers = mydb['blockusers']
reviews = mydb['reviews_2']
likes = mydb['likes_2']

##### blockusers part
user_id = '5fdf6bbcfe08e8c0191a7805'
# if reviews.find({'fromUserId': f'{user_id}'}):
#     print(f'{user_id} is in the database')
user_id_list = [rev['fromUserId'] for rev in list(reviews.find({}, {'fromUserId': 1}))]
# print(user_id_list)
if user_id in user_id_list:
    logging.info(f'{user_id} is in the database')
else:
    logging.info(f'{user_id} is NOT in the database')



cur = blockusers.find({}, {'blockUserId': 1, 'fromUserId': 1})
block_users_dict_list = [doc for doc in cur]
block_list = [([new['blockUserId'], new['fromUserId']]) for new in block_users_dict_list]


# print(block_users_dict_list)
try_list = []
# def get_data_block_users(new):
#     # try_dict[new['_id']] = []
#     # try_dict[new['_id']].append(new['blockUserId'])
#     # try_dict[new['_id']].append(new['fromUserId'])
#     try_list.extend([new['blockUserId'], new['fromUserId']])
#     # try_list.append(new['blockUserId'])
#     # try_list.append(new['fromUserId'])
#     return try_list

reult_list = [([new['blockUserId'], new['fromUserId']]) for new in block_users_dict_list]
# reult_list = list(map(get_data_block_users, block_users_dict_list))
# print(reult_list)
reult_list = [(y) for x in reult_list for y in x]
# user_id_lists = []
# [(user_id_lists.extend(id)) for id in try_dict.values()
#  if id in try_dict.values()]
# # user_id_lists = [(y) for x in user_id_lists for y in x]
# print(user_id_lists)
#### searchtext part
# search_text = 'toshiba'
search_text = None
if search_text != None:
    search_text = list(search_text.split())
# # search_text = None
# title_list = [rev['title'] for rev in list(reviews.find({}, {'title': 1}))]
# print(f'search_text: {search_text}')
# print(f'title_list: {title_list}')
reviews_filter = {"isApprove": 'approved', "isDeleted": False}
# reviews_filter = {"isApprove": 'approved', "isDeleted": False, "title": {"$regex": f".*{search_text}.*"}} if search_text!=None else {"isApprove": 'approved', "isDeleted": False}

#### blockusers part
# print(reult_list)
if len(user_id) > 0:
    reviews_filter['fromUserId'] = {'$nin': reult_list}

##### categoryid part
category_id = '602cba1878d3fda29f33060a'
# category_id = ''
if len(category_id) > 0:
    reviews_filter['categoryId'] = f'{category_id}'

print(reviews_filter)
df_reviews = pd.DataFrame(list(reviews.find(reviews_filter, {'_id': 1, "loc": 1, "title": 1,
                           'createdAt': 1, 'updatedAt': 1, 'fromUserId': 1, 'categoryId': f'{category_id}'})))
print(df_reviews.columns)
if search_text != None:
    print(f'INSIDE IF SEARCHTEXT LOOP: {search_text}')
    combine_title = ' '.join(df_reviews["title"])
    if [text for text in search_text if text in combine_title]:
        contains = [df_reviews['title'].str.contains(i) for i in search_text]
        df_reviews = df_reviews[np.all(contains, axis=0)]
df_likes = pd.DataFrame(list(likes.find({}, {'_id':1, 'resourceId':1})))
df_reviews.set_index('_id', inplace=True)
df_likes.set_index('resourceId', inplace=True)
df_merge = df_reviews.join(df_likes, how='left')
df_merge['longitude'] = df_merge['loc'].apply(lambda x: x['coordinates'][0])
df_merge['latitude'] = df_merge['loc'].apply(lambda x: x['coordinates'][1])
df_merge['created_dates'] = df_merge['createdAt'].apply(lambda x: str(x.split('T')[0]))
df_merge['updated_dates'] = df_merge['updatedAt'].apply(lambda x: str(x.split('T')[0]))
df_merge.drop(labels=['createdAt', 'updatedAt', 'loc', "_id"], inplace=True, axis=1)
df_merge['resourceId'] = df_merge.index
df_merge.reset_index(drop=True, inplace=True)

##### nearlocation part
def distance(lon1, lat1, lon2, lat2):
    x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
    y = (lat2 - lat1)
    return sqrt(x * x + y * y)

def f(lon1, lat1, x):
    return distance(lon1, lat1, x['longitude'], x['latitude'])
lon1, lat1 = -91.5, 35.0
# print(df_merge.head())
# print(distance(lon1, lat1, df_merge['longitude'][1], df_merge['latitude'][1]))
# print(df_merge.apply(lambda x: f(lon1, lat1, x), axis = 1))
df_merge['distance'] = df_merge.apply(lambda x: distance(lon1, lat1, x['longitude'], x['latitude']), axis=1)


##### targetuserid part
targetuserid = '5fdf6bbcfe08e8c0191a7830'
targetuserid_reviewlist = list(reviews.find({'fromUserId': targetuserid}, {'_id': 1}))
targetuserid_reviewlist = [reviews['_id'] for reviews in targetuserid_reviewlist]

# ##### grouping
# gb = (df_merge.groupby('categoryId'))
# cat_dict = dict((idx, gp) for idx, gp in gb)
# print(f'cat_dict: {cat_dict}')

end_time = time.perf_counter()
total_time = end_time - start_time
logging.info(f'Total time: {total_time} seconds')



df_merge.to_csv('optimize_df_merge.csv')