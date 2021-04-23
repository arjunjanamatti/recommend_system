import json
import random
import time
from datetime import datetime
import pickle

def randomDate(start, end):
    frmt = '%d-%m-%Y %H:%M:%S'

    stime = time.mktime(time.strptime(start, frmt))
    etime = time.mktime(time.strptime(end, frmt))

    ptime = stime + random.random() * (etime - stime)
    dt = datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    return dt


nested_dict = []
sample_dict = {'_id': '604cf485c4e5fa0b7f799386', 'loc': {'coordinates': [-90.1985, 38.6364], 'type': 'Point'}, 'tagIds': ['60144d615f8f003b9916242e'], 'image': [], 'isDeleted': False, 'isTrending': True, 'isFeature': False, 'isApprove': 'approved', 'rating': 2, 'socialMedia': [], 'title': 'Push Notification', 'fromUserId': '5fdf6bbcfe08e8c0191a7805', 'slug': 'push-notification-1615908828722', 'pros': [{'_id': '6051edcc184491b6b0c5e027', 'id': 1614620859396, 'text': 'sdfasdfasdf'}], 'cons': [{'_id': '6051edcc184491b6b0c5e028', 'id': 1614620864371, 'text': 'sdfasdfasdfasdfasdfasdfasdfadsf'}], 'createdAt': '2021-03-13T17:21:09.436Z', 'updatedAt': '2021-03-30T04:46:57.035Z', '__v': 0, 'video': 'review-1614620841111.mp4', 'videoUrl': 'http://staging.realreviews.org/api/review/video/stream/604cf485c4e5fa0b7f699386', 'approveBy': '5fc7c1e7a3fb189c35bb8945', 'categoryId': '602cb70978d3fda29f330606', 'description': 'Test Push Notification', 'coverImage': 'cover_image_review-1614620841111_1.jpg', 'coverImageUrl': 'http://staging.realreviews.org/api/review/image/stream/604cf485c4e5fa0b7f699386/cover_image_review-1614620841111_1.jpg', 'priceRange': 1, 'reason': 'test'}
cordinates_nested = []
for i in range(900):
    sample_dict['loc']['coordinates'][0] = sample_dict['loc']['coordinates'][0] + random.uniform(0.1, 1.2)
    sample_dict['loc']['coordinates'][1] = sample_dict['loc']['coordinates'][1] + random.uniform(0.1, 1.2)
    cordinates_nested.append([sample_dict['loc']['coordinates'][0],sample_dict['loc']['coordinates'][1]])

with open('categories.json') as file:
    data = json.load(file)
categores_id_list = []
for id in data:
    categores_id_list.append(id['_id'])
print(len(categores_id_list))



user_id_list = []
for i in range(30):
    user_id = '5fdf6bbcfe08e8c0191a7805'
    user_id_list.append(user_id[:20]+str(7805+i))
    # new_dict = sample_dict.copy()
    # # 'fromUserId': '5fdf6bbcfe08e8c0191a7805'
    # new_dict['_id'] = new_dict['_id'][:19]+str(799386+i)
    # new_dict['loc'] = {'coordinates':cordinates_nested[i], 'type': 'Point'}
    # new_dict['rating'] = random.uniform(1,10)
    # new_dict['createdAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    # new_dict['updatedAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    # # new_dict = sample_dict.copy()
    # nested_dict.append(new_dict)

for i in range(900):
    new_dict = sample_dict.copy()
    # 'fromUserId': '5fdf6bbcfe08e8c0191a7805'
    new_dict['_id'] = new_dict['_id'][:19]+str(799386+i)
    new_dict['loc'] = {'coordinates':cordinates_nested[i], 'type': 'Point'}
    new_dict['rating'] = random.uniform(1,10)
    new_dict['categoryId'] =  random.choice(categores_id_list)
    new_dict['fromUserId'] = random.choice(user_id_list)
    new_dict['createdAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    new_dict['updatedAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    # new_dict = sample_dict.copy()
    nested_dict.append(new_dict)



with open("reviews_2.json", "w") as fp:
    json.dump(nested_dict , fp, indent=4)

user_id = []
with open("reviews_2.json") as file:
    data = json.load(file)
for dic in data:
    user_id.append(dic['_id'])

with open('resourceId.pickle', 'wb') as us:
    pickle.dump(user_id, us)

with open('resourceId.pickle', 'rb') as f:
    mynewlist = pickle.load(f)
print(mynewlist)

from fill_collection import *

files_list = ['reviews_2.json']
SendJsonFilesToMongoDB(files_list)
