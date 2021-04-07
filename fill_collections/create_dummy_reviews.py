import json
import random
import time
from datetime import datetime

def randomDate(start, end):
    frmt = '%d-%m-%Y %H:%M:%S'

    stime = time.mktime(time.strptime(start, frmt))
    etime = time.mktime(time.strptime(end, frmt))

    ptime = stime + random.random() * (etime - stime)
    dt = datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    return dt


nested_dict = []
sample_dict = {'_id': '604cf485c4e5fa0b7f799386', 'loc': {'coordinates': [-90.1985, 38.6364], 'type': 'Point'}, 'tagIds': ['60144d615f8f003b9916242e'], 'image': [], 'isDeleted': False, 'isTrending': True, 'isFeature': False, 'isApprove': 'approved', 'rating': 2, 'socialMedia': [], 'title': 'Push Notification', 'fromUserId': '5fdf6bbcfe08e8c0191a7805', 'slug': 'push-notification-1615908828722', 'pros': [{'_id': '6051edcc184491b6b0c5e027', 'id': 1614620859396, 'text': 'sdfasdfasdf'}], 'cons': [{'_id': '6051edcc184491b6b0c5e028', 'id': 1614620864371, 'text': 'sdfasdfasdfasdfasdfasdfasdfadsf'}], 'createdAt': '2021-03-13T17:21:09.436Z', 'updatedAt': '2021-03-30T04:46:57.035Z', '__v': 0, 'video': 'review-1614620841111.mp4', 'videoUrl': 'http://staging.realreviews.org/api/review/video/stream/604cf485c4e5fa0b7f699386', 'approveBy': '5fc7c1e7a3fb189c35bb8945', 'categoryId': '602cb70978d3fda29f330606', 'description': 'Test Push Notification', 'coverImage': 'cover_image_review-1614620841111_1.jpg', 'coverImageUrl': 'http://staging.realreviews.org/api/review/image/stream/604cf485c4e5fa0b7f699386/cover_image_review-1614620841111_1.jpg', 'priceRange': 1, 'reason': 'test'}
for i in range(100):
    sample_dict['_id'] = sample_dict['_id'][:19]+str(799386+i)
    sample_dict['loc']['coordinates'][0] = sample_dict['loc']['coordinates'][0] + random.uniform(0.1, 0.2)
    sample_dict['loc']['coordinates'][1] = sample_dict['loc']['coordinates'][1] + random.uniform(0.1, 0.2)
    sample_dict['rating'] = random.uniform(1,10)
    sample_dict['createdAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    sample_dict['updatedAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    nested_dict.append(sample_dict)
    print(i,sample_dict['_id'], len(sample_dict['_id']))


with open("reviews_1.json", "w") as fp:
    json.dump(nested_dict , fp)

with open("reviews_1.json") as fp:
    data = json.load(fp)

for dic in data:
    print(dic)