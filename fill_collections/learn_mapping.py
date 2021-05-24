# num_list = list(range(11,20))
#
# def square(num):
#     return num**2
#
# square_num = map(square, num_list)
#
# print(list(square_num))

from pymongo import MongoClient

myclient = MongoClient(host=None, port=None)
mydb = myclient['real_reviews']
coll = mydb['blockusers']
cur = coll.find()
block_users_dict_list = [doc for doc in cur]

try_dict = {}
for new in block_users_dict_list:
    try_dict[new['_id']] = []
    try_dict[new['_id']].append(new['blockUserId'])
    try_dict[new['_id']].append(new['fromUserId'])