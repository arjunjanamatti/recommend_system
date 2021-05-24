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
def get_data_block_users(new):
    try_dict[new['_id']] = []
    try_dict[new['_id']].append(new['blockUserId'])
    try_dict[new['_id']].append(new['fromUserId'])
    return try_dict

print(block_users_dict_list)
print()
print([[value.items()] for value in block_users_dict_list])
# try_dict = {k: f(v) for k, v in my_dictionary.items()}

try_dict_1 = {}
for new in block_users_dict_list:
    try_dict_1[new['_id']] = []
    try_dict_1[new['_id']].append(new['blockUserId'])
    try_dict_1[new['_id']].append(new['fromUserId'])

print(try_dict_1)