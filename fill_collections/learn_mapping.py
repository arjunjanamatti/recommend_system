# # # num_list = list(range(11,20))
# # #
# # # def square(num):
# # #     return num**2
# # #
# # # square_num = map(square, num_list)
# # #
# # # print(list(square_num))
# #
# # from pymongo import MongoClient
# #
# # myclient = MongoClient(host=None, port=None)
# # mydb = myclient['real_reviews']
# # coll = mydb['blockusers']
# # cur = coll.find()
# # block_users_dict_list = [doc for doc in cur]
# #
# # try_dict = {}
# # def get_data_block_users(new):
# #     try_dict[new['_id']] = []
# #     try_dict[new['_id']].append(new['blockUserId'])
# #     try_dict[new['_id']].append(new['fromUserId'])
# #     return try_dict
# #
# # print(block_users_dict_list)
# # print()
# # # # {key: value for (key, value) in iterable}
# # # print([{k:v for k,v in value.items()} for value in block_users_dict_list])
# # # print([{(try_dict[value['_id']].append(value['blockUserId'])) for k,v in value.items()} for value in block_users_dict_list])
# # # # try_dict = {k: f(v) for k, v in my_dictionary.items()}
# # import time
# #
# # start_time = time.perf_counter()
# #
# # reult_list = list(map(get_data_block_users, block_users_dict_list))
# # print(reult_list[0])
# # end_time = time.perf_counter()
# # total_time_map = end_time-start_time
# # print(f'total_time = {total_time_map} seconds')
# #
# # import time
# #
# # start_time = time.perf_counter()
# #
# # try_dict_1 = {}
# # for new in block_users_dict_list:
# #     try_dict_1[new['_id']] = []
# #     try_dict_1[new['_id']].append(new['blockUserId'])
# #     try_dict_1[new['_id']].append(new['fromUserId'])
# #
# # end_time = time.perf_counter()
# # total_time_for = end_time-start_time
# # print(f'total_time = {total_time_for} seconds')
# # print(f'{total_time_map - total_time_for}')
# # print(try_dict_1)
#
# from pymongo import MongoClient
# # import os
# # os.environ["MODIN_ENGINE"] = "dask"
# # import modin.pandas as pd
# # from distributed import Client
# # client = Client(n_workers=6)
# # # Modin will connect to the Dask Client
# # import modin.pandas as pd
# import dask
# import dask.dataframe as dd
# import pandas as pd
# import time
#
# myclient = MongoClient(host='localhost', port=27017)
# mydb = myclient['real_reviews']
# # list_1 = self.looping_json_files()
# # self.files_list = ['reviews.json', 'likes.json']
# files_list = ['reviews_2.json', 'likes_2.json']
# tables_dictionary = {}
#
# start_time_for = time.perf_counter()
# for index, file in enumerate(files_list):
#     my_collection = mydb[file.split('.')[0]]
#     list_data = my_collection.find()
#     df = pd.DataFrame(list(list_data))
#     df = dd.from_pandas(df,npartitions=6)
#     tables_dictionary[file.split('.')[0]] = df
#
# # print(tables_dictionary)
# end_time_for = time.perf_counter()
# total_time_for = end_time_for - start_time_for
# print(f'Total time for loop: {total_time_for}')
# tables_dictionary_1 = {}
# start_time_map = time.perf_counter()
# def get_table_dict(file):
#     my_collection = mydb[file.split('.')[0]]
#     list_data = my_collection.find()
#     df = pd.DataFrame(list(list_data))
#     df = dd.from_pandas(df,npartitions=3)
#     tables_dictionary_1[file.split('.')[0]] = df
#
# reult_list = list(map(get_table_dict, files_list))
# tables_dictionary_2 = reult_list[0]
# # print(tables_dictionary_1)
# # print(tables_dictionary_2)
# end_time_map = time.perf_counter()
# total_time_map = end_time_map - start_time_map
# print(f'Total time map: {total_time_map}')
#
#
# tables_dictionary_3 = {}
#
# start_time_pandas = time.perf_counter()
# for index, file in enumerate(files_list):
#     my_collection = mydb[file.split('.')[0]]
#     list_data = my_collection.find()
#     df = pd.DataFrame(list(list_data))
#     tables_dictionary_3[file.split('.')[0]] = df
#
# end_time_pandas = time.perf_counter()
# total_time_pandas = end_time_pandas - start_time_pandas
# print(f'Total time pandas: {total_time_pandas}')
from pymongo import MongoClient
import pandas as pd
import time


start_time = time.perf_counter()
myclient = MongoClient(host='localhost', port=27017)
mydb = myclient['real_reviews']
reviews = mydb['reviews_2']
likes = mydb['likes_2']
df_reviews = pd.DataFrame(list(reviews.find({"isApprove": 'approved', "isDeleted": False}, {'_id': 1, "loc": 1, "title": 1,
                           'createdAt': 1, 'updatedAt': 1, 'fromUserId': 1, 'categoryId': 1})))
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
end_time = time.perf_counter()
total_time = end_time - start_time
print(f'Total time: {total_time} seconds')
df_merge.to_csv('optimize_df_merge.csv')