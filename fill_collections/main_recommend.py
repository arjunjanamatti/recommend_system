from pymongo import MongoClient
import json
import pandas as pd
from datetime import timedelta
from math import *
from flask import Flask, request
import numpy as np
import time

app = Flask(__name__)

class recommend_results:
    def GetBlockUsersData(self):
        myclient = MongoClient(host=None, port=None)
        mydb = myclient['real_reviews']
        coll = mydb['blockusers']
        cur = coll.find()
        block_users_dict_list = [doc for doc in cur]

        self.try_dict = {}
        for new in block_users_dict_list:
            self.try_dict[new['_id']] = []
            self.try_dict[new['_id']].append(new['blockUserId'])
            self.try_dict[new['_id']].append(new['fromUserId'])

    def GetTableDictionary(self):
        myclient = MongoClient(host='localhost', port=27017)
        mydb = myclient['real_reviews']
        # self.files_list = ['reviews.json', 'likes.json']
        self.files_list = ['reviews_2.json', 'likes_2.json']
        self.tables_dictionary = {}
        for index, file in enumerate(self.files_list):
            my_collection = mydb[file.split('.')[0]]
            list_data = my_collection.find()
            df = pd.DataFrame(list(list_data))
            self.tables_dictionary[file.split('.')[0]] = df
        return self.tables_dictionary

    def MergedDataframe(self, user_id):
        self.GetTableDictionary()
        self.GetBlockUsersData()
        # transform the reviews_1 table to df_1 dataframe
        df_1 = self.tables_dictionary[self.files_list[0].split('.')[0]]
        # select reviews which are approved
        df_1_approve = (df_1[df_1['isApprove'] == 'approved'])
        # transform the likes_1 table to df_1 dataframe
        df_2 = self.tables_dictionary[self.files_list[-1].split('.')[0]]
        # rename the column name in reviews_1 table to resourceId as per likes_1 table
        df_1_approve = df_1_approve.rename(columns={"_id": "resourceId"})
        # merge both the dataframes based on common column 'resourceId'
        df_merge = df_1_approve.merge(df_2, how='left', on='resourceId')

        # extract only required columns from the merged dataframe
        self.df_merge_1 = df_merge[['resourceId', 'loc','rating', 'createdAt_x', 'updatedAt_x','fromUserId_x' ,'fromUserId_y', 'categoryId']]
        # longititude extraction from the loc
        longitude = [_['coordinates'][0] for _ in self.df_merge_1['loc']]

        latitude = [_['coordinates'][1] for _ in self.df_merge_1['loc']]
        self.df_merge_1['longitude'] = longitude
        self.df_merge_1['latitude'] = latitude
        self.df_merge_1.drop(labels='loc', inplace=True, axis=1)
        created_dates = ([_.split('T')[0] for _ in self.df_merge_1['createdAt_x']])
        updated_dates = ([_.split('T')[0] for _ in self.df_merge_1['updatedAt_x']])
        self.df_merge_1['created_dates'] = created_dates
        self.df_merge_1['updated_dates'] = updated_dates
        self.df_merge_1['created_dates'] = pd.to_datetime(self.df_merge_1['created_dates'], dayfirst=True)
        self.df_merge_1['updated_dates'] = pd.to_datetime(self.df_merge_1['updated_dates'], dayfirst=True)

        self.df_merge_1.drop(labels=['createdAt_x', 'updatedAt_x'], inplace=True, axis=1)
        if len(user_id) > 0:
            print('INSIDE IF LOOP')
            for val in self.try_dict.values():
                if user_id in val:
                    remove_id = str([v for v in val if v != user_id][-1])
                    print(f'REMOVID: {remove_id}')
                    self.df_merge_1 = self.df_merge_1[~self.df_merge_1.fromUserId_x.str.contains(remove_id)]
            self.df_merge_1.reset_index(inplace=True)
        self.df_merge_1.to_csv('df_merge.csv')
        return self.df_merge_1

    def GetRecommendations(self, user_id):
        df_2 = self.MergedDataframe(user_id)
        review_id_like_count_df = (df_2.groupby(['resourceId'])['fromUserId_y'].count().reset_index().rename(
            columns={'fromUserId_y': 'ReviewViewCount'}))
        df_2_merge = df_2.merge(review_id_like_count_df, on='resourceId')
        df_2_merge = df_2_merge.drop(labels=['updated_dates', 'created_dates', 'longitude', 'latitude'], axis=1)
        df_2_merge = df_2_merge.drop_duplicates().reset_index()
        df_2_merge.to_csv('df_2_merge.csv')
        # pivot table
        features_df = df_2_merge.pivot_table(index='resourceId', columns='fromUserId_y',
                                             values='ReviewViewCount').fillna(0.0)
        features_df.to_csv('features_df_pivot.csv')
        # will convert the above to array matrix
        from scipy.sparse import csr_matrix
        from sklearn.neighbors import NearestNeighbors

        features_matrix = csr_matrix(arg1=features_df)
        model_knn = NearestNeighbors(metric='cosine', algorithm='brute', radius=0.001)
        model_knn.fit(features_matrix)

        query_index = np.random.choice(features_matrix.shape[0])
        previous_review_id = features_df.iloc[query_index, :].name
        print(f'Name of the mobile: {previous_review_id}')
        print(features_matrix.shape[0])
        try:
            distances, indices = model_knn.kneighbors(features_df.iloc[query_index, :].values.reshape(1, -1),
                                                      n_neighbors=10)
            review_id_recommend_cosine_similarity = []
            # print(distances)
            for i, j in zip(distances[0][:10], indices[0][:10]):
                if i == 0.0:
                    pass
                else:
                    review_id_recommend_cosine_similarity.append(features_df.iloc[j, :].name)
                    # print(
                    #     f'Mobile model: {features_df.iloc[j, :].name} is similar to {previous_review_id} with distance of {i}')
                    # print()
            return review_id_recommend_cosine_similarity
        except:
            distances, indices = model_knn.kneighbors(features_df.iloc[query_index, :].values.reshape(1, -1),
                                                      n_neighbors=features_matrix.shape[0])
            review_id_recommend_cosine_similarity = []
            # print(distances)
            for i, j in zip(distances[0], indices[0]):
                if i == 0.0:
                    pass
                else:
                    review_id_recommend_cosine_similarity.append(features_df.iloc[j, :].name)
                    # print(
                    #     f'Mobile model: {features_df.iloc[j, :].name} is similar to {previous_review_id} with distance of {i}')
                    # print()
            return review_id_recommend_cosine_similarity

@app.route('/recommend')
def main_1():
    user_id = request.args.get('userid')
    result = recommend_results()
    recommend_list = result.GetRecommendations(user_id)
    return {'recommend_result': recommend_list}
    pass

if __name__ == '__main__':
    # result = recommend_results()
    # # user_id = '5fdf6bbcfe08e8c0191a7805'
    # user_id = ''
    # recommend_list = result.GetRecommendations(user_id)
    # print(recommend_list)
    app.run(debug=True, port=5050)

