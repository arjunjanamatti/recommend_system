# import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json
import pandas as pd
from datetime import timedelta
from math import *
import random
from flask import Flask, request
import time

files_list = ['reviews_1.json','likes_1.json']

app = Flask(__name__)

class trend_results:
    def __init__(self):
        self.top_review_last_week = {}
        self.top_user_last_week = {}
        self.popular_review_last_month = {}
        self.popular_user_last_month = {}
        pass

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

    # def looping_json_files(self):
    #     list_1 = []
    #     self.files_list = ['reviews_1.json', 'likes_1.json']
    #     # self.files_list = ['reviews.json', 'likes.json']
    #     for files in self.files_list:
    #         with open(files) as file:
    #             data = json.load(file)
    #             list_1.append(data)
    #     return list_1

    def GetTableDictionary(self):
        myclient = MongoClient(host='localhost', port=27017)
        mydb = myclient['real_reviews']
        # list_1 = self.looping_json_files()
        # self.files_list = ['reviews.json', 'likes.json']
        self.files_list = ['reviews_1.json', 'likes_1.json']
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
        self.df_merge_1 = df_merge[['resourceId', 'loc', 'createdAt_x', 'updatedAt_x', 'fromUserId_x', 'categoryId']]
        # longititude extraction from the loc
        longitude = [_['coordinates'][0] for _ in self.df_merge_1['loc']]

        latitude = [_['coordinates'][1] for _ in self.df_merge_1['loc']]
        self.df_merge_1['longitude'] = longitude
        self.df_merge_1['latitude'] = latitude
        self.df_merge_1.drop(labels='loc', inplace=True, axis=1)
        self.df_merge_1['createdAt_x'] = self.df_merge_1['createdAt_x'].apply(lambda x: str(x))
        self.df_merge_1['updatedAt_x'] = self.df_merge_1['updatedAt_x'].apply(lambda x: str(x))
        created_dates = ([_.split('T')[0] for _ in self.df_merge_1['createdAt_x']])
        updated_dates = ([_.split('T')[0] for _ in self.df_merge_1['updatedAt_x']])
        self.df_merge_1['created_dates'] = created_dates
        self.df_merge_1['updated_dates'] = updated_dates
        self.df_merge_1['created_dates'] = pd.to_datetime(self.df_merge_1['created_dates'], dayfirst=True)
        self.df_merge_1['updated_dates'] = pd.to_datetime(self.df_merge_1['updated_dates'], dayfirst=True)

        self.df_merge_1.drop(labels=['createdAt_x', 'updatedAt_x'], inplace=True, axis=1)
        if len(user_id) > 0:
            print('INside IF LOOP')
            for val in self.try_dict.values():
                if user_id in val:
                    remove_id = str([v for v in val if v != user_id][-1])
                    print(user_id)
                    self.df_merge_1 = self.df_merge_1[~self.df_merge_1.fromUserId_x.str.contains(remove_id)]
        return self.df_merge_1

    def TopProducts(self, filename, user_id):
        self.MergedDataframe(user_id)
        files_list = [filename]
        # get the number of likes for each review from the dataframe
        # print(self.df_merge_1)
        review_id_like_count_df = (self.df_merge_1.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
            columns={'updated_dates': 'ReviewViewCount'}))
        review_id_like_count_df = (review_id_like_count_df.sort_values(['ReviewViewCount'], ascending=False))
        review_id_like_count_df.index = review_id_like_count_df['resourceId']
        review_id_like_count_df = review_id_like_count_df.drop(['resourceId'], axis=1)
        # print(review_id_like_count_df.head())
        myclient = MongoClient()
        mydb = myclient['realreview']
        tables_dictionary = {}
        for index, file in enumerate(files_list):
            my_collection = mydb[file]
            list_data = my_collection.find()
            df = pd.DataFrame(list(list_data))
            tables_dictionary[file] = df
        products_1_df = tables_dictionary[filename]
        # sum_ind_list = []

        def try_func(list_1):
            sum_ind = 0
            print(list_1)
            for indices in list_1:
                sum_ind += int(review_id_like_count_df.loc[indices])
            return sum_ind
        # print(review_id_like_count_df.head())
        products_1_df['likes_sum'] = products_1_df['reviews'].apply(lambda x: try_func(x))
        # for l in products_1_df['review_id_tags']:
        #     # loop through all the reviews inside the reviews array
        #     sum_ind = 0
        #     for indices in l:
        #         sum_ind += int(review_id_like_count_df.loc[indices])
        #     sum_ind_list.append(sum_ind)
        # products_1_df['likes_sum'] = sum_ind_list
        products_1_df = products_1_df.sort_values(by=['likes_sum'], ascending=False)
        return list(products_1_df['_id'])

    def distance(self, lon1, lat1, lon2, lat2):
        x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
        y = (lat2 - lat1)
        return sqrt(x * x + y * y)

    def SubgroupCategoriesToDictionary(self,user_id):
        self.MergedDataframe(user_id)
        gb = (self.df_merge_1.groupby('categoryId'))
        self.cat_dict = {}
        for cat in gb.groups:
            self.cat_dict[cat] = gb.get_group(cat).reset_index()
        return self.cat_dict

    def TrendingNearReviews(self, long_rand, lat_rand, user_id):
        self.MergedDataframe(user_id)
        # index_list = list(self.df_merge_1.index)
        # random_index = random.choice(index_list)
        # user_rand, review_rand = str(self.df_merge_1.iloc[random_index]['fromUserId_x']), str(
        #     self.df_merge_1.iloc[random_index]['resourceId'])
        # long_rand, lat_rand = float(self.df_merge_1.iloc[random_index]['longitude']), float(
        #     self.df_merge_1.iloc[random_index]['latitude'])
        dist_list = []
        for index, lat in enumerate(self.df_merge_1.loc[:, 'latitude']):
            dist_measured = self.distance(long_rand, lat_rand, self.df_merge_1.loc[index, 'longitude'], lat)
            dist_list.append(dist_measured)

        df_merge_cat_1 = self.df_merge_1.copy()
        df_merge_cat_1['dist_list'] = dist_list
        df_merge_cat_1 = df_merge_cat_1.sort_values('dist_list')
        groupby_like_count = (df_merge_cat_1.groupby(['resourceId'])['updated_dates'].count().reset_index().rename(
            columns={'updated_dates': 'ReviewViewCount'}))
        df_merge_cat_1_count_merge = (df_merge_cat_1.merge(groupby_like_count, on='resourceId'))
        df_merge_cat_1_count_merge = df_merge_cat_1_count_merge.sort_values(by=['ReviewViewCount', 'dist_list'],
                                                                            ascending=[False, True])
        return (df_merge_cat_1_count_merge['resourceId'].unique())

    def TopTrendingResults(self,df_merge_cat, num_days, column_name):
        today = pd.to_datetime('today').floor('D')
        week_prior = today - timedelta(days=num_days)
        df_last_week = df_merge_cat[
            (df_merge_cat['updated_dates'] <= today) & (df_merge_cat['updated_dates'] >= week_prior)]
        top_10_last_week_df = (df_last_week.groupby([column_name])['updated_dates'].count().reset_index().rename(
            columns={'updated_dates': 'ReviewLikeCount'}))
        top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewLikeCount'], ascending=False))
        week_num = num_days
        while (len(top_10_reviews_last_week[column_name].unique()) < 10):
            week_num += num_days
            week_prior = today - timedelta(days=week_num)
            df_last_week = df_merge_cat[
                (df_merge_cat['updated_dates'] <= today) & (df_merge_cat['updated_dates'] >= week_prior)]
            top_10_last_week_df = (df_last_week.groupby([column_name])['updated_dates'].count().reset_index().rename(
                columns={'updated_dates': 'ReviewLikeCount'}))
            top_10_reviews_last_week = (top_10_last_week_df.sort_values(['ReviewLikeCount'], ascending=False))
            if (week_prior < df_merge_cat['updated_dates'].min()):
                break
        return (top_10_reviews_last_week[column_name].unique())

    def AllTrendingResults(self, df_merge_cat):
        top_review_last_week = self.TopTrendingResults(df_merge_cat, 7, 'resourceId')
        top_user_last_week = self.TopTrendingResults(df_merge_cat, 7, 'fromUserId_x')
        popular_review_last_week = self.TopTrendingResults(df_merge_cat, 30, 'resourceId')
        popular_user_last_week = self.TopTrendingResults(df_merge_cat, 30, 'fromUserId_x')
        return top_review_last_week, top_user_last_week, popular_review_last_week, popular_user_last_week

    def CategoryWiseResult(self, user_id):
        self.SubgroupCategoriesToDictionary(user_id)
        cat_result = {}
        for keys in self.cat_dict.keys():
            results_list = []
            df_merge_cat = self.cat_dict[keys]
            top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = self.AllTrendingResults(
                df_merge_cat)
            top_review_last_week = top_review_last_week.tolist()
            top_user_last_week = top_user_last_week.tolist()
            popular_review_last_month = popular_review_last_month.tolist()
            popular_user_last_month = popular_user_last_month.tolist()
            # converting each element in list to a string
            top_review_last_week = [str(top) for top in top_review_last_week]
            top_user_last_week = [str(top) for top in top_user_last_week]
            popular_review_last_month = [str(top) for top in popular_review_last_month]
            popular_user_last_month = [str(top) for top in popular_user_last_month]
            # results_list.append(top_review_last_week.tolist())
            # results_list.append(top_user_last_week.tolist())
            # results_list.append(popular_review_last_month.tolist())
            # results_list.append(popular_user_last_month.tolist())
            # cat_result[keys] = results_list
            cat_result[keys] = {}
            cat_result[keys]['top_review_last_week'] = top_review_last_week[:10]
            cat_result[keys]['top_user_last_week'] = top_user_last_week[:10]
            cat_result[keys]['popular_review_last_month'] = popular_review_last_month[:10]
            cat_result[keys]['popular_user_last_month'] = popular_user_last_month[:10]
            self.top_review_last_week[keys] = top_review_last_week[:10]
            self.top_user_last_week[keys] = top_user_last_week[:10]
            self.popular_review_last_month[keys] = popular_review_last_month[:10]
            self.popular_user_last_month[keys] = popular_user_last_month[:10]
        # print(cat_result)
        return cat_result

    def CombinedResults(self):
        top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = self.AllTrendingResults(
            self.df_merge_1)
        top_review_last_week = top_review_last_week.tolist()
        top_user_last_week = top_user_last_week.tolist()
        popular_review_last_month = popular_review_last_month.tolist()
        popular_user_last_month = popular_user_last_month.tolist()
        # converting each element in list to a string
        top_review_last_week = [str(top) for top in top_review_last_week]
        top_user_last_week = [str(top) for top in top_user_last_week]
        popular_review_last_month = [str(top) for top in popular_review_last_month]
        popular_user_last_month = [str(top) for top in popular_user_last_month]
        # overall_result = {}
        # overall_result['combinedResults'] = {}
        # overall_result['combinedResults']['top_review_last_week'] = top_review_last_week[:10]
        # overall_result['combinedResults']['top_user_last_week'] = top_user_last_week[:10]
        # overall_result['combinedResults']['popular_review_last_month'] = popular_review_last_month[:10]
        # overall_result['combinedResults']['popular_user_last_month'] = popular_user_last_month[:10]
        self.top_review_last_week['combinedResults'] = top_review_last_week[:10]
        self.top_user_last_week['combinedResults'] = top_user_last_week[:10]
        self.popular_review_last_month['combinedResults'] = popular_review_last_month[:10]
        self.popular_user_last_month['combinedResults'] = popular_user_last_month[:10]
        return self.top_review_last_week, self.top_user_last_week, self.popular_review_last_month, self.popular_user_last_month

# @app.route('/trending', methods=['GET', 'POST'])
def main(user_id):
    # if request.method == 'POST':
    result = trend_results()
    # top_products = result.TopProducts('products')
    # top_services = result.TopProducts('services')
    _ = result.CategoryWiseResult(user_id)
    top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = result.CombinedResults()
    # return top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month, top_products, top_services
    return top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month


@app.route('/trending-review', methods=['GET', 'POST'])
def main_1():
    matching_key = request.args.get('categoryid')
    user_id = request.args.get('userid')
    print(matching_key, user_id)
    try:
        top_review_last_week, _, _, _ = main(user_id)

        if matching_key == '':
            return {'combined': top_review_last_week['combinedResults']}
        elif matching_key != '':
            try:
                return {'combined': top_review_last_week[matching_key]}
            except:
                return {'combined': f'This category {matching_key} has no results'}
    except:
        return {'combined': f'user_id: {user_id} does not exist in our records'}


@app.route('/trending-user', methods=['GET', 'POST'])
def main_2():
    matching_key = request.args.get('categoryid')
    # rev = top_popular_results()
    _, top_user_last_week, _, _ = main()
    print(top_user_last_week)
    if matching_key == '':
        return {'combined': top_user_last_week['combinedResults']}
    elif matching_key != '':
        try:
            return {'combined': top_user_last_week[matching_key]}
        except:
            return {'combined': f'This category {matching_key} has no results'}


@app.route('/popular-review', methods=['GET', 'POST'])
def main_3():
    matching_key = request.args.get('categoryid')
    # rev = top_popular_results()
    _, _, popular_review_last_month, _ = main()
    if matching_key == '':
        return {'combined': popular_review_last_month['combinedResults']}
    elif matching_key != '':
        try:
            return {'combined': popular_review_last_month[matching_key]}
        except:
            return {'combined': f'This category {matching_key} has no results'}


@app.route('/popular-user', methods=['GET', 'POST'])
def main_4():
    matching_key = request.args.get('categoryid')
    # rev = top_popular_results()
    _, _, _, popular_user_last_month = main()
    if matching_key == '':
        return {'combined': popular_user_last_month['combinedResults']}
    elif matching_key != '':
        try:
            return {f'{matching_key}': popular_user_last_month[matching_key]}
        except:
            return {'combined': f'This category {matching_key} has no results'}


@app.route('/near-location', methods=['GET', 'POST'])
def main_5():
    matching_key = request.args.get('longlat')
    # rev = top_popular_results()
    rev = trend_results()
    # print(matching_key.split(',')[1:])
    # print(float(str(matching_key.split(',')[0][1:])))
    long_rand, lat_rand = float(str(matching_key.split(',')[0])), float(str(matching_key.split(',')[1]))
    trending_reviews_nearby = rev.TrendingNearReviews(long_rand, lat_rand)
    trending_reviews_nearby = [str(review) for review in trending_reviews_nearby]
    # result = {}
    # result['near_review_id'] = trending_reviews_nearby[:10]
    return {'near_review_id': trending_reviews_nearby[:10]}

@app.route('/connected', methods=['GET', 'POST'])
def main_6():
    rev = trend_results()
    a  = rev.GetTableDictionary()
    print(a)
    if a:
        return {'Result': 'Connected to db'}
    else:
        return {'Result': 'Not Connected to db'}

# @app.route('/top-products', methods=['GET', 'POST'])
# def main_7():
#     _, _, _, _, top_products, _ = main()
#     result = {}
#     top_products = [str(top) for top in top_products]
#     result['top_products_id'] = top_products
#     return {'top_products_id': result}
#
# @app.route('/top-services', methods=['GET', 'POST'])
# def main_8():
#     _, _, _, _, _, top_services = main()
#     result = {}
#     top_services = [str(top) for top in top_services]
#     result['top_products_id'] = top_services
#     return {'top_products_id': top_services}

if __name__ == "__main__":
    app.run(debug=True)
    # top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = main()



