import pymongo
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

app = Flask(__name__)

class trend_results:
    def __init__(self):
        self.top_review_last_week = {}
        self.top_user_last_week = {}
        self.popular_review_last_month = {}
        self.popular_user_last_month = {}
        pass


    def GetTableDictionary(self):
        myclient = MongoClient(host=None, port=None)
        mydb = myclient['real_reviews']
        # list_1 = self.looping_json_files()
        # self.files_list = ['reviews_1.json', 'likes_1.json']
        self.files_list = ['reviews.json', 'likes.json']
        self.tables_dictionary = {}
        for index, file in enumerate(self.files_list):
            my_collection = mydb[file.split('.')[0]]
            list_data = my_collection.find()
            df = pd.DataFrame(list(list_data))
            self.tables_dictionary[file.split('.')[0]] = df
        return self.tables_dictionary

    def MergedDataframe(self):
        self.GetTableDictionary()
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
        created_dates = ([_.split('T')[0] for _ in self.df_merge_1['createdAt_x']])
        updated_dates = ([_.split('T')[0] for _ in self.df_merge_1['updatedAt_x']])
        self.df_merge_1['created_dates'] = created_dates
        self.df_merge_1['updated_dates'] = updated_dates
        self.df_merge_1['created_dates'] = pd.to_datetime(self.df_merge_1['created_dates'], dayfirst=True)
        self.df_merge_1['updated_dates'] = pd.to_datetime(self.df_merge_1['updated_dates'], dayfirst=True)

        self.df_merge_1.drop(labels=['createdAt_x', 'updatedAt_x'], inplace=True, axis=1)
        return self.df_merge_1

    def distance(self, lon1, lat1, lon2, lat2):
        x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
        y = (lat2 - lat1)
        return sqrt(x * x + y * y)

    def SubgroupCategoriesToDictionary(self):
        self.MergedDataframe()
        gb = (self.df_merge_1.groupby('categoryId'))
        self.cat_dict = {}
        for cat in gb.groups:
            self.cat_dict[cat] = gb.get_group(cat).reset_index()
        return self.cat_dict

    def TrendingNearReviews(self, long_rand, lat_rand):
        self.MergedDataframe()
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

    def CategoryWiseResult(self):
        self.SubgroupCategoriesToDictionary()
        cat_result = {}
        for keys in self.cat_dict.keys():
            results_list = []
            df_merge_cat = self.cat_dict[keys]
            top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = self.AllTrendingResults(
                df_merge_cat)
            # results_list.append(top_review_last_week.tolist())
            # results_list.append(top_user_last_week.tolist())
            # results_list.append(popular_review_last_month.tolist())
            # results_list.append(popular_user_last_month.tolist())
            # cat_result[keys] = results_list
            cat_result[keys] = {}
            cat_result[keys]['top_review_last_week'] = top_review_last_week.tolist()
            cat_result[keys]['top_user_last_week'] = top_user_last_week.tolist()
            cat_result[keys]['popular_review_last_month'] = popular_review_last_month.tolist()
            cat_result[keys]['popular_user_last_month'] = popular_user_last_month.tolist()
            self.top_review_last_week[keys] = top_review_last_week.tolist()
            self.top_user_last_week[keys] = top_user_last_week.tolist()
            self.popular_review_last_month[keys] = popular_review_last_month.tolist()
            self.popular_user_last_month[keys] = popular_user_last_month.tolist()
        # print(cat_result)
        return cat_result

    def CombinedResults(self):
        top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = self.AllTrendingResults(
            self.df_merge_1)
        top_review_last_week = top_review_last_week.tolist()
        top_user_last_week = top_user_last_week.tolist()
        popular_review_last_month = popular_review_last_month.tolist()
        popular_user_last_month = popular_user_last_month.tolist()

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
def main():
    # if request.method == 'POST':
    result = trend_results()
    _ = result.CategoryWiseResult()
    top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = result.CombinedResults()
    return top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month


@app.route('/trending-review', methods=['GET', 'POST'])
def main_1():
    matching_key = request.args.get('categoryid')
    # rev = top_popular_results()
    top_review_last_week, _, _, _ = main()
    if matching_key == '':
        return {'combined': top_review_last_week['combinedResults']}
    elif matching_key != '':
        try:
            return {'combined': top_review_last_week[matching_key]}
        except:
            return {'combined': f'This category {matching_key} has no results'}


@app.route('/trending-user', methods=['GET', 'POST'])
def main_2():
    matching_key = request.args.get('categoryid')
    # rev = top_popular_results()
    _, top_user_last_week, _, _ = main()
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
        if popular_review_last_month['combinedResults']:
            return {'combined': popular_review_last_month['combinedResults']}
        else:
            return {'combined': f'This category {matching_key} has no results'}
    elif matching_key != '':
        try:
            # popular_review_last_month[matching_key]:
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
    else:
        return {'No Result': 'please enter blank for combined result or the category number'}

@app.route('/near-location', methods=['GET', 'POST'])
def main_5():
    matching_key = request.args.get('longlat')
    # rev = top_popular_results()
    rev = trend_results()
    # print(matching_key.split(',')[1:])
    # print(float(str(matching_key.split(',')[0][1:])))
    long_rand, lat_rand = float(str(matching_key.split(',')[0])), float(str(matching_key.split(',')[1]))
    trending_reviews_nearby = rev.TrendingNearReviews(long_rand, lat_rand)
    result = {}
    result['near_review_id'] = trending_reviews_nearby.tolist()[:10]
    return {'near_review_id': result}

if __name__ == "__main__":
    app.run(debug=True)
    # top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = main()



