# import pymongo
from pymongo import MongoClient
import logging
import pandas as pd
from datetime import timedelta
from math import *
from flask import Flask, request
import numpy as np

app = Flask(__name__)

class trend_results:
    def __init__(self):
        self.top_review_last_week = {}
        self.top_user_last_week = {}
        self.popular_review_last_month = {}
        self.popular_user_last_month = {}
        self.myclient = MongoClient(host=None, port=None)
        self.mydb = self.myclient['real_reviews']
        pass

    def MergeDataframeUpdate(self, category_id, user_id, search_text, target_userid):
        reviews = self.mydb['reviews_2']
        likes = self.mydb['likes_2']
        blockusers = self.mydb['blockusers']

        ##### Blockusersdata
        cur = blockusers.find({}, {'blockUserId': 1, 'fromUserId': 1})
        block_users_dict_list = [doc for doc in cur]
        try_list = []
        block_list = [([new['blockUserId'], new['fromUserId']]) for new in block_users_dict_list]
        block_list = [(y) for x in block_list for y in x]

        ##### searchtext part
        # extract fields where review is approved and not deleted, also selecting only required fields
        reviews_filter = {"isApprove": 'approved', "isDeleted": False,
                          "title": {"$regex": f".*{search_text}.*"}} if search_text != None else {
            "isApprove": 'approved', "isDeleted": False}

        #### blockusers part
        if len(user_id) > 0:
            try:
                reviews_filter['fromUserId'] = {'$nin': block_list}
            except:
                raise

        ##### categoryid part
        if len(category_id) > 0:
            try:
                reviews_filter['categoryId'] = f'{category_id}'
            except:
                raise


        df_reviews = pd.DataFrame(list(reviews.find(reviews_filter, {'_id': 1, "loc": 1, "title": 1,
                                                                     'createdAt': 1, 'updatedAt': 1, 'fromUserId': 1,
                                                                     'categoryId': 1})))
        # from likes table only review _id and resourceId field
        df_likes = pd.DataFrame(list(likes.find({}, {'_id': 1, 'resourceId': 1})))
        df_reviews.set_index('_id', inplace=True)
        df_likes.set_index('resourceId', inplace=True)
        self.df_merge = df_reviews.join(df_likes, how='left')
        self.df_merge['longitude'] = self.df_merge['loc'].apply(lambda x: x['coordinates'][0])
        self.df_merge['latitude'] = self.df_merge['loc'].apply(lambda x: x['coordinates'][1])
        self.df_merge['created_dates'] = self.df_merge['createdAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge['updated_dates'] = self.df_merge['updatedAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge['created_dates'] = pd.to_datetime(self.df_merge['created_dates'], dayfirst=True)
        self.df_merge['updated_dates'] = pd.to_datetime(self.df_merge['updated_dates'], dayfirst=True)
        self.df_merge.drop(labels=['createdAt', 'updatedAt', 'loc', "_id"], inplace=True, axis=1)
        self.df_merge['resourceId'] = self.df_merge.index
        self.df_merge.reset_index(drop=True, inplace=True)

    def TargetUserId(self, target_userid):
        empty_list = []
        reviews = self.mydb['reviews_2']
        if target_userid != None:
            targetuserid_reviewlist = list(reviews.find({'fromUserId': target_userid}, {'_id': 1}))
            self.targetuserid_reviewlist = [reviews['_id'] for reviews in targetuserid_reviewlist]
            return self.targetuserid_reviewlist
        else:
            return empty_list
        pass

    def distance(self, lon1, lat1, lon2, lat2):
        x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
        y = (lat2 - lat1)
        return sqrt(x * x + y * y)

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

    def TopReviews(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text, target_userid)
        # return self.TopTrendingResults(self.df_merge, 7, 'resourceId')
        if target_userid != None:
            targetuserid_reviewlist = self.TargetUserId(target_userid)
            # print(f'lengh of targetuser id list: {len(targetuserid_reviewlist)}')
            if len(targetuserid_reviewlist) > 0:
                return list(set(targetuserid_reviewlist).intersection((self.TopTrendingResults(self.df_merge, 7, 'resourceId'))))
            else:
                empty_list = []
                return empty_list
        else:
            return self.TopTrendingResults(self.df_merge, 7, 'resourceId')

    def TopUsers(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text, target_userid)
        return self.TopTrendingResults(self.df_merge, 7, 'fromUserId')

    def PopularReviews(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text, target_userid)
        # return self.TopTrendingResults(self.df_merge, 30, 'resourceId')
        all_results = self.TopTrendingResults(self.df_merge, 30, 'resourceId')[:50]
        # print(all_results)
        if target_userid != None:
            targetuserid_reviewlist = self.TargetUserId(target_userid)
            # print(f'lengh of targetuser id list: {len(targetuserid_reviewlist)}')
            # print(targetuserid_reviewlist)
            if len(targetuserid_reviewlist) > 0:
                return list(set(all_results).intersection(targetuserid_reviewlist))
            else:
                empty_list = []
                return empty_list
        else:
            return all_results

    def PopularUsers(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text, target_userid)
        return self.TopTrendingResults(self.df_merge, 30, 'fromUserId')

        # if (target_userid != None) & (len(targetuserid_reviewlist) > 0):
        #     try:
        #         return list(set(targetuserid_reviewlist).intersection((self.TopTrendingResults(self.df_merge, 30, 'fromUserId'))))
        #     except:
        #         return []
        # elif (target_userid != None) & (len(targetuserid_reviewlist) == 0):
        #     print('inside empty list')
        #     return targetuserid_reviewlist
        # elif target_userid == None:
        #     return self.TopTrendingResults(self.df_merge, 30, 'fromUserId')

#     def AllTrendingResults(self, user_id, search_text, target_userid, category_id):
#         self.MergeDataframeUpdate(user_id, search_text, target_userid, category_id)
#         top_review_last_week = self.TopTrendingResults(self.df_merge, 7, 'resourceId')
#         top_user_last_week = self.TopTrendingResults(self.df_merge, 7, 'fromUserId')
#         popular_review_last_week = self.TopTrendingResults(self.df_merge, 30, 'resourceId')
#         popular_user_last_week = self.TopTrendingResults(self.df_merge, 30, 'fromUserId')
#         return top_review_last_week, top_user_last_week, popular_review_last_week, popular_user_last_week
#
# def main(user_id, search_text, target_userid, category_id):
#     result = trend_results()
#     top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = result.AllTrendingResults(user_id, search_text, target_userid, category_id)
#     print(top_review_last_week)

@app.route('/trending-review', methods=['GET', 'POST'])
def main_1():
    category_id = request.args.get('categoryid')
    user_id = request.args.get('userid')
    search_text = request.args.get('searchtext', default = None)

    target_userid = request.args.get('targetuserid', default = None)
    if search_text:
        search_text = search_text.lower()
        # search_text = list(search_text.split())
    print(search_text)
    try:
        result = trend_results()
        top_review_last_week = result.TopReviews(category_id, user_id, search_text, target_userid)
        top_review_last_week = [str(top) for top in top_review_last_week][:50]
        if category_id == '':
            return {'combined': top_review_last_week}
        elif category_id != '':
            try:
                return {f'{category_id}': top_review_last_week}
            except:
                return {f'{category_id}': f'This category {category_id} has no results'}
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # print(f'Exception: {type(e).__name__}')
        return {'error': f'user_id: {user_id} or {search_text} does not exist in our records'}


@app.route('/trending-user', methods=['GET', 'POST'])
def main_2():
    category_id = request.args.get('categoryid')
    user_id = request.args.get('userid')
    search_text = request.args.get('searchtext', default = None)

    target_userid = request.args.get('targetuserid', default = None)
    if search_text:
        search_text = search_text.lower()
        # search_text = list(search_text.split())
    print(search_text)
    try:
        result = trend_results()
        top_user_last_week = result.TopUsers(category_id, user_id, search_text, target_userid)
        top_user_last_week = [str(top) for top in top_user_last_week][:50]
        if category_id == '':
            return {'combined': top_user_last_week}
        elif category_id != '':
            try:
                return {f'{category_id}': top_user_last_week}
            except:
                return {f'{category_id}': f'This category {category_id} has no results'}
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # print(f'Exception: {type(e).__name__}')
        return {'error': f'user_id: {user_id} or {search_text} does not exist in our records'}


@app.route('/popular-review', methods=['GET', 'POST'])
def main_3():
    category_id = request.args.get('categoryid')
    user_id = request.args.get('userid')
    search_text = request.args.get('searchtext', default = None)

    target_userid = request.args.get('targetuserid', default = None)
    if search_text:
        search_text = search_text.lower()
        # search_text = list(search_text.split())
    print(search_text)
    try:
        result = trend_results()
        popular_review_last_month = result.PopularReviews(category_id, user_id, search_text, target_userid)
        popular_review_last_month = [str(top) for top in popular_review_last_month][:50]
        if category_id == '':
            return {'combined': popular_review_last_month}
        elif category_id != '':
            try:
                return {f'{category_id}': popular_review_last_month}
            except Exception as e:
                print(f'Exception: {e}')
                return {f'{category_id}': f'This category {category_id} has no results'}
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # print(f'Exception: {type(e).__name__}')
        print(f'Exception: {e}')
        return {'error': f'user_id: {user_id} or {search_text} does not exist in our records'}

@app.route('/popular-user', methods=['GET', 'POST'])
def main_4():
    category_id = request.args.get('categoryid')
    user_id = request.args.get('userid')
    search_text = request.args.get('searchtext', default = None)

    target_userid = request.args.get('targetuserid', default = None)
    if search_text:
        search_text = search_text.lower()
        # search_text = list(search_text.split())
    print(search_text)
    try:
        result = trend_results()
        popular_user_last_month = result.PopularUsers(category_id, user_id, search_text, target_userid)
        popular_user_last_month = [str(top) for top in popular_user_last_month][:50]
        if category_id == '':
            return {'combined': popular_user_last_month}
        elif category_id != '':
            try:
                return {f'{category_id}': popular_user_last_month}
            except:
                return {f'{category_id}': f'This category {category_id} has no results'}
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # print(f'Exception: {type(e).__name__}')
        return {'error': f'user_id: {user_id} or {search_text} does not exist in our records'}


if __name__ == '__main__':
    import time
    start_time = time.perf_counter()
    app.run(debug=True)
    # user_id, search_text, target_userid, category_id = '', 'nokia', None, ''
    # result = trend_results()
    # result = result.PopularReviews(user_id, search_text, target_userid, category_id)
    # print(result)
    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f'Total time: {total_time} seconds')