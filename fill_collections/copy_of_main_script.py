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
        self.mydb = self.myclient['realreview']
        pass

    def MergeDataframeUpdate(self, category_id, user_id, search_text):
        reviews = self.mydb['reviews']
        likes = self.mydb['likes']
        blockusers = self.mydb['blockusers']

        ##### Blockusersdata
        cur = blockusers.find({}, {'blockUserId': 1, 'fromUserId': 1})
        block_users_dict_list = [doc for doc in cur]
        try_list = []
        block_list = [([new['blockUserId'], new['fromUserId']]) for new in block_users_dict_list]
        block_list = [(y) for x in block_list for y in x]
        self.block_list = [str(bloc) for bloc in block_list]
        self.block_list = [str(block) for block in self.block_list if block != user_id]
        self.block_list = list(set(self.block_list))

        ##### searchtext part
        # extract fields where review is approved and not deleted, also selecting only required fields
        reviews_filter = {"isApprove": 'approved', "isDeleted": False, "categoryId": {"$exists": True}}



        # ##### categoryid part
        # if len(category_id) > 0:
        #     category_id_list = [rev['categoryId'] for rev in list(reviews.find({"isApprove": 'approved', "isDeleted": False, "categoryId": {"$exists": True}}, {'categoryId': 1}))]
        #     category_id_list = [str(cat) for cat in category_id_list]
        #     category_id = str(category_id)
        #     if category_id in category_id_list:
        #         reviews_filter['categoryId']['$eq'] = f'{category_id}'
        #     else:
        #         raise

        print(f'reviews_filter: {reviews_filter}')
        self.df_reviews = pd.DataFrame(list(reviews.find(reviews_filter, {'_id': 1, "title": 1,
                                                                     'createdAt': 1, 'updatedAt': 1, 'fromUserId': 1,
                                                                     'categoryId': 1})))
        # convert all columns to string and make title phrases lower
        self.df_reviews['_id'] = self.df_reviews['_id'].astype(str)
        self.df_reviews['title'] = self.df_reviews['title'].apply(lambda x: str(x).lower())
        self.df_reviews['createdAt'] = self.df_reviews['createdAt'].astype(str)
        self.df_reviews['updatedAt'] = self.df_reviews['updatedAt'].astype(str)
        self.df_reviews['fromUserId'] = self.df_reviews['fromUserId'].astype(str)
        self.df_reviews['categoryId'] = self.df_reviews['categoryId'].astype(str)

        category_id_list = list(self.df_reviews['categoryId'])
        self.category_id_list = list(set(category_id_list))
        self.combine_title = list(self.df_reviews["title"])
        self.user_id_list = list(self.df_reviews['fromUserId'])
        self.user_id_list = list(set(self.user_id_list))

        # #### blockusers part
        # if len(user_id) > 0:
        #     block_list = list(set(block_list))
        #     block_list = [str(block) for block in block_list if block != user_id]
        #     user_id_list = list(df_reviews['fromUserId'])
        #     user_id_list = list(set(user_id_list))
        #
        #     if user_id in user_id_list:
        #         self.df_reviews = df_reviews[~df_reviews['fromUserId'].isin(block_list)]
        #         if len(df_reviews) == 0:
        #             raise
        #     else:
        #         raise

        # # print(df_reviews.head())
        # if search_text != None:
        #     print(f'INSIDE IF SEARCHTEXT LOOP: {search_text}')
        #     combine_title = list(df_reviews["title"])
        #     if [s for s in combine_title if all(xs in s for xs in search_text)]:
        #         contains = [df_reviews['title'].str.contains(i) for i in search_text]
        #         df_reviews = df_reviews[np.all(contains, axis=0)]
        #     else:
        #         raise KeyError
        #
        #
        #
        # ##### categoryid part
        # if len(category_id) > 0:
        #     if category_id in category_id_list:
        #         df_reviews = df_reviews[df_reviews['categoryId'] == str(category_id)]
        #         if len(df_reviews) == 0:
        #             raise KeyError
        #     else:
        #         raise


        # from likes table only review _id and resourceId field
        df_likes = pd.DataFrame(list(likes.find({}, {'_id': 1, 'resourceId': 1})))
        df_likes['_id'] = df_likes['_id'].astype(str)
        df_likes['resourceId'] = df_likes['resourceId'].astype(str)
        self.df_reviews.set_index('_id', inplace=True)
        df_likes.set_index('resourceId', inplace=True)
        # df_likes['_id'] = df_likes['_id'].astype(str)

        self.df_merge = self.df_reviews.join(df_likes, how='left')
        self.df_merge.dropna(inplace=True)
        self.df_merge['created_dates'] = self.df_merge['createdAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge['updated_dates'] = self.df_merge['updatedAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge['created_dates'] = pd.to_datetime(self.df_merge['created_dates'], dayfirst=True)
        self.df_merge['updated_dates'] = pd.to_datetime(self.df_merge['updated_dates'], dayfirst=True)
        self.df_merge.drop(labels=['createdAt', 'updatedAt', "_id"], inplace=True, axis=1)
        self.df_merge['resourceId'] = self.df_merge.index
        self.df_merge.reset_index(drop=True, inplace=True)
        print(self.df_merge)

    def TargetUserId(self, target_userid):
        empty_list = []
        reviews = self.mydb['reviews']
        if target_userid != None:
            targetuserid_reviewlist = pd.DataFrame(list(reviews.find({}, {'_id': 1, 'fromUserId': 1})))
            targetuserid_reviewlist['_id'] = targetuserid_reviewlist['_id'].astype(str)
            targetuserid_reviewlist['fromUserId'] = targetuserid_reviewlist['fromUserId'].astype(str)
            targetuserid_reviewlist = targetuserid_reviewlist[targetuserid_reviewlist['fromUserId'] == target_userid]
            targetuserid_reviewlist = list(targetuserid_reviewlist["_id"])
            self.targetuserid_reviewlist = [str(reviews) for reviews in targetuserid_reviewlist]
            return self.targetuserid_reviewlist
        else:
            return empty_list
        pass

    def TopicsTrending(self):
        search_history = self.mydb['searchhistories']
        df_1 = pd.DataFrame(list(search_history.find({}, {"_id":1, "searchTerm": 1})))
        sort_dict = (df_1['searchTerm'].value_counts()).to_dict()
        trending_list = list(sort_dict.keys())
        trending_list = [str(trend) for trend in trending_list]
        return trending_list

    def CategoriesTrending(self):
        reviews = self.mydb['reviews']
        likes = self.mydb['likes']
        reviews_filter = {"isApprove": 'approved', "isDeleted": False}
        df_reviews = pd.DataFrame(list(reviews.find(reviews_filter, {'_id': 1, 'updatedAt': 1, 'categoryId': 1})))
        df_likes = pd.DataFrame(list(likes.find({}, {'_id': 1, 'resourceId': 1})))
        df_reviews.set_index('_id', inplace=True)
        df_likes.set_index('resourceId', inplace=True)
        df_merge_cat = df_reviews.join(df_likes, how='left')
        df_merge_cat['updatedAt'] = df_merge_cat['updatedAt'].apply(lambda x: str(x))
        df_merge_cat['updated_dates'] = df_merge_cat['updatedAt'].apply(lambda x: str(x.split('T')[0]))
        df_merge_cat['updated_dates'] = pd.to_datetime(df_merge_cat['updated_dates'], dayfirst=True)
        categories_count_df = (df_merge_cat.groupby(['categoryId'])['updated_dates'].count().reset_index().rename(
            columns={'updated_dates': 'ReviewLikeCount'}))
        categories_count_df = (categories_count_df.sort_values(['ReviewLikeCount'], ascending=False))
        return categories_count_df['categoryId'].unique()
        pass

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

    def InsideIfFixedParameter(self, input_parameter_in_parameter_list, dataframe, actual_topreviews):
        if input_parameter_in_parameter_list:
            df_reviews = dataframe
            if len(df_reviews) == 0:
                raise KeyError
            else:
                reviewlist = list(df_reviews.index)
                actual_topreviews = list(set(reviewlist).intersection(actual_topreviews))
        else:
            raise
        return actual_topreviews
        pass

    def IfOptionalParameter(self, input_parameter_in_parameter_list, step_1, step_2, actual_topreviews):
        if input_parameter_in_parameter_list:
            ab = step_1
            df_reviews = step_2
            if len(df_reviews) == 0:
                raise KeyError
            else:
                reviewlist = list(df_reviews.index)
                actual_topreviews = list(set(reviewlist).intersection(actual_topreviews))
        else:
            raise KeyError
        return actual_topreviews
        pass

    def ReviewsResult(self, category_id, user_id, search_text, target_userid, actual_topreviews):
        if len(category_id) > 0:
            if category_id in self.category_id_list:
                df_reviews = self.df_reviews[self.df_reviews['categoryId'] == str(category_id)]
                if len(df_reviews) == 0:
                    raise KeyError
                else:
                    category_id_reviews = list(df_reviews.index)
                    actual_topreviews = list(set(category_id_reviews).intersection(actual_topreviews))
            else:
                raise
        if len(user_id) > 0:
            if user_id in self.user_id_list:
                df_reviews = self.df_reviews[~self.df_reviews['fromUserId'].isin(self.block_list)]
                if len(df_reviews) == 0:
                    raise KeyError
                else:
                    userid_reviews = list(df_reviews.index)
                    actual_topreviews = list(set(userid_reviews).intersection(actual_topreviews))
            else:
                raise
        if search_text != None:
            if [s for s in self.combine_title if all(xs in s for xs in search_text)]:
                contains = [self.df_reviews['title'].str.contains(i) for i in search_text]
                df_reviews = self.df_reviews[np.all(contains, axis=0)]
                if len(df_reviews) == 0:
                    raise KeyError
                else:
                    search_text_reviews = list(df_reviews.index)
                    actual_topreviews = list(set(search_text_reviews).intersection(actual_topreviews))
            else:
                raise KeyError
        if target_userid != None:
            if target_userid in self.user_id_list:
                df_reviews = self.df_reviews[~self.df_reviews['fromUserId'].isin(self.block_list)]
                df_reviews = df_reviews[df_reviews['fromUserId'] == target_userid]
                if len(df_reviews) == 0:
                    raise KeyError
                else:
                    targetuserid_reviewlist = list(df_reviews.index)
                    actual_topreviews = list(set(targetuserid_reviewlist).intersection(actual_topreviews))
            else:
                raise KeyError

        return actual_topreviews
        pass

    def TopReviews(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text)
        # return self.TopTrendingResults(self.df_merge, 7, 'resourceId')
        actual_topreviews = (self.TopTrendingResults(self.df_merge, 7, 'resourceId'))[:50]
        if len(category_id) > 0:
            input_parameter_i_parameter_list = category_id in self.category_id_list
            dataframe = self.df_reviews[self.df_reviews['categoryId'] == str(category_id)]
            actual_topreviews = self.InsideIfFixedParameter(input_parameter_i_parameter_list, dataframe, actual_topreviews)
            # if category_id in self.category_id_list:
            #     df_reviews = self.df_reviews[self.df_reviews['categoryId'] == str(category_id)]
            #     if len(df_reviews) == 0:
            #         raise KeyError
            #     else:
            #         category_id_reviews = list(df_reviews.index)
            #         actual_topreviews = list(set(category_id_reviews).intersection(actual_topreviews))
            # else:
            #     raise

        if len(user_id) > 0:
            input_parameter_n_parameter_list = user_id in self.user_id_list
            dataframe = self.df_reviews[~self.df_reviews['fromUserId'].isin(self.block_list)]
            actual_topreviews = self.InsideIfFixedParameter(input_parameter_n_parameter_list, dataframe, actual_topreviews)
            # if user_id in self.user_id_list:
            #     df_reviews = self.df_reviews[~self.df_reviews['fromUserId'].isin(self.block_list)]
            #     if len(df_reviews) == 0:
            #         raise KeyError
            #     else:
            #         userid_reviews = list(df_reviews.index)
            #         actual_topreviews = list(set(userid_reviews).intersection(actual_topreviews))
            # else:
            #     raise

        if search_text != None:
            input_parameter_in_parameter_lis = [s for s in self.combine_title if all(xs in s for xs in search_text)]
            step_1 = [self.df_reviews['title'].str.contains(i) for i in search_text]
            step_2 = self.df_reviews[np.all(step_1, axis=0)]
            actual_topreviews = self.IfOptionalParameter(input_parameter_in_parameter_lis, step_1, step_2, actual_topreviews)
            # if [s for s in self.combine_title if all(xs in s for xs in search_text)]:
            #     contains = [self.df_reviews['title'].str.contains(i) for i in search_text]
            #     df_reviews = self.df_reviews[np.all(contains, axis=0)]
            #     if len(df_reviews) == 0:
            #         raise KeyError
            #     else:
            #         search_text_reviews = list(df_reviews.index)
            #         actual_topreviews = list(set(search_text_reviews).intersection(actual_topreviews))
            # else:
            #     raise KeyError

        if target_userid != None:
            input_parameter_in_parameter_li = target_userid in self.user_id_list
            step_1 = self.df_reviews[~self.df_reviews['fromUserId'].isin(self.block_list)]
            step_2 = step_1[step_1['fromUserId'] == target_userid]
            actual_topreviews = self.IfOptionalParameter(input_parameter_in_parameter_li, step_1, step_2, actual_topreviews)
            # if target_userid in self.user_id_list:
            #     df_reviews = self.df_reviews[~self.df_reviews['fromUserId'].isin(self.block_list)]
            #     df_reviews = df_reviews[df_reviews['fromUserId'] == target_userid]
            #     if len(df_reviews) == 0:
            #         raise KeyError
            #     else:
            #         targetuserid_reviewlist = list(df_reviews.index)
            #         actual_topreviews = list(set(targetuserid_reviewlist).intersection(actual_topreviews))
            # else:
            #     raise KeyError
            # targetuserid_reviewlist = self.TargetUserId(target_userid)
            # # print(f'lengh of targetuser id list: {len(targetuserid_reviewlist)}')
            # if len(targetuserid_reviewlist) > 0:
            #     actual_topreviews =  list(set(targetuserid_reviewlist).intersection(actual_topreviews))
            # else:
            #     raise KeyError
        return actual_topreviews



    def TopUsers(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text)
        return self.TopTrendingResults(self.df_merge, 7, 'fromUserId')[:50]

    def PopularReviews(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text)
        # return self.TopTrendingResults(self.df_merge, 30, 'resourceId')
        all_results = self.TopTrendingResults(self.df_merge, 30, 'resourceId')[:50]
        print(all_results)
        if target_userid != None:
            targetuserid_reviewlist = self.TargetUserId(target_userid)
            # print(f'lengh of targetuser id list: {len(targetuserid_reviewlist)}')
            print(targetuserid_reviewlist)
            if len(targetuserid_reviewlist) > 0:
                return list(set(all_results).intersection(targetuserid_reviewlist))
            else:
                empty_list = []
                return empty_list
        else:
            return all_results

    def PopularUsers(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate(category_id, user_id, search_text)
        return self.TopTrendingResults(self.df_merge, 30, 'fromUserId')


@app.route('/trending-review', methods=['GET', 'POST'])
def main_1():
    category_id = request.args.get('categoryid')
    user_id = request.args.get('userid')
    search_text = request.args.get('searchtext', default = None)
    target_userid = request.args.get('targetuserid', default = None)
    if search_text:
        search_text = search_text.lower()
        search_text = list(search_text.split())
    print(f'category_id: {category_id}, user_id: {user_id}, search_text: {search_text}, target_userid: {target_userid}')
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
        search_text = list(search_text.split())
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
    print(f'user_id: {user_id}')
    if search_text:
        search_text = search_text.lower()
        search_text = list(search_text.split())
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
        search_text = list(search_text.split())
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

@app.route('/trending-category', methods=['GET', 'POST'])
def main_45():
    result = trend_results()
    category_trend = result.CategoriesTrending()
    new_dic = {}
    category_trend = list(category_trend)
    category_trend = [str(cat) for cat in category_trend]
    new_dic['category_trend_results'] = category_trend[:50]
    # print(new_dic['category_trend_results'])
    try:
        return {'category_trend_results': new_dic['category_trend_results']}
    except:
        return {'error': f'category results are not available'}

@app.route('/trending-topics', methods=['GET', 'POST'])
def main_46():
    result = trend_results()
    topics_trend = result.TopicsTrending()
    new_dic = {}
    new_dic['topics_trend_results'] = topics_trend
    # print(new_dic['category_trend_results'])
    try:
        return {'category_trend_results': new_dic['topics_trend_results'][:50]}
    except:
        return {'error': f'category results are not available'}

if __name__ == '__main__':
    app.run(debug=True, port=6050)
