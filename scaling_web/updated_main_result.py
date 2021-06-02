from pymongo import MongoClient
import pandas as pd
from datetime import timedelta
from flask import Flask, request
import numpy as np
import logging, os

try:
    os.makedirs('Logs')
except:
    pass

logname = './Logs/loging.log'
logging.basicConfig(filename=logname,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

logger = logging.getLogger('urbanGUI')

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

    def MergeDataframeUpdate(self):
        reviews = self.mydb['reviews']
        likes = self.mydb['likes']
        views = self.mydb['views']
        blockusers = self.mydb['blockusers']

        ##### Blockusersdata
        cur = blockusers.find({}, {'blockUserId': 1, 'fromUserId': 1})
        block_users_dict_list = [doc for doc in cur]
        self.block_list = [([str(new['blockUserId']), str(new['fromUserId'])]) for new in block_users_dict_list]

        ##### reviews filter is set
        reviews_filter = {"isApprove": 'approved', "isDeleted": False, "categoryId": {"$exists": True}}

        self.df_reviews = pd.DataFrame(list(reviews.find(reviews_filter, {'_id': 1, "title": 1,
                                                                     'createdAt': 1, 'fromUserId': 1,
                                                                     'categoryId': 1})))
        # convert all columns to string and make title phrases lower
        self.df_reviews['_id'] = self.df_reviews['_id'].astype(str)
        self.df_reviews['title'] = self.df_reviews['title'].apply(lambda x: str(x).lower())
        self.df_reviews['createdAt'] = self.df_reviews['createdAt'].astype(str)
        # self.df_reviews['updatedAt'] = self.df_reviews['updatedAt'].astype(str)
        self.df_reviews['fromUserId'] = self.df_reviews['fromUserId'].astype(str)
        self.df_reviews['categoryId'] = self.df_reviews['categoryId'].astype(str)

        category_id_list = list(self.df_reviews['categoryId'])
        self.category_id_list = list(set(category_id_list))
        self.combine_title = list(self.df_reviews["title"])
        self.user_id_list = list(self.df_reviews['fromUserId'])
        self.user_id_list = list(set(self.user_id_list))

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
        # self.df_merge['updated_dates'] = self.df_merge['updatedAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge['created_dates'] = pd.to_datetime(self.df_merge['created_dates'], dayfirst=True)
        # self.df_merge['updated_dates'] = pd.to_datetime(self.df_merge['updated_dates'], dayfirst=True)
        self.df_merge.drop(labels=['createdAt', "_id"], inplace=True, axis=1)
        self.df_merge['resourceId'] = self.df_merge.index
        self.df_merge.reset_index(drop=True, inplace=True)
        self.df_merge.to_csv('df_merge.csv')

        # from views table only review_id and resourceId field
        df_views = pd.DataFrame(list(views.find({'resourceType' : "REVIEW"}, {'_id': 1, 'resourceId': 1, "updatedAt": 1})))
        df_views.set_index('resourceId', inplace=True)
        df_merge = self.df_merge.copy()
        df_merge.set_index('resourceId', inplace=True)

        # merge reviews and likes merged one to views one
        self.df_merge_1 = df_merge.join(df_views, how='left')
        self.df_merge_1['resourceId'] = self.df_merge_1.index
        self.df_merge_1['updatedAt'] = self.df_merge_1['updatedAt'].astype(str)
        self.df_merge_1['updated_dates'] = self.df_merge_1['updatedAt'].apply(lambda x: str(x.split('T')[0]))
        self.df_merge_1['updated_dates'] = pd.to_datetime(self.df_merge_1['updated_dates'], dayfirst=True)
        self.df_merge_1.reset_index(drop=True, inplace=True)
        self.df_merge_1.drop(labels=['updatedAt', "_id"], inplace=True, axis=1)
        self.df_merge_1.to_csv('df_merge_1.csv')

        # setting the _id column back, since we need it for ReviewsResult columnname
        self.df_reviews['_id'] = self.df_reviews.index
        self.df_reviews.reset_index(drop=True, inplace=True)

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

    def ReviewsResult(self, category_id, user_id, search_text, target_userid, actual_topreviews, columnname):
        df_reviews = self.df_reviews.copy()
        if len(category_id) > 0:
            if category_id in self.category_id_list:
                df_reviews = df_reviews[df_reviews['categoryId'] == str(category_id)]
                if len(df_reviews) == 0:
                    print('category problem')
                    raise KeyError
                else:
                    category_id_reviews = list(df_reviews[columnname])
                    actual_topreviews = list(set(category_id_reviews).intersection(actual_topreviews))
                    # print(f'category_id_reviews: {category_id_reviews}')
                    # print(f'After categoryid: {actual_topreviews}')
            else:
                raise
        if len(user_id) > 0:
            if user_id in self.user_id_list:
                blocklist = [bloc for bloc in self.block_list if user_id in bloc]
                blocklist = [(y) for x in blocklist for y in x]
                blocklist = [bloc for bloc in blocklist if bloc != user_id]
                df_reviews = df_reviews[~df_reviews['fromUserId'].isin(blocklist)]
                if len(df_reviews) == 0:
                    print('user_id problem')
                    raise KeyError
                else:
                    userid_reviews = list(df_reviews[columnname])
                    actual_topreviews = list(set(userid_reviews).intersection(actual_topreviews))
                    # print(f'userid_reviews: {userid_reviews}')
                    # print(f'After user_id: {actual_topreviews}')
            else:
                raise
        if search_text != None:
            if [s for s in self.combine_title if all(xs in s for xs in search_text)]:
                contains = [df_reviews['title'].str.contains(i) for i in search_text]
                df_reviews = df_reviews[np.all(contains, axis=0)]
                if len(df_reviews) == 0:
                    print('search text problem')
                    raise KeyError
                else:
                    search_text_reviews = list(df_reviews[columnname])
                    actual_topreviews = list(set(search_text_reviews).intersection(actual_topreviews))
                    # print(f'search_text_reviews: {search_text_reviews}')
                    # print(f'After search_text: {actual_topreviews}')
            else:
                raise KeyError
        if target_userid != None:
            if target_userid in self.user_id_list:
                # df_reviews = df_reviews[~df_reviews['fromUserId'].isin(self.block_list)]
                df_reviews = df_reviews[df_reviews['fromUserId'] == target_userid]
                if len(df_reviews) == 0:
                    print('target_userid problem')
                    raise KeyError
                else:
                    targetuserid_reviewlist = list(df_reviews[columnname])
                    actual_topreviews = list(set(targetuserid_reviewlist).intersection(actual_topreviews))
                    # print(f'targetuserid_reviewlist: {targetuserid_reviewlist}')
                    # print(f'After target_userid: {actual_topreviews}')
            else:
                raise KeyError

        return actual_topreviews
        pass


    def TopReviews(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate()
        actual_topreviews = (self.TopTrendingResults(self.df_merge_1, 7, 'resourceId'))[:50]
        actual_topreviews = self.ReviewsResult(category_id, user_id, search_text, target_userid, actual_topreviews, '_id')
        return actual_topreviews



    def TopUsers(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate()
        actual_topusers =  self.TopTrendingResults(self.df_merge_1, 7, 'fromUserId')[:50]
        actual_topusers = self.ReviewsResult(category_id, user_id, search_text, target_userid, actual_topusers,
                                               'fromUserId')
        return actual_topusers


    def PopularReviews(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate()
        actual_popularreviews = self.TopTrendingResults(self.df_merge_1, 30, 'resourceId')[:50]
        actual_popularreviews = self.ReviewsResult(category_id, user_id, search_text, target_userid, actual_popularreviews, '_id')
        return actual_popularreviews


    def PopularUsers(self, category_id, user_id, search_text, target_userid):
        self.MergeDataframeUpdate()
        actual_popularusers = self.TopTrendingResults(self.df_merge_1, 30, 'fromUserId')
        actual_popularusers = self.ReviewsResult(category_id, user_id, search_text, target_userid, actual_popularusers,
                                               'fromUserId')
        return actual_popularusers


@app.route('/trending-review', methods=['GET', 'POST'])
def main_1():
    category_id = request.args.get('categoryid')
    user_id = request.args.get('userid')
    search_text = request.args.get('searchtext', default = None)
    target_userid = request.args.get('targetuserid', default = None)
    if search_text:
        search_text = search_text.lower()
        search_text = list(search_text.split())
    try:
        result = trend_results()
        top_review_last_week = result.TopReviews(category_id, user_id, search_text, target_userid)
        top_review_last_week = [str(top) for top in top_review_last_week][:50]
        if category_id == '':
            return {'combined': top_review_last_week}
        elif category_id != '':
            try:
                return {'combined': top_review_last_week}
            except:
                return {'combined': f'This category {category_id} has no results'}
        app.logger.info(f'category_id: {category_id}, user_id: {user_id}, search_text: {search_text} & taret_userid: {target_userid} are valid!!!')
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # print(f'Exception: {type(e).__name__}')
        app.logger.info(f'Exception: {type(e).__name__} is there')
        # logging.info(f'Exception: {type(e).__name__} has occured for Trending reviews with category_id: {category_id}, user_id: {user_id}, search_text: {search_text}, target_userid:{target_userid}')
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
                return {'combined': top_user_last_week}
            except:
                return {'combined': f'This category {category_id} has no results'}
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # logging.info(f'Exception: {type(e).__name__} has occured for Trending users with category_id: {category_id}, user_id: {user_id}, search_text: {search_text}, target_userid:{target_userid}')
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
                return {'combined': popular_review_last_month}
            except Exception as e:
                print(f'Exception: {e}')
                return {'combined': f'This category {category_id} has no results'}
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # logging.info(f'Exception: {type(e).__name__} has occured for Popular reviews with category_id: {category_id}, user_id: {user_id}, search_text: {search_text}, target_userid:{target_userid}')
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
                return {'combined': f'This category {category_id} has no results'}
    except KeyError:
        return {'empty_result': []}
    except Exception as e:
        # logging.info(f'Exception: {type(e).__name__} has occured for Popular users with category_id: {category_id}, user_id: {user_id}, search_text: {search_text}, target_userid:{target_userid}')
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
    except Exception as e:
        # logging.info(f'Trending category results not available due to {type(e).__name__}')
        return {'error': f'category results are not available'}

@app.route('/trending-topics', methods=['GET', 'POST'])
def main_46():
    result = trend_results()
    topics_trend = result.TopicsTrending()
    new_dic = {}
    new_dic['topics_trend_results'] = topics_trend
    # print(new_dic['category_trend_results'])
    try:
        return {'topics_trend_results': new_dic['topics_trend_results'][:50]}
    except Exception as e:
        # logging.info(f'Trending topics results not available due to {type(e).__name__}')
        return {'error': f'topic results are not available'}

if __name__ == '__main__':
    # result = trend_results()
    # top_reviews = result.TopReviews(category_id='', user_id='', search_text = None, target_userid = None)
    # print(top_reviews)
    app.run(debug=True, port=5000)
