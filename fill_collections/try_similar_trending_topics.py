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
import numpy as np
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
        myclient = MongoClient(host='localhost', port=27017)
        mydb = myclient['real_reviews']
        # list_1 = self.looping_json_files()
        # self.files_list = ['reviews.json', 'likes.json']
        self.files_list = ['reviews_2.json', 'likes_2.json']
        self.tables_dictionary = {}
        for index, file in enumerate(self.files_list):
            my_collection = mydb[file.split('.')[0]]
            list_data = my_collection.find()
            df = pd.DataFrame(list(list_data))
            self.tables_dictionary[file.split('.')[0]] = df
        return self.tables_dictionary


    def TopicsTrending(self):
        myclient = MongoClient(host='localhost', port=27017)
        mydb = myclient['realreview']
        # list_1 = self.looping_json_files()
        # self.files_list = ['reviews.json', 'likes.json']
        files_list = ['searchhistories.json']
        tables_dictionary = {}
        for index, file in enumerate(files_list):
            my_collection = mydb[file.split('.')[0]]
            list_data = my_collection.find()
            df = pd.DataFrame(list(list_data))
            tables_dictionary[file.split('.')[0]] = df
        # transform the reviews_1 table to df_1 dataframe
        df_1 = tables_dictionary[files_list[0].split('.')[0]]
        sort_dict = (df_1['resourceId'].value_counts()).to_dict()
        trending_list = list(sort_dict.keys())
        trending_list = [str(trend) for trend in trending_list]
        return trending_list

@app.route('/trending-topics', methods=['GET', 'POST'])
def main_45():
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
    app.run(debug=True, port=7000)
    # result = trend_results()
    # df = result.MergedDataframe()
    # print(df.columns)
    # print()
    # print(df['resourceId'].value_counts()[:5])
    # print()
    # try_dict = (df['resourceId'].value_counts()).to_dict()
    # print(try_dict)





