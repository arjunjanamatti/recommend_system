import main_result
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

class top_popular_results:


    def result(self):
        top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month = main_result.main()
        return top_review_last_week, top_user_last_week, popular_review_last_month, popular_user_last_month

    pass

@app.route('/top-review', methods=['GET', 'POST'])
def main():
    matching_key = request.args.get('option')
    rev = top_popular_results()
    top_review_last_week, _, _, _ = rev.result()
    if matching_key == '':
        return {'combined': top_review_last_week['combinedResults']}
    elif matching_key != '':
        return {'combined': top_review_last_week[matching_key]}
    else:
        return {'No Result': 'please enter blank for combined result or the category number'}

@app.route('/top-user', methods=['GET', 'POST'])
def main_1():
    matching_key = request.args.get('option')
    rev = top_popular_results()
    _, top_user_last_week, _, _ = rev.result()
    if matching_key == '':
        return {'combined': top_user_last_week['combinedResults']}
    elif matching_key != '':
        return {'combined': top_user_last_week[matching_key]}
    else:
        return {'No Result': 'please enter blank for combined result or the category number'}

@app.route('/popular-review', methods=['GET', 'POST'])
def main_2():
    matching_key = request.args.get('option')
    rev = top_popular_results()
    _, _, popular_review_last_month, _ = rev.result()
    if matching_key == '':
        return {'combined': popular_review_last_month['combinedResults']}
    elif matching_key != '':
        return {'combined': popular_review_last_month[matching_key]}
    else:
        return {'No Result': 'please enter blank for combined result or the category number'}

@app.route('/popular-user', methods=['GET', 'POST'])
def main_3():
    matching_key = request.args.get('option')
    rev = top_popular_results()
    _, _, _, popular_user_last_month = rev.result()
    if matching_key == '':
        return {'combined': popular_user_last_month['combinedResults']}
    elif matching_key != '':
        return {'combined': popular_user_last_month[matching_key]}
    else:
        return {'No Result': 'please enter blank for combined result or the category number'}

if __name__ == '__main__':
    app.run(debug=True)




