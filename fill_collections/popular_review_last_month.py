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

class popular_review_results:


    def result(self):
        _, _, top_review_last_week = main_result.main()
        return top_review_last_week

    pass

@app.route('/popular-review', methods=['GET', 'POST'])
def main():
    matching_key = request.args.get('option')
    rev = popular_review_results()
    top_review_last_week = rev.result()
    if matching_key == '':
        return {'combined': top_review_last_week['combinedResults']}
    elif matching_key != '':
        return {'combined': top_review_last_week[matching_key]}
    else:
        return {'No Result': 'please enter blank for combined result or the category number'}

if __name__ == '__main__':
    app.run(debug=True)
    # print(main())




    pass
