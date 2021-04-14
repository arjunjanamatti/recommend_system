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


cat_result, overall_result, top_review_last_week = main_result.main()
print(cat_result)
print(overall_result)
print(top_review_last_week)
