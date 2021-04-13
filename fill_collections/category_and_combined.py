import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
import json
import pandas as pd
from datetime import timedelta
from math import *
import random


class trend_results:
    def __init__(self, files_list):
        self.files_list = self.files_list
    pass