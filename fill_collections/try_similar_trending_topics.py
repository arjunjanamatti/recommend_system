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

files_list = ['reviews_1.json','likes_1.json']

app = Flask(__name__)