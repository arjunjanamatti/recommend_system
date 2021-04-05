### LOAD DATASET

from datetime import datetime
import pandas as pd
import numpy as np
import seaborn as sns
import os
import random
import matplotlib
import matplotlib.pyplot as plt
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import mean_squared_error
import xgboost as xgb
from surprise import Reader, Dataset
from surprise import BaselineOnly
from surprise import KNNBaseline
from surprise import SVD
from surprise import  SVDpp
from surprise.model_selection import GridSearchCV

def load_data():
    try:
        df = pd.read_pickle('Data/netflix_rating.pickle')
    except Exception as e:
        netflix_csv = open('Data/netflix_rating.csv', mode='w')
        ratings_file = ['Data/combined_data_4.txt']
        for file in ratings_file:
            with open(file) as fil:
                for line in fil:
                    line = line.strip()
                    if line.endswith(':'):
                        movie_id = line.split(':')[0]
                    else:
                        row_data = [item for item in line.split(',')]
                        row_data.insert(0, movie_id)
                        netflix_csv.write(','.join(row_data))
                        netflix_csv.write('\n')
        netflix_csv.close()
        df = pd.read_csv('Data/netflix_rating.csv', sep=',', names=['movie_id', 'customer_id', 'rating', 'date'])
        df.to_pickle('Data/netflix_rating.pickle')
    return df
netflix_df = load_data()

print(netflix_df.head())

### FIND DUPLICATED RATINGS
print(netflix_df.duplicated(['movie_id', 'customer_id', 'rating', 'date']).sum())
