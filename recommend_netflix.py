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

### SPLIT TRAIN AND TEST DATA
split_value = int(len(netflix_df)*0.8)
train_data = netflix_df[:split_value]
test_data = netflix_df[split_value:]


### COUNT NUMBER OF RATINGS IN TRAINING DATASET
plt.figure(figsize = (12, 8))
ax = sns.countplot(x="rating", data=train_data)

ax.set_yticklabels([num for num in ax.get_yticks()])

plt.tick_params(labelsize = 15)
plt.title("Count Ratings in train data", fontsize = 20)
plt.xlabel("Ratings", fontsize = 20)
plt.ylabel("Number of Ratings", fontsize = 20)
plt.show()

### NUMBER OF RATED MOVIES PER USER
def rated_movies_groupby(variable):
    return (train_data.groupby(by=variable)['rating'].count().sort_values(ascending=False)).head()
ratings_movie_per_user = rated_movies_groupby('customer_id')
print(ratings_movie_per_user)
ratings_per_movie = rated_movies_groupby('movie_id')
print(ratings_per_movie)


### CREATE USER_ITEM SPARSE MATRIX
def get_user_item_sparse_matrix(df):
    sparse_data = sparse.csr_matrix((df['rating'], (df['customer_id'], df['movie_id'])))
    return sparse_data

train_sparse_data = get_user_item_sparse_matrix(train_data)
print(train_sparse_data)

test_sparse_data = get_user_item_sparse_matrix(test_data)
print(test_sparse_data)

### GLOBAL AVERAGE RATING
global_average_rating = train_sparse_data.sum()/train_sparse_data.count_nonzero()
print(f'Global average rating: {global_average_rating}')

def get_average_rating(sparse_matrix, is_user):
    if is_user:
        ax=1
    else:
        ax=0
    sum_of_ratings = sparse_matrix.sum(axis=ax).A1
    no_of_ratings = (sparse_matrix != 0).sum(axis=ax).A1
    rows, cols = sparse_matrix.shape
    average_ratings = {i: sum_of_ratings[i]/no_of_ratings[i] for i in range(rows if is_user else cols) if no_of_ratings[i] != 0}
    return average_ratings

average_ratings_user = get_average_rating(train_sparse_data, True)

average_rating_movie = get_average_rating(train_sparse_data, False)

### CHECK COLD START PROBLEM: USER
total_users = len(np.unique(netflix_df["customer_id"]))
train_users = len(average_ratings_user)
uncommonUsers = total_users - train_users

print("Total no. of Users = {}".format(total_users))
print("No. of Users in train data= {}".format(train_users))
print("No. of Users not present in train data = {}({}%)".format(uncommonUsers, np.round((uncommonUsers/total_users)*100), 2))

### CHECK COLD START PROBLEM: MOVIE
total_movies = len(np.unique(netflix_df["movie_id"]))
train_movies = len(average_rating_movie)
uncommonMovies = total_movies - train_movies

print("Total no. of Movies = {}".format(total_movies))
print("No. of Movies in train data= {}".format(train_movies))
print("No. of Movies not present in train data = {}({}%)".format(uncommonMovies, np.round((uncommonMovies/total_movies)*100), 2))

### COMPUTE USER SIMILARITY MATRIX
def compute_user_similarity(sparse_matrix, limit=100):
    row_index, col_index = sparse_matrix.nonzero()
    rows = np.unique(row_index)
    similar_arr = np.zeros(61700).reshape(617,100)

    for row in rows[:limit]:
        sim = cosine_similarity(sparse_matrix.getrow(row), train_sparse_data).ravel()
        similar_indices = sim.argsort()[-limit:]
        similar = sim[similar_indices]
        similar_arr[row] = similar

    return similar_arr

similar_user_matrix = compute_user_similarity(train_sparse_data, 100)
print(similar_user_matrix[0])

### LOAD MOVIES TITLE
movie_titles_df = pd.read_csv("movie_titles.txt",sep = ",",
                              header = None, names=['movie_id', 'year_of_release', 'movie_title'],
                              index_col = "movie_id", encoding = "iso8859_2")
print(movie_titles_df.head())

### COMPUTE SIMILAR MOVIES
def compute_movie_similarity_count(sparse_matrix, movie_titles_df, movie_id):
    similarity = cosine_similarity(sparse_matrix.T, dense_output = False)
    no_of_similar_movies = movie_titles_df.loc[movie_id][1], similarity[movie_id].count_nonzero()
    return no_of_similar_movies

similar_movies = compute_movie_similarity_count(train_sparse_data, movie_titles_df, 1775)
print("Similar Movies = {}".format(similar_movies))
