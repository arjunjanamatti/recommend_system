from surprise import KNNWithMeans
from surprise import *

# To use item-based cosine similarity
sim_options = {
    "name": "cosine",
    "user_based": False,  # Compute  similarities between items
}
algo_means = KNNWithMeans(sim_options=sim_options)
algo_basic = KNNBasic(sim_options=sim_options)
algo_zscore = KNNWithZScore(sim_options=sim_options)
algo_baseline = KNNBaseline(sim_options=sim_options)
