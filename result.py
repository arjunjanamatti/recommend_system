from load_data import data
from recommender import algo_basic, algo_means, algo_zscore, algo_baseline

train_data_set = data.build_full_trainset()

def get_result(algo):
    algo.fit(trainset = train_data_set)

    predict = algo.predict('E', 2)
    return predict.est

print('Result using KNN basic: {}'.format(get_result(algo_basic)))
print()
print('Result using KNN means: {}'.format(get_result(algo_means)))
print()
print('Result using KNN zscore: {}'.format(get_result(algo_zscore)))
print()
print('Result using KNN baseline: {}'.format(get_result(algo_baseline)))
