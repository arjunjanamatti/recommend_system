import json
ran_list = list(range(1,100))
import pickle
from itertools import islice
from random import randint

def random_chunk(li, min_chunk=10, max_chunk=11):
    it = iter(li)
    while True:
        nxt = list(islice(it,randint(min_chunk,max_chunk)))
        if nxt:
            yield nxt
        else:
            break

with open('resourceId.pickle', 'rb') as f:
    resourceId = pickle.load(f)

review_id_tags = list(random_chunk(resourceId))
print(review_id_tags)
print(len(review_id_tags))

with open('products.json') as file:
    products = json.load(file)
products_list = []
for index, pro in enumerate(products):
    pro['review_id_tags'] = review_id_tags[index]
    products_list.append(pro)
print(products_list)