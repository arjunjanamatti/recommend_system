
ran_list = list(range(1,100))

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

a = (list(random_chunk(ran_list)))

print(a)
print(len(a))
