# INF 553 2020S HW3
# Task 1
# command: spark-submit task1.py $ASNLIB/publicdata/train_review.json task1.res
from __future__ import print_function
import os
import sys
import json
import time
import random
from itertools import combinations
from pyspark import SparkContext
sc = SparkContext('local[*]', 'MHLSH')
sc.setLogLevel("ERROR")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw3_data')


def min_hash(x, a, b, p, m):
    return min([((a * j + b) % p) % m for j in x])


def band_sep(x, band, row):
    return [((i, tuple(x[1][i*row:(i+1)*row])), [x[0]]) for i in range(band)]


def jaccard(x, c):
    b1 = set(c[x[0]][1])
    b2 = set(c[x[1]][1])
    j = float(len(b1 & b2)) / len(b1 | b2)
    return (x[0], x[1], j)


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 3:
    #     print('Usage: task1.py train_review.json task1.res')
    # trRDD = sc.textFile(sys.argv[1])
    # f = open(sys.argv[2], 'w')

    # For Pycharm
    trRDD = sc.textFile(os.path.join(DATA_DIR, 'train_review.json'))
    f = open('task1.res', 'w')

    # characteristic matrix
    u = trRDD.map(lambda x: str(json.loads(x)['user_id']).strip(' ')).distinct().collect()
    u_dict = {}
    for i in range(len(u)):
        u_dict[u[i]] = i
    bus = trRDD.map(lambda x: str(json.loads(x)['business_id']).strip(' ')).distinct().collect()

    bus_dict = {}
    for i in range(len(bus)):
        bus_dict[bus[i]] = i
    buRDD = trRDD.map(lambda x: (str(json.loads(x)['business_id']).strip(' '), (str(json.loads(x)['user_id']).strip(' ')))).distinct()
    chRDD = buRDD.map(lambda x: (bus_dict[x[0]], [u_dict[x[1]]])).reduceByKey(lambda y, z: y + z).sortByKey()
    c = chRDD.collect()

    # signature matrix by Min-Hash
    n = 1000
    m = len(c)
    b = 1301
    p = 24593
    random.seed(b)
    a = random.sample(range(1, 10 * n), n)
    sRDD = chRDD.map(lambda x: (x[0], [min_hash(x[1], a[i], b, p, m) for i in range(n)]))

    # candidate pair by LSH
    band = 500
    row = int(n/band)
    canRDD = sRDD.flatMap(lambda x: band_sep(x, band, row)).reduceByKey(lambda y, z: y + z).filter(lambda f: len(f[1]) > 1).flatMap(lambda g: set(combinations(g[1], 2))).distinct()
    canjRDD = canRDD.map(lambda x: jaccard(x, c)).map(lambda x: (bus[x[0]], bus[x[1]], x[2])).filter(lambda y: y[2] >= 0.05).sortBy(lambda z: z[1]).sortBy(lambda z: z[0])
    bp = canjRDD.collect()
    print(len(bp))
    # write out
    for i in range(len(bp)):
        r = {}
        r['b1'] = bp[i][0]
        r['b2'] = bp[i][1]
        r['sim'] = bp[i][2]
        json.dump(r, f)
        f.write('\n')
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()