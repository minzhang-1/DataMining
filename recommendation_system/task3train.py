# INF 553 2020S HW3
# Task 3
# Name: Min Zhang
# ID: 6882-1644-04
# command: spark-submit task3train.py $ASNLIB/publicdata/train_review.json task3item.model item_based
# command: spark-submit task3train.py $ASNLIB/publicdata/train_review.json task3user.model user_based
from __future__ import print_function
import os
import sys
import math
import json
import time
import random
from itertools import combinations
from pyspark import SparkContext
sc = SparkContext('local[*]', 'RST')
sc.setLogLevel("ERROR")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw3_data')


def gen_pair(a):
    b = {}
    for i in a:
        try:
            b[i[0]].append(i[1])
        except KeyError:
            b[i[0]] = [i[1]]
    c = list(combinations(b.keys(), 2))
    d = []
    for i in c:
        n = min(i)
        m = max(i)
        d.append(((n, m), [(sum(b[n])/len(b[n]), sum(b[m])/len(b[m]))]))
    return d


def gen_pair_user(a, ubs):
    b1 = {}
    for i in ubs[a[0]]:
        try:
            b1[i[0]].append(i[1])
        except KeyError:
            b1[i[0]] = [i[1]]
    b2 = {}
    for i in ubs[a[1]]:
        try:
            b2[i[0]].append(i[1])
        except KeyError:
            b2[i[0]] = [i[1]]

    u = set(b1.keys()) & set(b2.keys())
    if len(u) < 3:
        w = -1
    else:
        s1 = [sum(b1[i])/len(b1[i]) for i in u]
        s2 = [sum(b2[i])/len(b2[i]) for i in u]
        ms1 = float(sum(s1)) / len(s1)
        ms2 = float(sum(s2)) / len(s2)
        c = sum([(s1[i] - ms1) * (s2[i] - ms2) for i in range(len(u))])
        d1 = pow(sum([pow((i - ms1), 2) for i in s1]), 0.5)
        d2 = pow(sum([pow((i - ms2), 2) for i in s2]), 0.5)
        if (d1 * d2) == 0:
            w = -1
        else:
            w = float(c) / (d1 * d2)
    return w


def pearson_sim(a):
    b1 = [i[0] for i in a]
    b2 = [i[1] for i in a]
    mb1 = float(sum(b1)) / len(b1)
    mb2 = float(sum(b2)) / len(b2)
    c = sum([(b1[i] - mb1) * (b2[i] - mb2) for i in range(len(a))])
    d1 = pow(sum([pow((i - mb1), 2) for i in b1]), 0.5)
    d2 = pow(sum([pow((i - mb2), 2) for i in b2]), 0.5)
    if (d1 * d2) == 0:
        w = -1
    else:
        w = float(c) / (d1 * d2)
    return w


def min_hash(x, a, b, p, m):
    return min([((a * j + b) % p) % m for j in x])


def band_sep(x, band, row):
    return [((i, tuple(x[1][i*row:(i+1)*row])), [x[0]]) for i in range(band)]


def comb(a):
    b = list(combinations(a, 2))
    return [(min(i), max(i)) for i in b]


def jaccard(x, c):
    b1 = set(c[x[0]][1])
    b2 = set(c[x[1]][1])
    j = float(len(b1 & b2)) / len(b1 | b2)
    return (x[0], x[1], j)


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 4:
    #     print('Usage: task3train.py train_review.json task3item.model item_based')
    # trRDD = sc.textFile(sys.argv[1])
    # f = open(sys.argv[2], 'w')
    # base = str(sys.argv[3])

    # For Pycharm
    trRDD = sc.textFile(os.path.join(DATA_DIR, 'train_review.json'))
    f = open('task3user.model', 'w')
    base = str('user_based')

    u = trRDD.map(lambda x: str(json.loads(x)['user_id']).strip(' ')).distinct().collect()
    u_dict = {}
    for i in range(len(u)):
        u_dict[u[i]] = i

    bus = trRDD.map(lambda x: str(json.loads(x)['business_id']).strip(' ')).distinct().collect()
    bus_dict = {}
    for i in range(len(bus)):
        bus_dict[bus[i]] = i

    if base == 'item_based':
        model = trRDD.map(lambda x: (json.loads(x)['user_id'], [(bus_dict[json.loads(x)['business_id']], json.loads(x)['stars'])]))\
            .reduceByKey(lambda d, e: d + e).flatMap(lambda a: gen_pair(a[1]))\
            .reduceByKey(lambda g, h: g + h).filter(lambda a: len(a[1]) >= 3)\
            .map(lambda a: (bus[a[0][0]], bus[a[0][1]], pearson_sim(a[1]))).filter(lambda b: b[2] > 0).collect()
        # write out
        for i in range(len(model)):
            r = {}
            r['b1'] = model[i][0]
            r['b2'] = model[i][1]
            r['sim'] = model[i][2]
            json.dump(r, f)
            f.write('\n')
    else:
        chRDD = trRDD.map(lambda x: (json.loads(x)['user_id'], json.loads(x)['business_id'])).distinct()\
            .map(lambda x: (u_dict[x[0]], [bus_dict[x[1]]])).reduceByKey(lambda y, z: y + z).sortByKey()
        c = chRDD.collect()
        n = 20
        m = len(c)
        b = 15
        p = 29
        random.seed(b)
        a = random.sample(range(1, 10 * n), n)
        band = 5
        row = int(n / band)
        canRDD = chRDD.map(lambda x: (x[0], [min_hash(x[1], a[i], b, p, m) for i in range(n)]))\
            .flatMap(lambda x: band_sep(x, band, row)).reduceByKey(lambda y, z: y + z)\
            .filter(lambda f: len(f[1]) > 1).flatMap(lambda g: comb(g[1])).distinct()\
            .map(lambda x: jaccard(x, c)).filter(lambda y: y[2] >= 0.01).map(lambda y: (y[0], y[1]))
        ubs = trRDD.map(lambda x: (u_dict[json.loads(x)['user_id']], [(bus_dict[json.loads(x)['business_id']], json.loads(x)['stars'])]))\
            .reduceByKey(lambda d, e: d + e).collectAsMap()
        model = canRDD.map(lambda a: (u[a[0]], u[a[1]], gen_pair_user(a, ubs))).filter(lambda b: b[2] > 0).collect() # 368404
        print(len(model))

        # # method 3
        # model = trRDD.map(lambda x: (json.loads(x)['business_id'], [(u_dict[json.loads(x)['user_id']], json.loads(x)['stars'])]))\
        #     .reduceByKey(lambda d, e: d + e).flatMap(lambda a: gen_pair(a[1]))\
        #     .reduceByKey(lambda g, h: g + h).filter(lambda a: len(a[1]) >= 3)\
        #     .map(lambda a: (u[a[0][0]], u[a[0][1]], pearson_sim(a[1]))).filter(lambda b: b[2] > 0).collect()  # 859459
        # write out
        for i in range(len(model)):
            r = {}
            r['u1'] = model[i][0]
            r['u2'] = model[i][1]
            r['sim'] = model[i][2]
            json.dump(r, f)
            f.write('\n')
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()