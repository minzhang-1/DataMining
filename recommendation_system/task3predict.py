# INF 553 2020S HW3
# Task 3
# command: spark-submit task3predict.py $ASNLIB/publicdata/train_review.json $ASNLIB/publicdata/test_review.json task3item.model task3item.predict item_based
# command: spark-submit task3predict.py $ASNLIB/publicdata/train_review.json $ASNLIB/publicdata/test_review.json task3user.model task3item.predict user_based

from __future__ import print_function
import os
import sys
import math
import json
import time
import string
from pyspark import SparkContext
sc = SparkContext('local[*]', 'RSP')
sc.setLogLevel("ERROR")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw3_data')


def pred_item_star(a, star, N):
    w = []
    s = []
    n = 0
    for i in a[1]:
        if (a[0], i[1]) in star:
            w.append(i[0])
            s.append(star[(a[0], i[1])])
            n += 1
        if n >= N:
            break
    below = sum([abs(i) for i in w])
    upper = sum([w[i]*s[i] for i in range(len(w))])
    if below == 0:
        p = -1
    else:
        p = upper/below
        if p > 5:
            p = 5
    return (a[0], p)


def pred_user_star(a, star, starall, N):
    u = a[0]
    v = a[1]
    ru = sum([i[1] for i in starall[u]]) / len(starall[u])
    w = []
    s = []
    b = []
    n = 0
    for i in v[1]:
        if (i[1], v[0]) in star:
            b.append(((i[1], v[0]), i[0]))
            w.append(i[0])
            s.append(star[(i[1], v[0])])
            n += 1
        if n >= N:
            break
    r = []
    for i in range(len(b)):
        d = starall[b[i][0][0]]
        r.append((sum([j[1] for j in d]) - s[i]) / (len(d) - 1))

    below = sum([abs(i) for i in w])
    upper = sum([w[i]*(s[i]-r[i]) for i in range(len(b))])
    if below == 0:
        p = -1
    else:
        p = ru + upper/below
        if p > 5:
            p = 5
    return (u, v[0], p)


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 6:
    #     print('Usage: task3predict.py train_review.json test_review.json task3item.model task3item.predict item_based')
    # trRDD = sc.textFile(sys.argv[1])
    # teRDD = sc.textFile(sys.argv[2])
    # mRDD = sc.textFile(sys.argv[3])
    # f = open(sys.argv[4], 'w')
    # base = str(sys.argv[5])

    # For Pycharm
    trRDD = sc.textFile(os.path.join(DATA_DIR, 'train_review.json'))
    teRDD = sc.textFile(os.path.join(DATA_DIR, 'test_review.json'))  # 58480 / 52246 / 37999
    mRDD = sc.textFile(os.path.join(BASE_DIR, 'task3user.model'))
    f = open('task3user.predict', 'w')
    base = str('user_based')

    N = 3
    if base == 'item_based':
        ubs = trRDD.map(lambda x: ((json.loads(x)['user_id'], json.loads(x)['business_id']), (json.loads(x)['stars'], 1)))\
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda c: ((c[0][0], c[0][1]), c[1][0]/c[1][1])).collectAsMap()
        mRDD = mRDD.map(lambda a: (json.loads(a)['b1'], json.loads(a)['b2'], json.loads(a)['sim']))\
            .flatMap(lambda a: [(a[0], [(a[2], a[1])]), (a[1], [(a[2], a[0])])]).reduceByKey(lambda x, y: x+y)\
            .map(lambda z: (z[0], sorted(z[1], reverse=True)))
        predict = teRDD.map(lambda a: (json.loads(a)['business_id'], json.loads(a)['user_id'])).distinct().join(mRDD)\
            .map(lambda a: (a[0], pred_item_star(a[1], ubs, N))).map(lambda b: (b[1][0], b[0], b[1][1])).filter(lambda a: a[2] > 0).collect()
    else:
        ubsRDD = trRDD.map(lambda x: ((json.loads(x)['user_id'], json.loads(x)['business_id']), (json.loads(x)['stars'], 1)))\
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda c: ((c[0][0], c[0][1]), c[1][0]/c[1][1]))
        ubs = ubsRDD.collectAsMap()
        ubas = ubsRDD.map(lambda a: (a[0][0], [(a[0][1], a[1])])).reduceByKey(lambda b, c: b + c).collectAsMap()

        mRDD = mRDD.map(lambda a: (json.loads(a)['u1'], json.loads(a)['u2'], json.loads(a)['sim']))\
            .flatMap(lambda a: [(a[0], [(a[2], a[1])]), (a[1], [(a[2], a[0])])]).reduceByKey(lambda x, y: x+y)\
            .map(lambda z: (z[0], sorted(z[1], reverse=True)))  # 6916

        predict = teRDD.map(lambda a: (json.loads(a)['user_id'], json.loads(a)['business_id'])).distinct().join(mRDD)\
            .map(lambda a: pred_user_star(a, ubs, ubas, N)).map(lambda b: (b[0], b[1], b[2])).filter(lambda a: a[2] > 0).collect()

    # write out
    for i in range(len(predict)):
        r = {}
        r['user_id'] = predict[i][0]
        r['business_id'] = predict[i][1]
        r['stars'] = predict[i][2]
        json.dump(r, f)
        f.write('\n')
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()