# INF 553 2020S HW3
# Task 2
# command: spark-submit task2train.py $ASNLIB/publicdata/train_review.json task2.model $ASNLIB/publicdata/stopwords
from __future__ import print_function
import os
import sys
import math
import json
import time
import string
import itertools
from pyspark import SparkContext
sc = SparkContext('local[*]', 'CBRST')
sc.setLogLevel("ERROR")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw3_data')


def rep_exp(a, b, c, d, e, f):
    for l in b:
        a = a.replace(l, '')
    for l in e:
        a = a.replace(l, '')
    for i in range(len(c)):
        a = a.replace(c[i], d[i])
    a = a.split(' ')
    g = []
    for i in a:
        if i not in f:
            if not i.isdigit():
                g.append(i)
    return g


def filt(a, word):
    return [i for i in a if i in word]


def tf(a, idf, word):
    c = {}
    for i in a:
        c[i] = c.get(i, 0) + 1
    m = max(c.values())
    d = sorted([((c[i]/m)*idf[i], word[i]) for i in c], reverse=True)[:200]
    e, f = zip(*d)
    return f


def uf(a, b):
    c = []
    for i in a:
        c += b[i]
    return list(set(c))


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 4:
    #     print('Usage: task2train.py train_review.json task2.model stopwords')
    # trRDD = sc.textFile(sys.argv[1])
    # f = open(sys.argv[2], 'w')
    # sw = sc.textFile(sys.argv[3]).collect()

    # For Pycharm
    trRDD = sc.textFile(os.path.join(DATA_DIR, 'train_review.json'))
    f = open('task2.model', 'w')
    sw = sc.textFile(os.path.join(DATA_DIR, 'stopwords')).collect()

    # PRE-PROCESS
    # remove punctuation and number
    uletter = string.ascii_uppercase
    lletter = string.ascii_lowercase
    punc = string.punctuation
    exp_1 = ['\n\n', '\n']
    exp_2 = [''] + sw
    btRDD = trRDD.map(lambda x: (json.loads(x)['business_id'], json.loads(x)['text']))\
        .reduceByKey(lambda a, b: a + b).map(lambda a: (a[0], rep_exp(a[1], exp_1, uletter, lletter, punc, exp_2)))  # total 10253 document

    hfwRDD = btRDD.flatMap(lambda a: a[1])
    threshold = (hfwRDD.count())*0.000001
    hfw = hfwRDD.map(lambda b: (b, 1)).reduceByKey(lambda c, d: c + d).filter(lambda e: e[1] >= threshold).collectAsMap()
    word = sorted(hfw.keys())
    word_dict = {}
    for i in range(len(word)):
        word_dict[word[i]] = i

    dl = btRDD.count()
    btRDD = btRDD.map(lambda a: (a[0], filt(a[1], hfw)))
    idf = btRDD.flatMap(lambda a: set(a[1])).map(lambda b: (b, 1)).reduceByKey(lambda c, d: c + d).map(lambda e: (e[0], math.log2(float(dl)/e[1]))).collectAsMap()
    itemfile = btRDD.map(lambda a: (a[0], tf(a[1], idf, word_dict))).collectAsMap()
    userfile = trRDD.map(lambda x: (json.loads(x)['user_id'], {json.loads(x)['business_id']})).reduceByKey(lambda a, b: a.union(b)).map(lambda c: (c[0], uf(c[1], itemfile))).collectAsMap()

    # write out
    result = {}
    result['itemfile'] = itemfile
    result['userfile'] = userfile
    json.dump(result, f)
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()