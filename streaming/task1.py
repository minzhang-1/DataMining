# INF 553 2020S HW6
# Task 1
# Name: Min Zhang
# ID: 6882-1644-04
# command: spark-submit task1.py $ASNLIB/publicdata/business_first.json $ASNLIB/publicdata/business_second.json task1_ans.csv
from __future__ import print_function
import os
import sys
import csv
import time
import json
import random
import binascii
from pyspark import SparkContext
sc = SparkContext('local[*]', 'BF')
sc.setLogLevel("ERROR")

k = 8
n = 10000
b = 1301
p = 24593
random.seed(b)
a = random.sample(range(1, 10 * k), k)


def hash(x, a, b, p, m):
    return ((a * x + b) % p) % m


def predict(x, A):
    if x == '':
        res = 0
    else:
        x = int(binascii.hexlify(x.encode('utf8')), 16)
        y = [A[hash(x, a[i], b, p, n)] for i in range(k)]
        if sum(y) == k:
            res = 1
        else:
            res = 0
    return res


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 4:
    #     print('Usage: task1.py first.json second.json task1_ans.csv')
    # b1RDD = sc.textFile(sys.argv[1])
    # b2RDD = sc.textFile(sys.argv[2])
    # f = open(sys.argv[3], 'w')

    # For Pycharm
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'hw6_data')
    b1RDD = sc.textFile(os.path.join(DATA_DIR, 'business_first.json'))
    b2RDD = sc.textFile(os.path.join(DATA_DIR, 'business_second.json'))
    f = open('task1_ans.csv', 'w')

    b1 = b1RDD.map(lambda x: json.loads(x)['city']).filter(lambda x: x != '').distinct()\
        .map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16))\
        .flatMap(lambda x: [hash(x, a[i], b, p, n) for i in range(k)]).distinct().collect()
    A = []
    for i in range(n):
        A.append(0)
    for i in b1:
        A[i] = 1
    print(A)

    b2 = b2RDD.map(lambda x: json.loads(x)['city']).map(lambda a: predict(a, A)).collect()
    print(len(b2))

    fw = csv.writer(f, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    fw.writerow(b2)
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()
