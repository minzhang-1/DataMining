# INF 553 2020S HW3
# Task 2
# command: spark-submit task2predict.py $ASNLIB/publicdata/test_review.json task2.model task2.predict

from __future__ import print_function
import os
import sys
import math
import json
import time
import string
from pyspark import SparkContext
sc = SparkContext('local[*]', 'CBRSP')
sc.setLogLevel("ERROR")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw3_data')


def cosim(a, userfile, itemfile):
    try:
        b = userfile[a[0]]
        c = itemfile[a[1]]
        s = len(set(b) & set(c)) / math.sqrt(len(b) * len(c))
    except KeyError:
        s = 0
    return (a[0], a[1], s)


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 4:
    #     print('Usage: task2predict.py test_review.json task2.model task2.predict')
    # trRDD = sc.textFile(sys.argv[1])
    # model = open(sys.argv[2], 'r')
    # f = open(sys.argv[3], 'w')

    # For Pycharm
    trRDD = sc.textFile(os.path.join(DATA_DIR, 'test_review.json'))
    model = open(os.path.join(BASE_DIR, 'task2.model'), 'r')
    f = open('task2.predict', 'w')

    result = json.load(model)
    itemfile = result['itemfile']
    userfile = result['userfile']
    ubRDD = trRDD.map(lambda a: (json.loads(a)['user_id'], json.loads(a)['business_id'])).distinct()
    sim = ubRDD.map(lambda a: cosim(a, userfile, itemfile)).filter(lambda b: b[2] >= 0.01).collect()

    # write out
    for i in range(len(sim)):
        r = {}
        r['user_id'] = sim[i][0]
        r['business_id'] = sim[i][1]
        r['sim'] = sim[i][2]
        json.dump(r, f)
        f.write('\n')
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()