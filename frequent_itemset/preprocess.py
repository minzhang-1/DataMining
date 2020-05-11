# INF 553 2020S HW2
# Preprocess of Task 2
# command: spark-submit preprocess.py $ASNLIB/publicdata/review.json $ASNLIB/publicdata/business.json user-business.csv
from __future__ import print_function
import os
import sys
import csv
import json
import time
from pyspark import SparkContext
sc = SparkContext('local[*]', 'Preprocess')
sc.setLogLevel("ERROR")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw2_data')


def main():
    time_start = time.time()

    # For Vocareum
    # if len(sys.argv) != 4:
    #     print('Usage: preprocess.py review.json business.json user-business.csv')
    # f = open(sys.argv[3], 'w')
    # ub = csv.writer(f, delimiter=',')
    # ub.writerow(['user_id', 'business_id'])
    # reviewRDD = sc.textFile(sys.argv[1])
    # businessRDD = sc.textFile(sys.argv[2])

    # For Pycharm
    f = open(os.path.join(DATA_DIR, 'user-business.csv'), 'w')
    ub = csv.writer(f, delimiter=',')
    ub.writerow(['user_id', 'business_id'])
    reviewRDD = sc.textFile(os.path.join(DATA_DIR, 'review.json'))
    businessRDD = sc.textFile(os.path.join(DATA_DIR, 'business.json'))

    reviewRDD = reviewRDD.map(lambda a: (json.loads(a)['business_id'], json.loads(a)['user_id']))
    businessRDD = businessRDD.map(lambda a: (json.loads(a)['business_id'], json.loads(a)['state'])).filter(lambda b: b[1] == 'NV').map(lambda c: (c[0], 1))
    busRDD = businessRDD.join(reviewRDD).map(lambda a: (a[1][1], a[0])).distinct().collect()
    for i in range(len(busRDD)):
        ub.writerow(busRDD[i])
    sc.stop()
    time_end = time.time()
    print(time_end - time_start)


if __name__ == '__main__':
    main()