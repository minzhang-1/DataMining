# INF 553 2020S HW1
# Task 2
# command: spark-submit task2.py $ASNLIB/publicdata/review.json $ASNLIB/publicdata/business.json task2_no_spark_ans no_spark 20
# command: spark-submit task2.py $ASNLIB/publicdata/review.json $ASNLIB/publicdata/business.json task2_spark_ans spark 20
from __future__ import print_function
import os
import sys
import json
import time
from pyspark import SparkContext

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw1_data')


def main():
    result = {}

    # For Vocareum
    # if len(sys.argv) != 6:
    #     print('Usage: task2.py review.json business.json task2_ans if_spark n')
    # if_spark = str(sys.argv[4])
    # n = int(sys.argv[5])
    #
    # if if_spark == 'spark':
    #     time_start = time.time()
    #     sc = SparkContext('local[*]', 'DataExpMul')
    #     sc.setLogLevel("ERROR")
    #     reviewRDD = sc.textFile(sys.argv[1])
    #     bustarRDD = reviewRDD.map(lambda a: (str(json.loads(a)['business_id']).strip(' '), json.loads(a)['stars']))
    #     businessRDD = sc.textFile(sys.argv[2])
    #     buscatRDD = businessRDD.map(lambda a: (str(json.loads(a)['business_id']).strip(' '), str(json.loads(a)['categories']).split(','))) \
    #         .flatMap(lambda b: map(lambda c: (b[0], c), b[1])).map(lambda d: (d[0], d[1].strip(' ')))
    #     tRDD = buscatRDD.join(bustarRDD).values().map(lambda a: (a[0], (a[1], 1))).reduceByKey(lambda b, c: (b[0] + c[0], b[1] + c[1])).map(lambda d: (d[0], d[1][0] / d[1][1])).sortBy(lambda e: (-e[1], e[0]))
    #     result['result'] = tRDD.take(n)
    #     sc.stop()
    #     time_end = time.time()
    #     print(time_end - time_start)
    # else:
    #     time_start = time.time()
    #     bs = {}
    #     with open(sys.argv[1], 'r') as f:
    #         for line in f:
    #             try:
    #                 bs['%s' % (str(json.loads(line)['business_id']).strip(' '))].append(json.loads(line)['stars'])
    #             except KeyError:
    #                 bs['%s' % (str(json.loads(line)['business_id']).strip(' '))] = [json.loads(line)['stars']]
    #
    #     bc = {}
    #     with open(sys.argv[2], 'r') as f:
    #         for line in f:
    #             category = str(json.loads(line)['categories']).split(',')
    #             for cate in category:
    #                 try:
    #                     bc['%s' % (cate.strip(' '))].append(str(json.loads(line)['business_id']).strip(' '))
    #                 except KeyError:
    #                     bc['%s' % (cate.strip(' '))] = [str(json.loads(line)['business_id']).strip(' ')]
    #
    #     cat = []
    #     for k in bc:
    #         s = 0
    #         c = 0
    #         for j in range(len(bc[k])):
    #             if bc[k][j] in bs:
    #                 s = s + sum(bs[bc[k][j]])
    #                 c = c + len(bs[bc[k][j]])
    #         if c != 0:
    #             cat.append([k, s / c])
    #     cat = sorted(cat, key=lambda x: (-x[1], x[0]))
    #     result['result'] = cat[:n]
    # time_end = time.time()
    # print(time_end - time_start)
    # # Vocareum write out
    # with open(sys.argv[3], 'w') as f:
    #     json.dump(result, f)

    # For Pycharm
    if_spark = str('spark')
    n = 20

    if if_spark == 'spark':
        time_start = time.time()
        sc = SparkContext('local[*]', 'DataExpMul')
        sc.setLogLevel("ERROR")
        reviewRDD = sc.textFile(os.path.join(DATA_DIR, 'review.json'))
        bustarRDD = reviewRDD.map(lambda a: (str(json.loads(a)['business_id']).strip(' '), json.loads(a)['stars']))
        businessRDD = sc.textFile(os.path.join(DATA_DIR, 'business.json'))
        buscatRDD = businessRDD.map(lambda a: (str(json.loads(a)['business_id']).strip(' '), str(json.loads(a)['categories']).split(',')))\
            .flatMap(lambda b: map(lambda c: (b[0], c), b[1])).map(lambda d: (d[0], d[1].strip(' ')))
        tRDD = buscatRDD.join(bustarRDD).values().map(lambda a: (a[0], (a[1], 1))).reduceByKey(lambda b, c: (b[0] + c[0], b[1] + c[1])).map(lambda d: (d[0], d[1][0]/d[1][1])).sortBy(lambda e: [-e[1], e[0]])
        result['result'] = tRDD.take(n)
        sc.stop()
        time_end = time.time()
        print(time_end - time_start)
    else:
        time_start = time.time()
        bs = {}
        with open(os.path.join(DATA_DIR, 'review.json'), 'r') as f:
            for line in f:
                try:
                    bs['%s' % (str(json.loads(line)['business_id']).strip(' '))].append(json.loads(line)['stars'])
                except KeyError:
                    bs['%s' % (str(json.loads(line)['business_id']).strip(' '))] = [json.loads(line)['stars']]
        bc = {}
        with open(os.path.join(DATA_DIR, 'business.json'), 'r') as f:
            for line in f:
                category = str(json.loads(line)['categories']).split(',')
                for cate in category:
                    try:
                        bc['%s' % (cate.strip(' '))].append(str(json.loads(line)['business_id']).strip(' '))
                    except KeyError:
                        bc['%s' % (cate.strip(' '))] = [str(json.loads(line)['business_id']).strip(' ')]

        cat = []
        for k in bc:
            s = 0
            c = 0
            for j in range(len(bc[k])):
                if bc[k][j] in bs:
                    s = s + sum(bs[bc[k][j]])
                    c = c + len(bs[bc[k][j]])
            if c != 0:
                cat.append([k, s/c])
        cat = sorted(cat, key=lambda x: (-x[1], x[0]))
        result['result'] = cat[:n]
        time_end = time.time()
        print(time_end - time_start)
    print(result)

    # # Pycharm write out
    # with open(os.path.join(DATA_DIR, 'task2_result.json'), 'w') as f:
    #     json.dump(result, f)
    #
    # # Pycharm load in
    # with open(os.path.join(DATA_DIR, 'task2_result.json'), 'r') as f:
    #     result = json.load(f)
    # print(result)


if __name__ == '__main__':
    main()