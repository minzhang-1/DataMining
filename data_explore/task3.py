# INF 553 2020S HW1
# Task 3
# command: spark-submit task3.py $ASNLIB/publicdata/review.json task3_default_ans default 20 50
# command: spark-submit task3.py $ASNLIB/publicdata/review.json task3_customized_ans customized 20 50
from __future__ import print_function
import os
import sys
import json
import time
from pyspark import SparkContext
sc = SparkContext('local[*]', 'DataExpPar')
sc.setLogLevel("ERROR")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw1_data')


def CustomPartitioner(key):
    return hash(key)


def main():
    result = {}

    # For Vocareum
    # if len(sys.argv) != 6:
    #     print('Usage: task3.py review.json task3_ans if_default n_partition n')
    # if_default = str(sys.argv[3])
    # n_partitions = int(sys.argv[4])
    # n = int(sys.argv[5])
    #
    # time_start = time.time()
    # reviewRDD = sc.textFile(sys.argv[1])
    # busRDD = reviewRDD.map(lambda a: (str(json.loads(a)['business_id']).strip(' '), 1))
    # if if_default == 'customized':
    #     busRDD = busRDD.partitionBy(n_partitions, lambda a: hash(a[0]))
    # busrRDD = busRDD.reduceByKey(lambda b, c: b + c).filter(lambda d: d[1] > n)
    # result['n_partitions'] = busRDD.getNumPartitions()
    # result['n_items'] = (busRDD.mapPartitions(lambda a: [sum(1 for _ in a)])).collect()
    # result['result'] = busrRDD.collect()
    # sc.stop()
    # time_end = time.time()
    # print(time_end - time_start)

    # # Vocareum write out
    # with open(sys.argv[2], 'w') as f:
    #     json.dump(result, f)

    # For Pycharm
    if_default = str('customized')
    n_partitions = 20
    n = 50
    time_start = time.time()
    reviewRDD = sc.textFile(os.path.join(DATA_DIR, 'review.json'))
    busRDD = reviewRDD.map(lambda a: (str(json.loads(a)['business_id']).strip(' '), 1))
    if if_default == 'customized':
        busRDD = busRDD.partitionBy(n_partitions, CustomPartitioner)
    busrRDD = busRDD.reduceByKey(lambda b, c: b + c).filter(lambda d: d[1] > n)
    result['n_partitions'] = busRDD.getNumPartitions()
    result['n_items'] = busRDD.glom().map(len).collect()
    result['result'] = busrRDD.collect()
    sc.stop()
    time_end = time.time()
    print(time_end - time_start)
    print(result)

    # # Pycharm write out
    # with open(os.path.join(DATA_DIR, 'task1_result.json'), 'w') as f:
    #     json.dump(result, f)
    #
    # # Pycharm load in
    # with open(os.path.join(DATA_DIR, 'task1_result.json'), 'r') as f:
    #     result = json.load(f)
    # print(result)


if __name__ == '__main__':
    main()