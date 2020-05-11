# INF 553 2020S HW4
# Task 1
# command: spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 task1.py 7 $ASNLIB/publicdata/ub_sample_data.csv task1_ans
from __future__ import print_function
import sys
import time
from itertools import combinations
from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext('local[*]', 'GC')
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
from graphframes import *


def comb(a):
    b = list(combinations(a, 2))
    return [(min(i), max(i)) for i in b]


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 4:
    #     print('Usage: task1.py ub_sample_data.csv task1_ans')
    # ft = int(sys.argv[1])
    # ubRDD = sc.textFile(sys.argv[2])
    # f = open(sys.argv[3], 'w')

    # For Pycharm
    ft = int(7)
    ubRDD = sc.textFile('ub_sample_data.csv')
    f = open('task1_ans', 'w')

    name = ubRDD.first()
    ubRDD = ubRDD.filter(lambda a: a != name).distinct().map(lambda b: b.split(','))

    # graph construction
    edgeRDD = ubRDD.map(lambda a: (a[1], [a[0]])).reduceByKey(lambda a, b: a + b).flatMap(lambda a: comb(a[1]))\
        .map(lambda a: (a, 1)).reduceByKey(lambda a, b: a + b).filter(lambda a: a[1] >= ft).keys()
    vertRDD = edgeRDD.flatMap(lambda a: a).distinct().map(lambda a: (a, ))
    edgeRDD = edgeRDD.flatMap(lambda a: ((a[0], a[1]), (a[1], a[0])))

    # LPA
    vert = sqlContext.createDataFrame(vertRDD, ['id'])
    edge = sqlContext.createDataFrame(edgeRDD, ['src', 'dst'])
    g = GraphFrame(vert, edge)
    lpa = g.labelPropagation(maxIter=5)
    res = lpa.rdd.map(lambda a: (a[1], [a[0]])).reduceByKey(lambda a, b: a + b).map(lambda a: (len(a[1]), sorted(set(a[1])))).sortByKey()
    res_1 = res.filter(lambda a: a[0] == 1).sortBy(lambda a: a[1]).map(lambda a: tuple(a[1])).collect()
    res_2 = res.filter(lambda a: a[0] != 1).sortBy(lambda a: (a[0], a[1])).map(lambda a: tuple(a[1])).collect()

    for i in res_1:
        f.write(str(i)[1:(len(str(i))-2)])
        f.write('\n')
    for i in res_2:
        f.write(str(i)[1:-1])
        f.write('\n')
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()

    # # graph construction
    # edgeRDD = ubRDD.map(lambda a: (a[1], [a[0]])).reduceByKey(lambda a, b: a + b).flatMap(lambda a: comb(a[1]))\
    #     .map(lambda a: (a, 1)).reduceByKey(lambda a, b: a + b).filter(lambda a: a[1] >= ft).keys()
    # vertRDD = edgeRDD.flatMap(lambda a: a).distinct()
    # v = vertRDD.collect()
    # v_dict = {}
    # for i in range(len(v)):
    #     v_dict[v[i]] = i
    # vertRDD = vertRDD.map(lambda a: (v_dict[a], ))
    # edgeRDD = edgeRDD.flatMap(lambda a: ((v_dict[a[0]], v_dict[a[1]]), (v_dict[a[1]], v_dict[a[0]])))
    #
    # # LPA
    # vert = sqlContext.createDataFrame(vertRDD, ['id'])
    # edge = sqlContext.createDataFrame(edgeRDD, ['src', 'dst'])
    # g = GraphFrame(vert, edge)
    # lpa = g.labelPropagation(maxIter=5).rdd.map(tuple)
    # res = lpa.map(lambda a: (a[1], [v[a[0]]])).reduceByKey(lambda a, b: a + b).map(lambda a: (len(a[1]), sorted(set(a[1])))).sortByKey()
    # res_1 = res.filter(lambda a: a[0] == 1).sortBy(lambda a: a[1]).map(lambda a: tuple(a[1])).collect()
    # res_2 = res.filter(lambda a: a[0] != 1).sortBy(lambda a: (a[0], a[1])).map(lambda a: tuple(a[1])).collect()