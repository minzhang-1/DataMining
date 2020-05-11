# INF 553 2020S HW2
# Task 1
# Name: Min Zhang
# ID: 6882-1644-04
# command: spark-submit task1.py 1 4 $ASNLIB/publicdata/small1.csv task1_ans
# command: spark-submit task1.py 2 9 $ASNLIB/publicdata/small1.csv task1_ans
# command: spark-submit task1.py 1 4 $ASNLIB/publicdata/small2.csv task1_ans
# command: spark-submit task1.py 2 9 $ASNLIB/publicdata/small2.csv task1_ans
from __future__ import print_function
import os
import sys
import time
import string
from itertools import combinations
from pyspark import SparkContext
sc = SparkContext('local[*]', 'SON')
sc.setLogLevel("ERROR")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw2_data')


def aprior(chunk, sup, k, freq):
    chunk = list(chunk)
    if k == 1:
        count = {}
        for basket in chunk:
            for item in basket:
                if item not in count:
                    count[item] = 1
                else:
                    count[item] += 1
        freq = []
        for item in count:
            if count[item] >= sup:
                freq.append({item})
    if k >= 2:
        g = []
        for i in range(len(freq)):
            for j in range(i + 1, len(freq)):
                candidate = set(freq[i]) | set(freq[j])
                if len(candidate) == k:
                    if candidate not in g:
                        if k == 2:
                            g.append(candidate)
                        else:
                            candidates = list(combinations(candidate, k - 1))
                            c = 0
                            for can in candidates:
                                if tuple(sorted(can)) not in freq:
                                    c += 1
                                    break
                            if c == 0:
                                g.append(candidate)
        freq = []
        for candidate in g:
            c = 0
            for basket in chunk:
                if candidate.issubset(basket):
                    c += 1
            if c >= sup:
                freq.append(candidate)
    return [(tuple(i), 1) for i in freq]


def occur(basket, candidates):
    count = []
    for can in candidates:
        if set(can).issubset(basket):
            count.append((can, 1))
    return count


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 5:
    #     print('Usage: task1.py c s small.csv task1_ans')
    # c = int(sys.argv[1])
    # s = int(sys.argv[2])
    # smallRDD = sc.textFile(sys.argv[3])
    # f = open(sys.argv[4], 'w')

    # For Pycharm
    smallRDD = sc.textFile(os.path.join(DATA_DIR, 'small2.csv'))
    c = int(1)
    s = int(4)
    f = open(os.path.join(DATA_DIR, 'task1_ans'), 'w')

    name = smallRDD.first()
    basketRDD = smallRDD.filter(lambda a: a != name).distinct().map(lambda b: b.split(','))
    if c == 1:
        # frequent businesses
        basketRDD = basketRDD.groupByKey().map(lambda c: (c[0], list(c[1]))).values()
    if c == 2:
        # frequent users
        basketRDD = basketRDD.map(lambda c: (c[1], c[0])).groupByKey().map(lambda d: (d[0], list(d[1]))).values()

    p = basketRDD.getNumPartitions()
    k = 1
    Flag = True
    freq = []
    candidates = []
    frequent = []
    while Flag:
        # Phase 1
        can = basketRDD.mapPartitions(lambda a: aprior(a, s/p, k, sorted(freq))).distinct().keys().map(lambda e: tuple(sorted(e))).collect()
        if k == 1:
            print(can)
        # Phase 2
        freq = basketRDD.flatMap(lambda a: occur(a, sorted(can))).reduceByKey(lambda b, c: b + c).filter(lambda d: d[1] >= s).keys().map(lambda e: tuple(sorted(e))).collect()
        print(k, len(can), len(freq))
        if len(freq) == 0:
            Flag = False
        else:
            candidates.append(sorted(can))
            frequent.append(sorted(freq))
            k += 1
    f.write('Candidates:\n')
    for i in candidates:
        f.write(str(i)[1:-1].replace(',)', ')').replace('), (', '),('))
        f.write('\n\n')
    f.write('Frequent Itemsets:\n')
    for i in frequent:
        f.write(str(sorted(i))[1:-1].replace(',)', ')').replace('), (', '),('))
        f.write('\n\n')
    f.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()