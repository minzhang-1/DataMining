# INF 553 2020S HW2
# Task 2
# command: spark-submit task2.py 70 50 user-business.csv task2_ans
from __future__ import print_function
import os
import sys
import time
import string
from itertools import combinations
from pyspark import SparkContext
sc = SparkContext('local[*]', 'SON_LARGER')
sc.setLogLevel("ERROR")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw2_data')


def counting(g, chunk):
    counter = {}
    for c in g:
        _c = set(c)
        for basket in chunk:
            if _c.issubset(basket):
                counter[c] = counter.get(c, 0) + 1
    return counter


def thres(count, sup):
    freq = set()
    for item in count:
        if count[item] >= sup:
            freq.add(item)
    return freq


def aprior(chunk, sup, k, freq):
    chunk = tuple(chunk)
    count = {}

    if k == 1:
        for basket in chunk:
            for item in basket:
                if item not in count:
                    count[item] = 1
                else:
                    count[item] += 1
    if k == 2:
        freq_new = set()
        for item in freq:
            for i in item:
                freq_new.add(i)
        g = list(combinations(freq_new, k))
        count = counting(g, chunk)
    if k > 2:
        g = []
        for i in range(len(freq)):
            for j in range(i + 1, len(freq)):
                candidate = set(freq[i]) | set(freq[j])
                if len(candidate) == k:
                    if tuple(sorted(candidate)) not in g:
                        candidates = list(combinations(candidate, k - 1))
                        c = 0
                        for can in candidates:
                            if tuple(sorted(can)) not in freq:
                                c += 1
                                break
                        if c == 0:
                            g.append(tuple(sorted(candidate)))
        count = counting(g, chunk)
    freq = thres(count, sup)
    return freq


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
    #     print('Usage: task2.py ft s user-business.csv task2_ans')
    # ft = int(sys.argv[1])
    # s = int(sys.argv[2])
    # ubRDD = sc.textFile(sys.argv[3])
    # f = open(sys.argv[4], 'w')

    # For Pycharm
    ubRDD = sc.textFile(os.path.join(DATA_DIR, 'user-business.csv'))
    ft = int(70)
    s = int(50)
    f = open(os.path.join(DATA_DIR, 'task2_ans'), 'w')

    name = ubRDD.first()
    basketRDD = ubRDD.filter(lambda a: a != name).distinct().map(lambda b: b.split(',')).groupByKey().map(lambda c: (c[0], set(c[1])))
    basketRDD = basketRDD.filter(lambda a: len(a[1]) > ft).values()
    p = basketRDD.getNumPartitions()
    k = 1
    Flag = True
    freq_t = []
    candidates = []
    frequent = []
    while Flag:
        # Phase 1
        p = basketRDD.getNumPartitions()
        can = basketRDD.mapPartitions(lambda a: aprior(a, int(s/p), k, freq_t))
        if k == 1:
            can = can.map(lambda b: (b, )).distinct().map(lambda e: tuple(sorted(e))).collect()
        else:
            can = can.map(lambda b: (b)).distinct().map(lambda e: tuple(sorted(e))).collect()
        can = sorted(can)
        # Phase 2
        freq_t = basketRDD.flatMap(lambda a: occur(a, can)).reduceByKey(lambda b, c: b + c).filter(lambda d: d[1] >= s).keys().map(lambda e: tuple(sorted(e))).collect()
        freq_t = sorted(freq_t)
        if len(freq_t) == 0:
            Flag = False
        else:
            candidates.append(can)
            frequent.append(freq_t)
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