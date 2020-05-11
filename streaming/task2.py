# INF 553 2020S HW6
# Task 2
# command: java -cp $ASNLIB/publicdata/generate_stream.jar StreamSimulation $ASNLIB/publicdata/business.json 9999 100
# command: spark-submit task2.py 9999 task2_ans.csv
from __future__ import print_function
import os
import sys
import csv
import time
import json
import random
import binascii
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext('local[*]', 'FM')
sc.setLogLevel("ERROR")

k = 45
g = 9
n = 450
a = [random.randint(1, 100) for i in range(k)]
b = [random.randint(1, 100) for i in range(k)]


def hash(x):
    h_r = []
    for i in range(k):
        y = ((a[i] * x + b[i])) % n
        y = '{0:08b}'.format(y)
        r = 0
        for j in range(len(list(y))-1, -1, -1):
            if y[j] == '1':
                break
            else:
                r += 1
        h_r.append(r)
    return h_r


def get_median(x):
    y = sorted(x)
    n = len(x)
    ind = n // 2
    if n % 2 == 0:
        m1 = y[ind]
        m2 = y[ind - 1]
        m = (m1 + m2) / 2
    else:
        m = y[ind]
    return m


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 3:
    #     print('Usage: task2.py 9999 task2_ans.csv')
    # p = int(sys.argv[1])
    # fp = sys.argv[2]
    # f = open(fp, 'w')

    # For Pycharm
    p = 9999
    fp = 'task2_ans.csv'
    f = open(fp, 'w')

    w = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    w.writerow(['Time', 'Gound Truth', 'Estimation'])
    f.close()

    ssc = StreamingContext(sc, 5)
    ssc.checkpoint('/home/minzhang/dm/hw6/hw6_data/')
    dRDD = ssc.socketTextStream('localhost', p).window(30, 10)

    def FM(time, x):
        gt = set()
        h = []
        y = x.collect()
        for i in y:
            c = json.loads(i)['city']
            if c != '':
                gt.add(c)
                h.append(hash(int(binascii.hexlify(c.encode('utf8')), 16)))
        ht = list(zip(*h))
        hg = []
        for i in range(g):
            gl = int(k / g)
            s = [pow(2, max(ht[i * gl + j])) for j in range(gl)]
            hg.append(sum(s) / len(s))
        et = get_median(hg)
        print([str(time), str(len(gt)), str(et)])
        f = open(fp, 'a')
        w = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        w.writerow([str(time), str(len(gt)), str(et)])
        f.close()

    dRDD.foreachRDD(FM)
    ssc.start()
    ssc.awaitTermination()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()
