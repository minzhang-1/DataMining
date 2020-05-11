# INF 553 2020S HW1
# Task 1
# command: spark-submit task1.py $ASNLIB/publicdata/review.json task1_ans $ASNLIB/publicdata/stopwords 2018 10 10
from __future__ import print_function
import os
import sys
import json
import string
from pyspark import SparkContext
sc = SparkContext('local[*]', 'DataExp')
sc.setLogLevel("ERROR")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'hw1_data')


def split_date(a):
    a = a.split('-')
    return a[0], 1


def rep_exp(a, b, c, d):
    for l in b:
        a = a.replace(l, ' ')
    for i in range(len(c)):
        a = a.replace(c[i], d[i])
    a = a.split(' ')
    return a


def main():
    result = {}

    # # For Vocareum
    # if len(sys.argv) != 7:
    #     print('Usage: task1.py review.json task1_ans stopwords y m n')
    # reviewRDD = sc.textFile(sys.argv[1])
    # idRDD = reviewRDD.map(lambda a: json.loads(a)['review_id'])
    # dateRDD = reviewRDD.map(lambda a: json.loads(a)['date'])
    # userRDD = reviewRDD.map(lambda a: json.loads(a)['user_id'])
    # textRDD = reviewRDD.map(lambda a: json.loads(a)['text'])
    # stopwordsRDD = sc.textFile(sys.argv[3])
    # y = str(sys.argv[4])
    # m = int(sys.argv[5])
    # n = int(sys.argv[6])

    # For Pycharm
    reviewRDD = sc.textFile(os.path.join(DATA_DIR, 'review.json'))
    idRDD = reviewRDD.map(lambda a: json.loads(a)['review_id'])
    dateRDD = reviewRDD.map(lambda a: json.loads(a)['date'])
    userRDD = reviewRDD.map(lambda a: json.loads(a)['user_id'])
    textRDD = reviewRDD.map(lambda a: json.loads(a)['text'])
    stopwordsRDD = sc.textFile(os.path.join(DATA_DIR, 'stopwords'))
    y = str('2018')
    m = 5
    n = 5

    countyearRDD = dateRDD.map(lambda a: split_date(a)).reduceByKey(lambda b, c: b + c).filter(
        lambda d: d[0] == y).values()

    countdistRDD = userRDD.distinct()

    countopRDD = userRDD.map(lambda a: (a, 1)).reduceByKey(lambda b, c: b + c).sortBy(lambda d: (-d[1], d[0]))

    exp_1 = ['(', '[', ',', '.', '!', '?', ':', ';', ']', ')']
    uletter = string.ascii_uppercase
    lletter = string.ascii_lowercase
    exp_2 = [''] + stopwordsRDD.collect()
    countfreqRDD = textRDD.flatMap(lambda a: rep_exp(a, exp_1, uletter, lletter)).filter(lambda b: b not in exp_2).map(lambda c: (c, 1))\
        .reduceByKey(lambda d, e: d + e).sortBy(lambda f: (-f[1], f[0])).keys()

    result['A'] = idRDD.distinct().count()
    result['B'] = (countyearRDD.collect())[0]
    result['C'] = countdistRDD.count()
    result['D'] = countopRDD.take(m)
    result['E'] = countfreqRDD.take(n)
    sc.stop()

    # # Vocareum write out
    # with open(sys.argv[2], 'w') as f:
    #     json.dump(result, f)

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