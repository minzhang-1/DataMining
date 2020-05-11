# INF 553 2020S HW4
# Task 2
# command: spark-submit task2.py 7 $ASNLIB/publicdata/ub_sample_data.csv task2_1_ans task2_2_ans
from __future__ import print_function
import sys
import time
from itertools import combinations
from pyspark import SparkContext
sc = SparkContext('local[*]', 'GC')
sc.setLogLevel("ERROR")


def comb(a):
    b = list(combinations(a, 2))
    return [(min(i), max(i)) for i in b]


def girvan_newman(a, ve_dict):
    # build tree by BFS: go down
    tree = [a]
    tree_rd = {a}
    tl = {}
    p_dict = {}
    weight = {}
    tl[a] = 0
    weight[a] = 1
    k = 0
    while k < len(tree):
        par = tree[k]
        chi = ve_dict[par]
        for c in chi:
            if c not in tree_rd:
                tree.append(c)
                tree_rd.add(c)
                tl[c] = tl[par] + 1
                p_dict[c] = [par]
                weight[c] = weight[par]
            else:
                if tl[c] == tl[par] + 1:
                    p_dict[c] += [par]
                    weight[c] += weight[par]
        k += 1

    # calculate betweenness: go up
    bn = []
    credit = {}
    for i in range(len(tree)-1, -1, -1):
        v = tree[i]
        if v not in credit:
            credit[v] = 1
        if v in p_dict:
            par = p_dict[v]
            for p in par:
                credit_partial = credit[v] * weight[p] / float(weight[v])
                if p not in credit:
                    credit[p] = 1
                credit[p] += credit_partial
                edge = (min(v, p), max(v, p))
                bn.append((edge, credit_partial/2))
    return bn


def modul_ori(ve, ve_dict, m):
    Q_dict = {}
    for i in ve:
        for j in ve:
            if j in ve_dict[i]:
                A = 1
            else:
                A = 0
            k1 = len(ve_dict[i])
            k2 = len(ve_dict[j])
            Q_dict[(i, j)] = (A - k1 * k2 / float(2 * m)) / float(2 * m)
    return Q_dict


def modul(community, Q_dict):
    Q = 0
    for c in community:
        v = c[1]
        l = c[0]
        if l > 1:
            for i in v:
                for j in v:
                    Q = Q + Q_dict[(i, j)]
    return Q


def build_community(ve, edges):
    community = []
    for i in ve:
        if edges[i] == []:
            community.append((1, [i]))
            edges.pop(i)
    while bool(edges):
        v = list(edges.keys())
        c = [v[0]]
        n = {v[0]}
        k = 0
        while k < len(c):
            for i in edges[c[k]]:
                if i not in n:
                    c.append(i)
                    n.add(i)
            edges.pop(c[k])
            k += 1
        community.append((len(c), sorted(c)))
    community = sorted(community, key=lambda a: (a[0], a[1]))
    return community


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 5:
    #     print('Usage: task2.py 7 ub_sample_data.csv task2_1_ans task2_2_ans')
    # ft = int(sys.argv[1])
    # ubRDD = sc.textFile(sys.argv[2])
    # f1 = open(sys.argv[3], 'w')
    # f2 = open(sys.argv[4], 'w')

    # For Pycharm
    ft = int(7)
    ubRDD = sc.textFile('ub_sample_data.csv')
    f1 = open('task2_1_ans', 'w')
    f2 = open('task2_2_ans', 'w')

    name = ubRDD.first()
    ubRDD = ubRDD.filter(lambda a: a != name).distinct().map(lambda b: b.split(','))
    edgeRDD = ubRDD.map(lambda a: (a[1], [a[0]])).reduceByKey(lambda a, b: a + b).flatMap(lambda a: comb(a[1]))\
        .map(lambda a: (a, 1)).reduceByKey(lambda a, b: a + b).filter(lambda a: a[1] >= ft).keys()\
        .flatMap(lambda a: ((a[0], [a[1]]), (a[1], [a[0]]))).reduceByKey(lambda a, b: a + b).sortBy(lambda a: a[0])
    ve_dict = edgeRDD.collectAsMap()
    ve = list(ve_dict.keys())
    bnRDD = edgeRDD.flatMap(lambda a: girvan_newman(a[0], ve_dict)).reduceByKey(lambda a, b: a + b).sortBy(lambda a: (-a[1], a[0][0], a[0][1]))
    bn = bnRDD.collect()
    for i in bn:
        f1.write(str(i)[1:(len(str(i))-2)])
        f1.write('\n')
    f1.close()

    m = len(bn)
    Q_dict = modul_ori(ve, ve_dict, m)
    k = 0
    Qm = 0
    Q = []
    com = {}
    new_bnRDD = bnRDD
    while k < m:
        bnh = new_bnRDD.map(lambda a: (a[0][0], a[0][1])).first()
        ve_dict[bnh[0]].remove(bnh[1])
        ve_dict[bnh[1]].remove(bnh[0])
        community = build_community(ve, ve_dict.copy())
        q = modul(community, Q_dict)
        new_edgeRDD = sc.parallelize(ve_dict.items())
        new_bnRDD = new_edgeRDD.flatMap(lambda a: girvan_newman(a[0], ve_dict)).reduceByKey(lambda a, b: a + b).sortBy(lambda a: (-a[1], a[0][0], a[0][1]))
        Q.append(q)
        com[k] = community
        if q >= Qm:
            Qm = q
            k += 1
        elif q < Qm and q > (Qm - 0.05):
            k += 1
        elif q < (Qm - 0.05):
            break
    for i in range(len(Q)):
        if Q[i] == Qm:
            if Q[i] >= Q[i-1] and Q[i] > Q[i+1]:
                community_p = com[i]
                break
    for i in community_p:
        f2.write(str(i[1])[1:-1])
        f2.write('\n')
    f2.close()
    sc.stop()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()