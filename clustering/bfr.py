# INF 553 2020S HW5
# Task 1
# command: python3 bfr.py $ASNLIB/publicdata/test1/ 10 cluster_res1.json intermediate1.csv
# command: python3 bfr.py $ASNLIB/publicdata/test2/ 10 cluster_res2.json intermediate2.csv
# command: python3 bfr.py $ASNLIB/publicdata/test3/ 5 cluster_res3.json intermediate3.csv
# command: python3 bfr.py $ASNLIB/publicdata/test4/ 8 cluster_res4.json intermediate4.csv
# command: python3 bfr.py $ASNLIB/publicdata/test5/ 15 cluster_res5.json intermediate5.csv

from __future__ import print_function
import os
import sys
import csv
import json
import time
import random
from sklearn.cluster import KMeans
from sklearn.metrics.cluster import normalized_mutual_info_score as nmi


def data_preprocess(a):
    b = {}
    for i in a:
        i = i.replace('\n', '').split(',')
        for j in i:
            if j != i[0]:
                try:
                    b[int(i[0])].append(float(j))
                except KeyError:
                    b[int(i[0])] = [float(j)]
    return b


def kmean_initialize(sample, n_cluster, d):
    centroids = []
    centroids.append(random.choice(sample))

    for k in range(n_cluster-1):
        ds = []
        for i in range(len(sample)):
            dis = []
            for j in range(len(centroids)):
                dis.append(sum([pow(sample[i][t] - centroids[j][t], 2) for t in range(d)]))
            ds.append(min(dis))
        ind = ds.index(max(ds))
        centroids.append(sample[ind])
    return centroids


def kmean(sample, n_cluster):
    # kmeans = KMeans(n_clusters=n_cluster, random_state=0, max_iter=100).fit(sample)
    # label = kmeans.labels_
    d = len(sample[0])
    centroids = kmean_initialize(sample, n_cluster, d)
    k = 0
    flag = True
    while flag:
        label = []
        for i in range(len(sample)):
            dis = []
            for j in range(n_cluster):
                dis.append(sum([pow(sample[i][t] - centroids[j][t], 2) for t in range(d)]))
            ind = dis.index(min(dis))
            label.append(ind)
        centroids_new = []
        for i in range(n_cluster):
            sample_c = [sample[j] for j in range(len(sample)) if label[j] == i]
            sample_c_t = list(zip(*sample_c))
            centroids_new.append([sum(sample_c_t[j])/float(len(sample_c)) for j in range(d)])
        if k > 100 or centroids_new == centroids:
            flag = False
        else:
            centroids = centroids_new
            k += 1
    return label


def initialize(data, n_cluster, n_total, d):
    cls_label = {}

    # Intialize by running k-means on a small random number of data points
    n_sample = int(n_total * 0.2)
    ind = random.sample(range(1, n_total), n_sample)
    sample_index = {}
    for i in range(n_sample):
        sample_index[i] = ind[i]
    n_sample_new = n_sample
    n_outlier = 0
    flag = True
    while flag:
        sample = [data[sample_index[i]] for i in sample_index]
        label = kmean(sample, n_cluster)
        for i in range(n_cluster):
            c = [j for j in range(n_sample_new) if label[j] == i]
            if len(c) == 1:
                data.pop(sample_index[c[0]])
                cls_label[sample_index[c[0]]] = -1
                sample_index.pop(c[0])
                n_outlier += 1
        if len(sample_index) == n_sample_new:
            index = [sample_index[j] for j in sample_index]
            n_sample = n_sample_new
            flag = False
        else:
            n_sample_new = len(sample_index)

    stat = {}
    stat['outlier'] = n_outlier
    stat['DS_CLS'] = n_cluster
    for i in range(n_cluster):
        cls = []
        for j in range(n_sample):
            if label[j] == i:
                cls.append(sample[j])
                cls_label[index[j]] = i
        cls = [sample[j] for j in range(n_sample) if label[j] == i]
        N = len(cls)
        cls_t = list(zip(*cls))
        stat['DS_N_%d' % i] = N
        stat['DS_SUM_%d' % i] = [sum(cls_t[j]) for j in range(d)]
        stat['DS_SUMSQ_%d' % i] = [sum([pow(cls_t[j][t], 2) for t in range(N)]) for j in range(d)]

    for i in index:
        data.pop(i)
    return stat, data, cls_label


def cal_maha(sample, stat, d, SET):
    md = []
    n_cluster = stat[SET+'_CLS']
    for i in range(n_cluster):
        N = stat[SET+'_N_%d' % i]
        SUM = stat[SET+'_SUM_%d' % i]
        SUMSQ = stat[SET+'_SUMSQ_%d' % i]
        std = [pow((SUMSQ[j] / float(N) - pow(SUM[j] / float(N), 2)), 0.5) for j in range(d)]
        me = [SUM[j] / float(N) for j in range(d)]
        md_t = 0
        for t in range(d):
            if std[t] == 0:
                md_t += pow((sample[t] - me[t]), 2)
            else:
                md_t += pow((sample[t] - me[t]) / float(std[t]), 2)
        md.append(pow(md_t, 0.5))
    return md


def update(stat, i, cls, SET, d):
    N_p = stat[SET+'_N_%d' % i]
    SUM_p = stat[SET+'_SUM_%d' % i]
    SUMSQ_p = stat[SET+'_SUMSQ_%d' % i]
    stat[SET+'_N_%d' % i] = 1 + N_p
    stat[SET+'_SUM_%d' % i] = [cls[j]+SUM_p[j] for j in range(d)]
    stat[SET+'_SUMSQ_%d' % i] = [pow(cls[j], 2) + SUMSQ_p[j] for j in range(d)]
    return stat


def decide_newpoint(stat, cls_label, cs_idx, data, n_cluster, alpha, d, K):
    ot = []
    for i in data:
        # DS
        md_ds = cal_maha(data[i], stat, d, 'DS')
        min_md_ds = min(md_ds)
        ind = md_ds.index(min_md_ds)
        if min_md_ds < alpha * pow(d, 0.5):
            stat = update(stat, ind, data[i], 'DS', d)
            cls_label[i] = ind
        else:
            ot.append(i)

    if bool(ot):
        if 'RS_CLS' not in stat:
            r = 0
        else:
            r = stat['RS_CLS']

        if 'CS_CLS' not in stat:
            if len(ot) > K * n_cluster:
                data_ot = [data[i] for i in ot]
                label = kmean(data_ot, K * n_cluster)
                c = 0
                for i in range(K * n_cluster):
                    idx = [j for j in range(len(ot)) if label[j] == i]
                    N = len(idx)
                    if N > 1:
                        cls = [data_ot[j] for j in idx]
                        cls_t = list(zip(*cls))
                        stat['CS_N_%d' % c] = N
                        stat['CS_SUM_%d' % c] = [sum(cls_t[j]) for j in range(d)]
                        stat['CS_SUMSQ_%d' % c] = [sum([pow(cls_t[j][t], 2) for t in range(N)]) for j in range(d)]
                        cs_idx[c] = [ot[j] for j in idx]
                        c += 1
                    else:
                        stat['RS_%d' % r] = (data_ot[idx[0]], ot[idx[0]])
                        r += 1
                if c != 0:
                    stat['CS_CLS'] = c
            else:
                for i in ot:
                    stat['RS_%d' % r] = (data[i], i)
                    r += 1
            if r != 0:
                stat['RS_CLS'] = r
        else:
            # CS
            for i in ot:
                md_cs = cal_maha(data[i], stat, d, 'CS')
                min_md_cs = min(md_cs)
                ind = md_cs.index(min_md_cs)
                if min_md_cs < alpha * pow(d, 0.5):
                    stat = update(stat, ind, data[i], 'CS', d)
                    cs_idx[ind].append(i)
                else:
                    stat['RS_%d' % r] = (data[i], i)
                    r += 1
            if r != 0:
                stat['RS_CLS'] = r
    return stat, cls_label, cs_idx


def merge_rs(stat, cs_idx, n_cluster, K, d):
    r = stat['RS_CLS']
    stat.pop('RS_CLS')
    RS_set = []
    rs_idx = []
    for i in range(r):
        RS_set.append((stat['RS_%d' % i])[0])
        rs_idx.append((stat['RS_%d' % i])[1])
        stat.pop('RS_%d' % i)
    label = kmean(RS_set, K * n_cluster)

    if 'CS_CLS' in stat:
        c = stat['CS_CLS']
    else:
        c = 0

    r = 0
    for i in range(K * n_cluster):
        idx = [j for j in range(len(RS_set)) if label[j] == i]
        N = len(idx)
        if N > 1:
            cls = [RS_set[j] for j in idx]
            cls_t = list(zip(*cls))
            stat['CS_N_%d' % c] = N
            stat['CS_SUM_%d' % c] = [sum(cls_t[j]) for j in range(d)]
            stat['CS_SUMSQ_%d' % c] = [sum([pow(cls_t[j][t], 2) for t in range(N)]) for j in range(d)]
            cs_idx[c] = [rs_idx[j] for j in idx]
            c += 1
        elif N == 1:
            stat['RS_%d' % r] = (RS_set[idx[0]], rs_idx[idx[0]])
            r += 1
    if c != 0:
        stat['CS_CLS'] = c
    if r != 0:
        stat['RS_CLS'] = r
    return stat, cs_idx


def build_community(edges):
    ve = list(edges.keys())
    single = []
    for i in ve:
        if edges[i] == []:
            single.append(i)
            edges.pop(i)

    new_edges = edges.copy()
    for i in new_edges:
        for j in new_edges[i]:
            if j in edges:
                if i not in edges[j]:
                    edges[j].append(i)
            else:
                edges[j] = [i]

    community = []
    n_single = []
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
                    n_single.append(i)
            edges.pop(c[k])
            k += 1
        community.append((len(c), sorted(c)))

    for i in n_single:
        if i in single:
            single.remove(i)
    for i in single:
        community.append((1, [i]))
    community = sorted(community, key=lambda a: (a[0], a[1]))
    return community


def merge_cs(stat, cs_idx, alpha, d):
    edges = {}
    c = stat['CS_CLS']
    for i in range(c):
        N = stat['CS_N_%d' % i]
        SUM = stat['CS_SUM_%d' % i]
        me = [SUM[j] / float(N) for j in range(d)]
        md_cs = cal_maha(me, stat, d, 'CS')
        md_cs_sorted = sorted(md_cs)
        ind = sorted(range(len(md_cs)), key=lambda j: md_cs[j])
        if ind[0] == i:
            min_md_cs = md_cs_sorted[1]
            idx = ind[1]
        else:
            min_md_cs = md_cs_sorted[0]
            idx = ind[0]
        if min_md_cs < alpha * pow(d, 0.5):
            try:
                edges[i].append(idx)
            except KeyError:
                edges[i] = [idx]
        else:
            edges[i] = []
    community = build_community(edges)
    c_new = len(community)
    cs_idx_new = {}
    xx = []
    yy = []
    zz = []
    for i in range(c_new):
        for j in range(len(community[i][1])):
            if j == 0:
                x = stat['CS_N_%d' % community[i][1][j]]
                y = stat['CS_SUM_%d' % community[i][1][j]]
                z = stat['CS_SUMSQ_%d' % community[i][1][j]]
                cs_idx_new[i] = cs_idx[community[i][1][j]]
            else:
                x += stat['CS_N_%d' % community[i][1][j]]
                y = [y[k] + (stat['CS_SUM_%d' % community[i][1][j]])[k] for k in range(d)]
                z = [z[k] + (stat['CS_SUMSQ_%d' % community[i][1][j]])[k] for k in range(d)]
                cs_idx_new[i] += cs_idx[community[i][1][j]]
        xx.append(x)
        yy.append(y)
        zz.append(z)
    if c_new < c:
        for i in range(c):
            if i < c_new:
                stat['CS_N_%d' % i] = xx[i]
                stat['CS_SUM_%d' % i] = yy[i]
                stat['CS_SUMSQ_%d' % i] = zz[i]
            else:
                stat.pop('CS_N_%d' % i)
                stat.pop('CS_SUM_%d' % i)
                stat.pop('CS_SUMSQ_%d' % i)
        stat['CS_CLS'] = c_new
    return stat, cs_idx_new


def merge_cs_to_ds(stat, cls_label, cs_idx, alpha, d):
    c = stat['CS_CLS']
    stat.pop('CS_CLS')
    for i in range(c):
        N = stat['CS_N_%d' % i]
        SUM = stat['CS_SUM_%d' % i]
        SUMSQ = stat['CS_SUMSQ_%d' % i]
        stat.pop('CS_N_%d' % i)
        stat.pop('CS_SUM_%d' % i)
        stat.pop('CS_SUMSQ_%d' % i)

        me = [SUM[j] / float(N) for j in range(d)]
        md = cal_maha(me, stat, d, 'DS')
        min_md = min(md)
        ind = md.index(min_md)
        idx = cs_idx[i]
        if min_md < alpha * pow(d, 0.5):
            N_D = stat['DS_N_%d' % ind]
            SUM_D = stat['DS_SUM_%d' % ind]
            SUMSQ_D = stat['DS_SUMSQ_%d' % ind]
            stat['DS_N_%d' % ind] = N_D + N
            stat['DS_SUM_%d' % ind] = [SUM[j] + SUM_D[j] for j in range(d)]
            stat['DS_SUMSQ_%d' % ind] = [SUMSQ[j] + SUMSQ_D[j] for j in range(d)]
            for j in idx:
                cls_label[j] = ind
        else:
            for j in idx:
                cls_label[j] = -1
            stat['outlier'] += len(idx)
    return stat, cls_label


def rs_to_outlier(stat, cls_label):
    r = stat['RS_CLS']
    stat.pop('RS_CLS')
    rs_idx = []
    for i in range(r):
        rs_idx.append((stat['RS_%d' % i])[1])
        stat.pop('RS_%d' % i)
    for i in range(len(rs_idx)):
        cls_label[rs_idx[i]] = -1
    stat['outlier'] += len(rs_idx)
    return stat, cls_label


def count_no(stat, SET):
    c = 0
    for i in range(stat[SET + '_CLS']):
        c += stat[SET + '_N_%d' % i]
    return c


def main():
    time_start = time.time()

    # # For Vocareum
    # if len(sys.argv) != 5:
    #     print('Usage: python3 bfr.py $ASNLIB/publicdata/test1/ 10 cluster_res1.json intermediate1.csv')
    # n_cluster = int(sys.argv[2])
    # f1 = open(sys.argv[3], 'w')
    # f2 = open(sys.argv[4], 'w')
    # DATA_DIR = sys.argv[1]

    # For Pycharm
    n_cluster = int(10)
    f1 = open('cluster_res1.json', 'w')
    f2 = open('intermediate1.csv', 'w')
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'hw5_data/test1/')

    fieldnames = ['round_id', 'nof_cluster_discard', 'nof_point_discard', 'nof_cluster_compression', 'nof_point_compression', 'nof_point_retained']
    writer = csv.DictWriter(f2, fieldnames=fieldnames)
    writer.writeheader()

    alpha = 3
    K = 3
    files = sorted(os.listdir(DATA_DIR))
    n_chunk = len(files)
    n_all = 0
    k = 0
    for file in files:
        f = open(os.path.join(DATA_DIR, file), "r")
        data = data_preprocess(f.readlines())
        n_total = len(data)
        n_all += n_total
        if k == 0:
            d = len(data[0])
            cs_idx = {}
            stat, data, cls_label = initialize(data, n_cluster, n_total, d)

        stat, cls_label, cs_idx = decide_newpoint(stat, cls_label, cs_idx, data, n_cluster, alpha, d, K)

        if 'RS_CLS' in stat and stat['RS_CLS'] > K * n_cluster:
            stat, cs_idx = merge_rs(stat, cs_idx, n_cluster, K, d)

        if 'CS_CLS' in stat:
            if stat['CS_CLS'] > 1:
                stat, cs_idx = merge_cs(stat, cs_idx, alpha, d)

        # last round
        if k == n_chunk - 1:
            if 'CS_CLS' in stat:
                stat, cls_label = merge_cs_to_ds(stat, cls_label, cs_idx, alpha, d)
            if 'RS_CLS' in stat:
                stat, cls_label = rs_to_outlier(stat, cls_label)
        k += 1
        intermediate = {}
        intermediate['round_id'] = k
        if 'DS_CLS' in stat:
            intermediate['nof_cluster_discard'] = stat['DS_CLS']
            intermediate['nof_point_discard'] = count_no(stat, 'DS')
        else:
            intermediate['nof_cluster_discard'] = 0
            intermediate['nof_point_discard'] = 0
        if 'CS_CLS' in stat:
            intermediate['nof_cluster_compression'] = stat['CS_CLS']
            intermediate['nof_point_compression'] = count_no(stat, 'CS')
        else:
            intermediate['nof_cluster_compression'] = 0
            intermediate['nof_point_compression'] = 0
        if 'RS_CLS' in stat:
            intermediate['nof_point_retained'] = stat['RS_CLS'] + stat['outlier']
        else:
            intermediate['nof_point_retained'] = stat['outlier']

        writer.writerow(intermediate)

    if len(cls_label) != n_all or 'CS_CLS' in stat or 'RS_CLS' in stat:
        print('error')
    else:
        # evaluate
        with open(os.path.join(BASE_DIR, 'hw5_data/cluster1.json')) as f:
            gt_label = json.load(f)

        gt = []
        for i in range(n_all):
            gt.append(gt_label[str(i)])
        cls = []
        for i in range(n_all):
            cls.append(cls_label[i])
        print(nmi(gt, cls))

    json.dump(cls_label, f1)
    f1.close()
    f2.close()
    time_end = time.time()
    print('Duration:', time_end - time_start)


if __name__ == '__main__':
    main()