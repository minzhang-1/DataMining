# INF 553 2020S HW6
# Task 3
# command: spark-submit task3.py 9999 task3_ans.csv
from __future__ import print_function
import sys
import csv
import tweepy
import random
import collections


# # For Vocareum
# if len(sys.argv) != 3:
#     print('Usage: task3.py 9999 task3_ans.csv')
# fp = sys.argv[2]


# For Pycharm
fp = 'task3_ans.csv'


class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(MyStreamListener, self).__init__()
        self.n = 0
        self.samples = []
        self.tags = collections.Counter()

    def on_status(self, status):
        sample = status.text
        if self.n < 100:
            sample_s = sample.split()
            tag = [s[1:] for s in sample_s if s[0] == "#" and len(s) > 1 and s[1:].encode('UTF-8').isalpha()]
            if bool(tag):
                self.n += 1
                self.samples.append(sample)
                self.tags.update(tag)
                key = list(self.tags.keys())
                value = list(self.tags.values())
                freq = sorted(list(set(value)), reverse=True)
                if len(freq) > 3:
                    freq = freq[:3]
                top = []
                for i in freq:
                    k = []
                    for j in range(len(value)):
                        if value[j] == i:
                            k.append(key[j])
                    top.append((sorted(k), int(i)))

                if self.n == 1:
                    f = open(fp, 'w')
                    f.write('The number of tweets with tags from the beginning: ' + str(self.n) + '\n')
                    print('The number of tweets with tags from the beginning: ' + str(self.n))
                else:
                    f = open(fp, 'a')
                    f.write('\nThe number of tweets with tags from the beginning: ' + str(self.n) + '\n')
                    print('\nThe number of tweets with tags from the beginning: ' + str(self.n))

                for i in top:
                    for j in i[0]:
                        print(str(j) + ' : ' + str(i[1]))
                        f.write(str(j) + ' : ' + str(i[1]) + '\n')
                f.close()
        else:
            n_d = random.randint(1, self.n)
            if n_d < 100:
                sample_s = sample.split()
                tag = [s[1:] for s in sample_s if s[0] == "#" and len(s) > 1 and s[1:].encode('UTF-8').isalpha()]
                if bool(tag):
                    self.n += 1
                    discard = self.samples.pop(n_d)
                    self.samples.append(sample)
                    self.tags.update(tag)
                    discard_s = discard.split()
                    tag_d = [s[1:] for s in discard_s if s[0] == "#" and len(s) > 1 and s[1:].encode('UTF-8').isalpha()]
                    self.tags.subtract(tag_d)
                    key = list(self.tags.keys())
                    value = list(self.tags.values())
                    freq = sorted(list(set(value)), reverse=True)
                    if len(freq) > 3:
                        freq = freq[:3]
                    top = []
                    for i in freq:
                        k = []
                        for j in range(len(value)):
                            if value[j] == i:
                                k.append(key[j])
                        top.append((sorted(k), int(i)))
                    f = open(fp, 'a')
                    f.write('\nThe number of tweets with tags from the beginning: ' + str(self.n) + '\n')
                    print('\nThe number of tweets with tags from the beginning: ' + str(self.n))

                    for i in top:
                        for j in i[0]:
                            print(str(j) + ' : ' + str(i[1]))
                            f.write(str(j) + ' : ' + str(i[1]) + '\n')
                    f.close()

    def on_error(self, status_code):
        if status_code == 420:
            return False


def main():

    auth = tweepy.OAuthHandler('-', '-')
    auth.set_access_token('-', '-')
    api = tweepy.API(auth)

    msl = MyStreamListener()
    ms = tweepy.Stream(auth=api.auth, listener=msl)
    tweets = ms.filter(track=['#', 'COVID'], languages=['en'])


if __name__ == '__main__':
    main()