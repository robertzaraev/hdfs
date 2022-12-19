#!/usr/bin/python

import sys

flag = False
prices = []
for line in sys.stdin:
    if flag == False:
        flag = True
        continue
    line = line.strip()
    words = line.split(',')
    try:
        prices.append(float(words[9]))
    except:
        continue
avg = sum(prices) / len(prices)
var = sum((i - avg) ** 2 for i in prices) / len(prices)
print(len(prices), avg, var)
