#!/usr/bin/python

import sys

flag = False
prices = []
for line in sys.stdin:
    if flag == False:
        flag = True
        continue
    line = line.strip()
    line = line.split(',')
    try:
        prices.append(float(line[9]))
    except:
        continue
avg = sum(prices) / len(prices)
print(len(prices), avg)