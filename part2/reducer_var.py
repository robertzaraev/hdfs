#!/usr/bin/python

import sys

size_arr = []
avg_arr = []
var_arr = []
for (idx, line) in enumerate(sys.stdin):
    (size, avg, var) = line.split(' ')
    size_arr.append(int(size))
    avg_arr.append(float(avg))
    var_arr.append(float(var))
if len(size_arr) == 1:
    print(var_arr[0])
else:
    var = (size_arr[0] * var_arr[0] + size_arr[1] * size_arr[1]) \
        / (size_arr[0] + size_arr[1]) + size_arr[0] * size_arr[1] \
        * ((avg_arr[0] - avg_arr[1]) / (size_arr[0] + size_arr[1])) ** 2
    print(var)