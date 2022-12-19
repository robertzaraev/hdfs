#!/usr/bin/python

import sys

c = []
cm = []
for line in sys.stdin:
    line = line.strip()
    line = line.split(' ')
    c.append(float(line[0]))
    cm.append(float(line[1]) * float(line[0]))

print(sum(cm) / sum(c))