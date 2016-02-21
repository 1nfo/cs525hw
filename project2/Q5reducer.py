#!/usr/bin/env python

#group
import sys
summary= {}
for line in sys.stdin:
	record = line.split(',')
	cid = int(record[0])
	val = summary.get(int(record[0]), [record[1], 0])
	val[1] += int(record[2])
	summary[cid] = val

for cid in summary:
	print '%d,%s,%d' % tuple([cid] + summary[cid])
