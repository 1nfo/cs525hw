#!/usr/bin/env python

'''
hadoop jar  /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar -input input/Transactions  -output out/py -reducer Q5reducer.py -mapper Q5mapper.py -file Customers -file Q5reducer.py -file Q5mapper.py
'''

#map side join first
import sys

customers = {}
with open('Customers','rb') as f:
	for line in f:
		record = line.strip().split(',')
		if int(record[3].strip())==5:
			customers[int(record[0])]=record[1]
for line in sys.stdin:
	record = line.split(',')
	cid = int(record[1].strip())
	if cid in customers.keys():
		print "%d,%s,%d" % (cid,customers[cid],1)
