#!/usr/bin/env python
import sys;

currentKey = ""

for line in sys.stdin:
	line = line.strip()
	key, value = line.split('\t',1)
	if currentKey == key :
		continue
	print '%s\t%s' % (key, value)
