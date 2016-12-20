import re
import sys
import os
import operator
import random
from subprocess import call
from datetime import datetime, timedelta
from bisect import bisect

def walklevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]

def peerFromCommAddress(commAddress):
	splittedLine = commAddress.split("-")
	return str(int(splittedLine[3]))

def timeFromLine(line):
    timeString = line.split()[0]
    time = datetime.strptime(timeString, "%H:%M:%S,%f")
    return time

if len(sys.argv) != 2:
    print("Argument needed!")
    sys.exit(1)

rootDir = sys.argv[1]

delaysMapsMap = {}
for _, dirs, _ in walklevel(rootDir, 0):
    for dr in dirs:
	print "Starting calc for peer " + dr
	try:
	    testingLog = open(os.path.join(rootDir, dr, "logs", "testing.log"), "r")
	except IOError:
	    print "Could not read the testing log for peer " + dr
	    continue
	delaysMapsMap[dr] = {}
	for line in testingLog:
	    if re.search(re.compile("current delays"), line) is not None:
		delays = line.split("{")[-1].split("}")[0]
		if delays == "":
		    continue
		entries = delays.split(",")
		for entry in entries:
		    try:
			key = entry.split("=")[0]
			value = entry.split("=")[1]
		    except IndexError:
			continue
		    if not delaysMapsMap[dr].has_key(key):
			delaysMapsMap[dr][key] = long(value)
	testingLog.close()
	print "Calculated for peer " + dr
	values = delaysMapsMap[dr].values()
	if len(values) == 0:
	    print "No information about delays"
	else:
	    print "Result: " + str(sum(values)/float(len(values)))

all_values = reduce(lambda x, y: x + y, map(lambda x: x.values(), delaysMapsMap.values()))
all_values.sort()
if len(all_values) == 0:
    print "No information about overall delays"
else:
    print "Overall mean result: " + str(sum(all_values)/float(len(all_values)))
    print "Overall median: " + str(all_values[len(all_values)/2])
