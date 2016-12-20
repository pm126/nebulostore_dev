import re
import sys
import os
import operator
import random
from subprocess import call
from datetime import datetime, timedelta
from bisect import bisect
import pprint

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
    print("Arguments needed!")
    sys.exit(1)

rootDir = sys.argv[1]
pp = pprint.PrettyPrinter(indent=4)
gaps = []
try:
    checkerLog = open(os.path.join(rootDir, "logs", "testing.log"), "r")
except IOError:
    print "Could not read " + rootDir + " commLog"
lastTime = None
for line in checkerLog:
    try:
	time = timeFromLine(line)
	if re.search(re.compile("Starting|Ending"), line) is not None:
	    lastTime = None
	    continue
	if lastTime is not None:
	    if time < lastTime:
		#print "Warn!"
		continue
	    gaps.append(((time - lastTime).total_seconds(), lastTime, time))
	lastTime = time
    except (IndexError, ValueError):
	continue
checkerLog.close()

'''
print "First 1/4:"
shortGaps = gaps[0:len(gaps)/4]
print sum(shortGaps)/len(shortGaps)

print "Second 1/4:"
shortGaps = gaps[len(gaps)/4:len(gaps)/2]
print sum(shortGaps)/len(shortGaps)

print "Third 1/4:"
shortGaps = gaps[len(gaps)/2:3*len(gaps)/4]
print sum(shortGaps)/len(shortGaps)

print "Fourth 1/4:"
shortGaps = gaps[3*len(gaps)/4:len(gaps)]
print sum(shortGaps)/len(shortGaps)

print "First half:"
shortGaps = gaps[0:len(gaps)/2]
print sum(shortGaps)/len(shortGaps)

print "Second half:"
shortGaps = gaps[len(gaps)/2:len(gaps)]
print sum(shortGaps)/len(shortGaps)
'''
print "Overall:"
gaps.sort()
tmp = map(lambda x : x[0], gaps)


print "Overall:"
gaps.sort()
pp.pprint(gaps[-1:])
pp.pprint(sum(tmp)/len(tmp))
