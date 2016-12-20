import re
import sys
import os
import operator
import random
from subprocess import call
from datetime import datetime, timedelta, time, date as dt
from bisect import bisect
import argparse
import copy
import pprint
import math

pp = pprint.PrettyPrinter(indent = 4)

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

def datetimeFromLine(line, date=dt.today()):
    timeString = line.split()[0]
    return datetime.combine(date, datetime.strptime(timeString, "%H:%M:%S,%f").time())

def timeFromLine(line):
    timeString = line.split()[0]
    return datetime.strptime(timeString, "%H:%M:%S,%f").time()

parser = argparse.ArgumentParser(description='Calculate rest start delays for each peer.')
parser.add_argument("rootDir", metavar='rootDir', help='Logs root directory')
parser.add_argument("counter", metavar="counter")

args = parser.parse_args()

rootDir = args.rootDir

helperStats = []
for _, dirs, _ in walklevel(rootDir, 0):
    for dr in dirs:
	counter = 1
	startTimes = []
	currDay = dt.today()
	lastDate = None
	peersLog = open(os.path.join(rootDir, dr, "logs", "peers.log"), "r")
	for line in peersLog:
	    try:
		currentDate = datetimeFromLine(line, currDay)
		if lastDate is not None and (lastDate > currentDate and abs((lastDate - currentDate).total_seconds() / 3600) > 2):
		    currDay = currDay + timedelta(days = 1)
		lastDate = currentDate
	    except (ValueError, IndexError):
		continue
	    if re.search(re.compile("ing bdb-peer"), line) is not None:
		if counter == int(args.counter):
		    startTimes.append(datetimeFromLine(line, currDay))
		    counter = 1
		else:
		    counter += 1
	

	currDay = dt.today()
	lastDate = None
	testingLog = open(os.path.join(rootDir, dr, "logs", "testing.log"), "r")
	for line in testingLog:
	    try:
		currentDate = datetimeFromLine(line, currDay)
		if lastDate is not None and (lastDate > currentDate and abs((lastDate - currentDate).total_seconds() / 3600) > 2):
		    currDay = currDay + timedelta(days = 1)
		lastDate = currentDate
	    except (ValueError, IndexError):
		continue
	    if re.search(re.compile("Downloaded login string:"), line) is not None:
		if len(startTimes) > 0:
		    startIndex = 0
		    currDate = datetimeFromLine(line, currDay)
		    while startIndex < len(startTimes) and (startTimes[startIndex] <= currDate):
			startIndex += 1
		helperStats.append((currDate - startTimes[startIndex - 1]).total_seconds())
                break

print sum(helperStats)/len(helperStats)
print max(helperStats)
print min(helperStats)
