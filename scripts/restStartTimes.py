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

args = parser.parse_args()

rootDir = args.rootDir

restStatsDict = {}
peersPeriods = {}
for _, dirs, _ in walklevel(rootDir, 0):
    for dr in dirs:
	startTimes = []
	peersPeriods[dr] = []
	peersLog = open(os.path.join(rootDir, dr, "logs", "peers.log"), "r")
	for line in peersLog:
	    if re.search(re.compile("Starting nebulostore"), line) is not None:
		startTimes.append(datetimeFromLine(line))
	    elif re.search(re.compile("Started core threads"), line) is not None:
		peersPeriods[dr].append((datetimeFromLine(line), startTimes.pop(0)))

	print "----------------------" + dr + "-----------------------------------"
	pp.pprint(peersPeriods[dr])

	restStatsDict[dr] = []
	restLog = open(os.path.join(rootDir, dr, "logs", "rest.log"), "r")
	lastRunStartTime = None
	for line in restLog:
	    if re.search(re.compile("Running rest module"), line) is not None:
		lastRunStartTime = datetimeFromLine(line)
	    elif re.search(re.compile("Started http server"), line) is not None:
		if len(peersPeriods[dr]) > 0:
		    periodIndex = 0
		    while periodIndex < len(peersPeriods[dr]) and (peersPeriods[dr][periodIndex][0] <= lastRunStartTime or (peersPeriods[dr][periodIndex][0] - lastRunStartTime).total_seconds() < 0.5):
			periodIndex += 1
		    
		restStatsDict[dr].append((peersPeriods[dr][periodIndex - 1][1], (datetimeFromLine(line) - peersPeriods[dr][periodIndex - 1][1]).total_seconds(), datetimeFromLine(line), lastRunStartTime))

pp.pprint(restStatsDict)

allRestStats = []
ended = False
while not ended:
    minKey = None
    minVal = None
    for key, val in restStatsDict.iteritems():
	if len(val) > 0 and (minVal is None or minVal[0] > val[0][0]):
	    minKey = key
	    minVal = val[0]
    if minVal is None:
	ended = True
    else:
	allRestStats.append(minVal)
	restStatsDict[minKey].pop(0)

pp.pprint(allRestStats)

lastGenStart = None
lastGenList = []
for restStats in allRestStats:
    if lastGenStart is None or (restStats[0] - lastGenStart).total_seconds() > 10:
	if len(lastGenList) > 0:
	    print str(lastGenStart) + ": " + str(sum(lastGenList)/len(lastGenList))
	lastGenStart = restStats[0]
	lastGenList = []
    lastGenList.append(restStats[1])

if len(lastGenList) > 0:
    print str(lastGenStart) + ": " + str(sum(lastGenList)/len(lastGenList))
