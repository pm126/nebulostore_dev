import re
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
import copy

pp = pprint.PrettyPrinter(indent = 4)

class PeersData:
    def __init__(self, stopped, running):
	self.stopped = stopped
	self.running = running

    def startUpPeer(self, peer):
	try:
	    self.stopped.remove(peer)
	except KeyError:
	    pass
	self.running.add(peer)

    def shutDownPeer(self, peer):
	self.stopped.add(peer)
	try:
	    self.running.remove(peer)
	except KeyError:
	    pass

    def __str__(self):
	return "Stopped: " + str(self.stopped) + "\n" + "Running: " + str(self.running)

    def __repr__(self):
	return self.__str__()

def peerFromCommAddress(commAddress):
	splittedLine = commAddress.split("-")
	return str(int(splittedLine[3]))

def datetimeFromLine(line, date=dt.today()):
    timeString = line.split()[0]
    return datetime.combine(date, datetime.strptime(timeString, "%H:%M:%S,%f").time())

def timeFromLine(line):
    timeString = line.split()[0]
    return datetime.strptime(timeString, "%H:%M:%S,%f").time()

parser = argparse.ArgumentParser(description='Analyze peers availability basing on testing log.')
parser.add_argument("testingLogPath", metavar='rootDir', help='testing log path')

args = parser.parse_args()

testingLogPath = args.testingLogPath

results = {}

testingLog = open(testingLogPath, "r")
lastStartTime = None
lastGenStart = None
for line in testingLog:
    if re.search(re.compile("Running test"), line) is not None:
	print "Next test with start time:" + str(datetimeFromLine(line))
	lastGenStart = None
	lastStartTime = datetimeFromLine(line)
	results[lastStartTime] = []
	results[lastStartTime].append((lastStartTime, PeersData(set(range(45)), set())))
    if re.search(re.compile("\] startUp|\] shutDown"), line) is not None:
	if lastGenStart is None or (datetimeFromLine(line) - lastGenStart).total_seconds() > 10:
	    #Start new generation
	    lastGenStart = datetimeFromLine(line)
	    results[lastStartTime].append((lastGenStart, copy.deepcopy(results[lastStartTime][-1][1])))
	peer = int(line.split("[")[2].split("]")[0])
	if re.search(re.compile("startUp"), line) is not None:
	    results[lastStartTime][-1][1].startUpPeer(peer)
	else:
	    results[lastStartTime][-1][1].shutDownPeer(peer)

pp.pprint(results)

for test, testData in results.iteritems():
    print "--------------------------- Test started at: " + str(test) + "--------------------------------------"
    lastEntry = None
    for testEntry in testData:
	if lastEntry is not None:
	    print "=== Next generation started at: " + str(testEntry[0]) + " ==="
	    print "Number of running peers: " + str(len(lastEntry[1].running))
	    print "Number of stopped peers: " + str(len(lastEntry[1].running - testEntry[1].running))
	    print "Number of started peers: " + str(len(lastEntry[1].stopped - testEntry[1].stopped))
	lastEntry = testEntry
