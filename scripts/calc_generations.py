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

class TestMessage:
    def __init__(self, idn, sender, receiver, jobIdAbbr):
	self.idn = idn
	self.sender = sender
	self.receiver = receiver
	self.jobIdAbbr = jobIdAbbr
	self.jobId = ""

    def __str__(self):
	return self.idn

    def __repr__(self):
	return self.__str__()

synchroPeers = None
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

def datetimeFromLine(line, date):
    timeString = line.split()[0]
    return datetime.combine(date, datetime.strptime(timeString, "%H:%M:%S,%f").time())

def timeFromLine(line):
    timeString = line.split()[0]
    return datetime.strptime(timeString, "%H:%M:%S,%f").time()

def calcActivityPeriods():
    for _, dirs, _ in walklevel(rootDir, 0):
	for dr in dirs:
	    try:
		peersLog = open(os.path.join(rootDir, dr, "logs", "peers.log"), "r")
	    except IOError:
		print "Could not read the peers log for peer " + dr
		continue
	    activityPeriods[dr] = []
	    closed = False
	    first = True
	    date = dt.today()
	    for line in peersLog:
		try:
		    if first:
			first = False
			startTime = datetimeFromLine(line, date)
			startTimes[dr] = startTime
			activityPeriods[dr].append(startTime)
		    if closed:
			tmpTime = timeFromLine(line)
			if (tmpTime.hour < activityPeriods[dr][-1].time().hour):
			    date = date + timedelta(days = 1)
			activityPeriods[dr].append(datetimeFromLine(line, date))
			closed = False
		    elif re.search(re.compile("Finished quitNebuloStore.*"), line) is not None:
			tmpTime = timeFromLine(line)
			if (tmpTime < activityPeriods[dr][-1].time()):
			    date = date + timedelta(days = 1)
			#activityPeriods[dr].append(datetimeFromLine(line, date))
			closed = True
		except (IndexError, ValueError):
		    continue
	    peersLog.close()

parser = argparse.ArgumentParser(description='Analyze NebuloStore logs for asynchronous messages test.')
parser.add_argument("rootDir", metavar='rootDir', help='Logs root directory')

args = parser.parse_args()

rootDir = args.rootDir

activityPeriods = {}
startTimes = {}

calcActivityPeriods()

generations = []
while True:
    minPeer = None
    minDate = None
    for peer, periods in activityPeriods.iteritems():
	if len(periods) > 0 and (minPeer is None or minDate > periods[0]):
	    minPeer = peer
	    minDate = periods[0]
    if minPeer is None:
	break
    if len(generations) == 0 or (minDate - generations[-1][0]).total_seconds() > 15:
	generations.append((minDate, set()))
    generations[-1][1].add(minPeer)
    activityPeriods[minPeer].pop(0)

for generation in generations:
    print "Next generation started at " + str(generation[0])
    print "Number of starting peers: " + str(len(generation[1]))

print "Average number of started peers: " + str(sum(map(lambda x : float(len(x[1])), generations[1:]))/float(len(generations[1:])))
