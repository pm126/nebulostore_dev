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

parser = argparse.ArgumentParser(description='Analyze NebuloStore logs for asynchronous messages test.')
parser.add_argument("rootDir", metavar='rootDir', help='Logs root directory')
parser.add_argument("label", metavar="label", help="Label that should be added to the second column")

args = parser.parse_args()

rootDir = args.rootDir

overallDelays = []

for _, dirs, _ in walklevel(rootDir, 0):
    for dr in sorted(dirs, key=lambda x : int(x)):
	if dr == "0":
	    continue
	try:
	    testingLog = open(os.path.join(rootDir, dr, "logs", "testing.log"), "r")
	except IOError:
	    print "Could not read the testing log for peer " + dr
	    continue
	date = dt.today()
	lastDelayLine = None
	lastMessageId = None
	delaysMap = {}
	for line in testingLog:
	    if re.search(re.compile("Next async message: "), line) is not None:
		lastMessageId = line.split("message: ")[1].split("\n")[0]
	    if re.search(re.compile("Added new message delay"), line) is not None:
		if not lastMessageId in delaysMap:
		    try:
			delaysMap[lastMessageId] = int(line.split(lastMessageId + "=")[1].split("}")[0].split(",")[0])
		    except IndexError:
			print lastMessageId
			continue
	testingLog.close()

	delays = delaysMap.values()
	for delay in delays:
	    print str(delay) + "\t" + args.label
