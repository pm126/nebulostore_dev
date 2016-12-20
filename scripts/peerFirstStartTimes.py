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

peerStats = []
for _, dirs, _ in walklevel(rootDir, 0):
    for dr in dirs:
	startTime = None
	counter = 0
	peersLog = open(os.path.join(rootDir, dr, "logs", "peers.log"), "r")
	ended = False
	for line in peersLog:
	    if ended:
		break
	    if re.search(re.compile("Binding app-key"), line) is not None:
		if counter == 0:
		    startTime = datetimeFromLine(line)
		counter += 1
	    elif re.search(re.compile("Binding bdb-peer.holder"), line) is not None:
		if counter == int(args.counter):
		    peerStats.append((datetimeFromLine(line) - startTime).total_seconds())
		    counter = 0
		    ended = True
		
print "Avg: " + str(sum(peerStats)/len(peerStats))
print "Max: " + str(max(peerStats))
print "Min: " + str(min(peerStats))
