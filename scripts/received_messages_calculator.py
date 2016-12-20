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

parser = argparse.ArgumentParser(description='Analyze NebuloStore logs for asynchronous messages test.')
parser.add_argument("report", metavar='report', help='Report file path')

args = parser.parse_args()

reportPath = args.report

report = open(reportPath)

zeroPeerSentMsgs = None
zeroPeerReceivedMsgs = None
overallSentMsgs = None
overallReceivedMsgs = None

for line in report:
    if re.search(re.compile("sent to peer"), line) is not None:
	sentMsgsNumber = int(line.split("peer: ")[1].split("\n")[0])
	if zeroPeerSentMsgs is None:
	    zeroPeerSentMsgs = sentMsgsNumber
	else:
	    overallSentMsgs = sentMsgsNumber
    elif re.search(re.compile("received by peer"), line) is not None:
	receivedMsgsNumber = int(line.split("peer: ")[1].split("\n")[0])
	if zeroPeerReceivedMsgs is None:
	    zeroPeerReceivedMsgs = receivedMsgsNumber
	else:
	    overallReceivedMsgs = receivedMsgsNumber

print(str(zeroPeerSentMsgs) + "/" + str(zeroPeerReceivedMsgs))
print(str(overallSentMsgs) + "/" + str(overallReceivedMsgs))

print("{0:.2f}".format(100 - (overallReceivedMsgs - zeroPeerReceivedMsgs)/float(overallSentMsgs - zeroPeerSentMsgs) * 100) + "%")

