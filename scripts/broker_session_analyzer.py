import re
import sys
import os
import operator
from subprocess import call
from datetime import datetime, timedelta

def timeFromLine(line):
	timeString = line.split()[0]
	return datetime.strptime(timeString, "%H:%M:%S,%f")

if len(sys.argv) != 2:
    print("Argument needed!")
    sys.exit(1)

rootDir = sys.argv[1]

try:
    report = open(os.path.join(rootDir, "broker.log"), "r")

    peers = {}
    for line in report:
	if re.search(re.compile("Starting session agree.*with.*"), line) is not None:
	    peer = line.split("with ")[1].split("\n")[0]
	    if not peers.has_key(peer):
		peers[peer] = 0
	    peers[peer] = peers[peer] + 1
    report.close()

    print peers

except IOError:
    print "Could not open the log"
#print threadHistory[-1]
#print threadTypeHistory[-1]
