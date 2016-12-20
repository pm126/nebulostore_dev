import re
import sys
import os
import operator
from subprocess import call
from datetime import datetime, timedelta
import pprint

def timeFromLine(line):
	timeString = line.split()[0]
	return datetime.strptime(timeString, "%H:%M:%S,%f")

if len(sys.argv) != 2:
    print("Argument needed!")
    sys.exit(1)

rootDir = sys.argv[1]
pp = pprint.PrettyPrinter(indent=4)

try:
    report = open(os.path.join(rootDir, "comm.log"), "r")

    waitingMessagesHistory = []
    currentNumber = 0
    totalNumber = 0
    waitTimes = {}
    lastTimes = {}

    for line in report:
	if re.search(re.compile("sendMessage"), line) is not None:
	    currentNumber += 1
	    totalNumber += 1
	    waitingMessagesHistory.append(currentNumber)
	elif re.search(re.compile("MessageSenderCallable.call.* with"), line) is not None:
	    currentNumber -= 1
	    waitingMessagesHistory.append(currentNumber)
	    thread = line.split("[")[1].split(" ")[0]
	    time = timeFromLine(line)
	    if lastTimes.has_key(thread):
		waitTimes[line] = (time - lastTimes[thread]).total_seconds()
	    else:
		waitTimes[line] = 0
	    lastTimes[thread] = time

    report.close()

    print waitingMessagesHistory
    print max(waitingMessagesHistory)
    print totalNumber
    pp.pprint(waitTimes)

except IOError:
    print "Could not open the log"
#print threadHistory[-1]
#print threadTypeHistory[-1]
