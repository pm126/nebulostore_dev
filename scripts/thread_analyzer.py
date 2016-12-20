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

try:
    report = open(os.path.join(rootDir, "dispatcher.log"), "r")

    threads = {}
    times = {}
    jobIds = set()
    threadHistory = []
    startTimes = {}
    durations = {}
    threadTypeHistory = []
    jobIdsTypes = {}
    for line in report:
	if re.search(re.compile("Received message with.*"), line) is not None:
	    jobId = line.split("jobID: ")[1].split(" and")[0]
	elif re.search(re.compile("Starting new thread.*"), line) is not None:
	    jobIds.add(jobId)
	    threadHistory.append(jobIds.copy())
	    threadType = line.split("type ")[1].split("\n")[0]
	    if not threads.has_key(threadType):
		threads[threadType] = 0
	    threads[threadType] = threads[threadType] + 1
	    threadTypeHistory.append((timeFromLine(line), dict(threads)))
	    jobIdsTypes[jobId] = threadType
	    startTimes[jobId] = timeFromLine(line)
	elif re.search(re.compile("Got job ended"), line) is not None:
	    jobId = line.split("ID: ")[1].split("\n")[0]
	    if jobId in jobIds:
		threads[jobIdsTypes[jobId]] = threads[jobIdsTypes[jobId]] - 1
		jobIds.remove(jobId)
		threadHistory.append(jobIds.copy())
		threadTypeHistory.append((timeFromLine(line), dict(threads)))
		durations[jobId] = (timeFromLine(line) - startTimes[jobId]).total_seconds()

    report.close()

    print max([len(x) for x in threadHistory])
    #print jobIdsTypes

except IOError:
    print "Could not open the log"
pp = pprint.PrettyPrinter(indent=4)
print threadHistory[-1]
pp.pprint(threadTypeHistory)
pp.pprint(durations)
