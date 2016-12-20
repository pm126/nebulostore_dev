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

report = open(os.path.join(rootDir, "replicaresolver.log"), "r")

times = []
startTimes = {}
jobIds = []
for line in report:
    if re.search(re.compile("process.*with jobId:"), line) is not None:
	jobId = line.split("jobId: ")[1].split(")")[0]
	time = timeFromLine(line)
	startTimes[jobId] = time
	jobIds.append(jobId)
    elif re.search(re.compile("pool-9-thread-1.*get"), line) is not None:
	times.append((timeFromLine(line) - startTimes[line.split("jobId: ")[1].split(")")[0]]).total_seconds())

report.close()

crypto = open(os.path.join(rootDir, "crypto.log"), "r")
cryptoJobIds = []
for line in crypto:
    if re.search(re.compile("Job id of GetKeyModule"), line) is not None:
	jobId = line.split("GetKeyModule: ")[1].split("\n")[0]
	if jobId in jobIds:
	    cryptoJobIds.append(jobId)

print len(jobIds)
print len(cryptoJobIds)
times.sort()
print times
print sum(times) / float(len(times))
