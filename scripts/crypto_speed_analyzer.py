import re
import sys
import os
import operator
from subprocess import call
from datetime import datetime, timedelta
import pprint
from operator import itemgetter

def walklevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]

def timeFromLine(line):
	timeString = line.split()[0]
	return datetime.strptime(timeString, "%H:%M:%S,%f")

if len(sys.argv) != 2:
    print("Argument needed!")
    sys.exit(1)

rootDir = sys.argv[1]

overallResults = []
for _, dirs, _ in walklevel(rootDir, 0):
    for dr in dirs:
	try:
	    report = open(os.path.join(rootDir, dr, "logs", "crypto.log"), "r")

	    results = []
	    lastResult = None
	    for line in report:
		if re.search(re.compile("Before decrypting the"), line) is not None:
		    lastTime = timeFromLine(line)
		elif re.search(re.compile("dded.*to the.*queue"), line) is not None:
		    time = timeFromLine(line)
		    if (time - lastTime).total_seconds() >= 0:
			lastResult = (lastTime, (time - lastTime).total_seconds())
		    	results.append(lastResult)
		    else:
			lastResult = None
	    report.close()

	    print "PEER " + dr
	    pp = pprint.PrettyPrinter(indent=4)
	    #pp.pprint(results)
	    if (len(results) > 0):
		pp.pprint(sum(map(lambda x : x[1], results))/len(results))
	    else:
		pp.pprint(0)
	    overallResults += map(lambda x : (x, dr), results)

	except IOError:
	    print "Could not open the log"

pp = pprint.PrettyPrinter(indent=4)
sortedResults = sorted(overallResults, key = lambda x : x[0][1], reverse = True)
pp.pprint("Longest results: ")
pp.pprint(sortedResults[len(sortedResults)/100])
onlyResults = map(lambda x : x[0][1], overallResults)
if len(overallResults) > 0:
    print("Mean: " + str(sum(onlyResults)/len(onlyResults)))
    print("Max: " + str(max(onlyResults)))
    print("Min: " + str(min(onlyResults)))
else:
    pp.pprint(0)
