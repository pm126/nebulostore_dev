import re
import sys
import os
import operator
from subprocess import call
from datetime import datetime, timedelta

if len(sys.argv) != 2:
    print("Argument needed!")
    sys.exit(1)

rootDir = sys.argv[1]

report = open(rootDir, "r")

types = {}
found = False
threadTypes = {}
for line in report:
    if re.search(re.compile("NegotiatorModule.*\""), line) is not None:
	found = True
    elif re.search(re.compile("at .*"), line) is not None and found:
	place = line.split("at ")[1].split("\n")[0]
	if not types.has_key(place):
	    types[place] = 0
	types[place] = types[place] + 1
	found = False
    if re.search(re.compile("\""), line) is not None:
	threadType = line.split("\"")[1].split(":")[0].split("-")[0]
	if not threadTypes.has_key(threadType):
	    threadTypes[threadType] = 0
	threadTypes[threadType] = threadTypes[threadType] + 1

report.close()
length = sum([value for key, value in types.items()])
print types
print {key: "{0:.2f}".format(value/float(length) * 100) for key, value in types.items()}
print threadTypes
