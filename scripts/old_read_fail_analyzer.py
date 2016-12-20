from __future__ import print_function
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
from itertools import combinations

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

replicators = None
testStartTimestamp = None
testEndTimestamp = None
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
    global runStartTimestamp
    for _, dirs, _ in walklevel(rootDir, 0):
	for dr in dirs:
	    try:
		peersLog = open(os.path.join(rootDir, dr, "logs", "peers.log"), "r")
	    except IOError:
		print("Could not read the peers log for peer " + dr)
		continue
	    activityPeriods[dr] = []
	    closed = False
	    first = True
	    date = dt.today()
	    for line in peersLog:
		try:
		    if first:
			time = timeFromLine(line)
			tmpDatetime = datetime.combine(runStartTimestamp.date(), time)
			diff1 = abs((tmpDatetime - runStartTimestamp).total_seconds())
			nextDay = runStartTimestamp.date() + timedelta(days = 1)
			tmpDatetime = datetime.combine(nextDay, time)
			diff2 = abs((tmpDatetime - runStartTimestamp).total_seconds())
			if (diff1 < diff2):
			    date = runStartTimestamp.date()
			else:
			    date = runStartTimestamp.date() + timedelta(days = 1)
		    if first and re.search(re.compile("Started core threads"), line) is not None:
			first = False
			startTime = datetimeFromLine(line, date)
			startTimes[dr] = startTime
			activityPeriods[dr].append(startTime)
		    elif re.search(re.compile(".*quitNebuloStore.*"), line) is not None:
			if not closed:
			    tmpTime = timeFromLine(line)
			    if (tmpTime < activityPeriods[dr][-1].time()):
				date = date + timedelta(days = 1)
			    activityPeriods[dr].append(datetimeFromLine(line, date))
			    closed = True
		    elif closed and re.search(re.compile("Started core threads"), line) is not None:
			tmpTime = timeFromLine(line)
			if (tmpTime < activityPeriods[dr][-1].time()):
			    date = date + timedelta(days = 1)
			activityPeriods[dr].append(datetimeFromLine(line, date))
			closed = False
		except (IndexError, ValueError):
		    continue
	    peersLog.close()
	    #Calculate inactive replicators for given jobId of SendAsynchronousMessageForPeerModule
	    '''try:
		commLog = open(os.path.join(rootDir, dr, "logs", "comm.log"), "r")
	    except IOError:
		print "Could not read the comm logs for peer " + dr
		continue
	    date = dt.today()
	    for line in commLog:
		if re.search(re.compile("sendMessage.*StoreAsynchronousMessage.*error"), line) is not None:
		    jobId = line.split("jobId_='")[1].split("',")[0]
		    if not inactiveReplicators.has_key(jobId):
			inactiveReplicators[jobId] = 0
		    inactiveReplicators[jobId] = inactiveReplicators[jobId] + 1
		elif re.search(re.compile("Callable.call.*with.*AsyncTestMess"), line) is not None:
		    messageId = line.split(", id_ ='")[1].split("'")[0]
		    sentMessages.append(messageId)
	    commLog.close()'''

def calcReplicators():
    global replicators
    if replicators is not None:
	return
    replicators = {}
    for peer in activityPeriods.keys():
	if peer == "0":
	    continue
	date = dt.today()
	lastTime = None
	try:
	    appcoreLog = open(os.path.join(rootDir, peer, "logs", "appcore.log"))
	except IOError:
	    continue
	for line in appcoreLog:
	    reps = None
	    try:
		time = timeFromLine(line)
	    except ValueError:
		continue
	    if (lastTime is not None and lastTime > time):
		date = date + timedelta(days=1)
	    if re.search(re.compile("Created new replication"), line) is not None:
		reps = line.split("with replicators: [")[1].split("]")[0].split(",")
	    elif re.search(re.compile("Replicators currently"), line) is not None:
		reps = line.split("in group: [")[1].split("]")[0].split(",")
	    if reps is not None:
		reps = map(peerFromCommAddress, reps)
		time = datetimeFromLine(line, date)
		lastTime = timeFromLine(line)
		if not replicators.has_key(peer):
		    replicators[peer] = []
		replicators[peer].append((time, reps))

def calcPeersAvailability():
    global testStartTimestamp
    global testEndTimestamp
    overallWholeTime = 0
    overallActivityTime = 0
    pp.pprint(activityPeriods)
    #Calculate peers' availability
    for peer, periods in activityPeriods.iteritems():
	if peer == "42":
	    pp.pprint(periods)
	activityTime = 0
	even = True
	last = 0
	for period in periods:
	    if even:
		last = period
		even = False
	    else:
		if period >= testStartTimestamp and last <= testEndTimestamp:
		    activityTime += (min(period, testEndTimestamp) - max(last, testStartTimestamp)).total_seconds()
		even = True

	wholeTime = (testEndTimestamp - testStartTimestamp).total_seconds()
	if peer != "0":
	    overallWholeTime += wholeTime
	    overallActivityTime += activityTime
	print("Peer " + peer + ":")
	print("Activity time: " + str(activityTime))
	print("Whole time: " + str(wholeTime))
	print("Activity percentage: " + "{0:.2f}".format((activityTime)/float(wholeTime) * 100) + "%")
	print("Original:")
	print("Activity time: " + str(activityTime))
	print("Whole time: " + str(wholeTime + 1800))
	print("Activity percentage: " + "{0:.2f}".format((activityTime)/float(wholeTime + 1800) * 100) + "%")
	print("------------------------------------------------")

    print("Overall activity:")
    print("Activity time: " + str(overallActivityTime))
    print("Whole time: " + str(overallWholeTime))
    print("Activity percentage: " + "{0:.2f}".format((overallActivityTime)/float(overallWholeTime) * 100) + "%")


def calcSynchroAvailability():
    global activityPeriods
    activityPeriodsBackup = copy.deepcopy(activityPeriods)

    calcSynchroPeers()

    if len(args.p) == 0:
	p = map(lambda x : int(x), activityPeriods.keys())
    else:
	p = map(lambda x : int(x), args.p)
   
    overallTime = 0
    overallActivityTime = 0 
    for peerToCheck in p:
	if peerToCheck == 0:
	    continue
	    print("------------------------------ NEXT PEER TO CHECK: " + str(peerToCheck) + " ------------------------")
	tempP = synchroPeers[str(peerToCheck)]
	activityPeriods = copy.deepcopy(activityPeriodsBackup)
	tempP.append(peerToCheck)
	availabilityTime = 0
	wholeTime = 0
	currentTime = 0
	lastTime = None
	availablePeers = []
	nextPeriod = 0
	while nextPeriod is not None:
	    nextPeriod = None
	    minPeer = None
	    for peer, periods in activityPeriods.iteritems():
		if len(periods) > 0 and (nextPeriod is None or periods[0] < nextPeriod):
		    nextPeriod = periods[0]
		    minPeer = peer
	    if nextPeriod is not None:
		if lastTime is not None:
		    currentTime += (nextPeriod - lastTime).total_seconds()
		    wholeTime += (nextPeriod - lastTime).total_seconds()
		lastTime = nextPeriod
		    
		activityPeriods[minPeer].pop(0)
		if int(minPeer) in availablePeers:
		    availablePeers.remove(int(minPeer))
		    if len(filter(lambda x : x in tempP, availablePeers)) == 0 and int(minPeer) in tempP:
			availabilityTime += currentTime
			if args.v:
			    print("Whole: " + str(wholeTime))
			    print("Avail: " + str(availabilityTime))
		else:
		    if len(filter(lambda x : x in tempP, availablePeers)) == 0 and int(minPeer) in tempP:
			currentTime = 0
		    availablePeers.append(int(minPeer))
		availablePeers.sort()
		if int(minPeer) in tempP and args.v:
		    print("Period: " + nextPeriod.strftime("%H:%M:%S"))
		    print(filter(lambda x : x in tempP, availablePeers))
	if len(filter(lambda x : x in tempP, availablePeers)) > 0:
	    availabilityTime += currentTime
	print("Average availability: " + "{0:.2f}".format((availabilityTime - 1800)/float(wholeTime - 1800) * 100) + "%")
	overallTime += wholeTime - 1800
	overallActivityTime += availabilityTime - 1800
    print("Average overall availability: " + "{0:.2f}".format((overallActivityTime)/float(overallTime) * 100) + "%")

def findClosestId(abbrJobId, jobIds, time):
    minDiff = None
    closestId = None
    for (abbrId, dtime) in jobIds:
	if time > dtime:
	    diff = (time - dtime).total_seconds()
	else:
	    diff = (dtime - time).total_seconds()
	if minDiff is None or diff < minDiff:
	    minDiff = diff
	    closestId = (abbrId, dtime)
    return closestId

def calcFailReasons(f):
    global testStartTimestamp
    calcReplicators()
    objectJobIds = {}
    objectAppKeys = {}
    readTimestamps = {}
    failedReads = set()

    GLOBAL_PARTS_NUMBER = int(f[0])
    LOCAL_PARTS_NUMBER = int(f[1])
    IN_SYMBOLS_NUMBER = int(f[2])

    apiLog = open(os.path.join(rootDir, "0", "logs", "api.log"), "r")
    lastTime = None
    date = testStartTimestamp.date()
    for line in apiLog:
	try:
	    time = timeFromLine(line)
	except ValueError:
	    pass
	if lastTime is not None and time is not None and lastTime.hour > time.hour:
	    date = date + timedelta(days=1)
	if re.search(re.compile("Retrieving files"), line) is not None:
	    address = line.split("files [")[1].split(",")[1].split("}")[0]
	    time = timeFromLine(line)
	    dtime = datetimeFromLine(line, date)
	    abbrJobId = line.split("WithSize:")[1].split(" ")[0]
	    objectJobIds[(abbrJobId, dtime)] = address
	    objectAppKeys[(abbrJobId, dtime)] = line.split("AppKey[")[1].split("]")[0]
	    readTimestamps[(abbrJobId, dtime)] = datetimeFromLine(line, date)
	if re.search(re.compile("to ask:"), line) is not None:
	    abbrJobId = line.split("WithSize:")[1].split(" ")[0]
	    jobId = findClosestId(abbrJobId, readTimestamps.keys(), datetimeFromLine(line, date))
	    if re.search(re.compile(re.escape(objectJobIds[jobId]) + "=\[\]"), line) is not None:
		failedReads.add(jobId)
	lastTime = time
    apiLog.close()
    testingLog = open(os.path.join(rootDir, "0", "logs", "testing.log"), "r")
    lastLine = None
    date = dt.today()
    for line in testingLog:
	try:
	    time = timeFromLine(line)
	except ValueError:
	    pass
	if lastTime is not None and time is not None and lastTime.hour > time.hour:
	    date = date + timedelta(days=1)
	if lastLine is not None and re.search(re.compile("GetModule.*failed"), line):
	    try:
		abbrJobId = lastLine.split("jobId_=")[1][:8]
	    except IndexError:
		continue
	    failedReads.add(findClosestId(abbrJobId, readTimestamps.keys(), datetimeFromLine(line, date)))
	lastLine = line

    print(replicators.keys())
    mdsFailed = 0
    trulyFailed = 0
    failedReads = sorted(failedReads, key=lambda x : readTimestamps[x])
    periodStart = None
    PERIOD_LENGTH = timedelta(minutes=10)
    for ident in failedReads:#sorted(readTimestamps.keys(), key=lambda x : readTimestamps[x]):
	if periodStart is None or periodStart + PERIOD_LENGTH < readTimestamps[ident]:
	    if periodStart is not None:
		print("\033[1;33m Next period ended: " + str(periodStart) + " - " + str(periodStart + PERIOD_LENGTH) + "\033[1;m")
		print("\033[1;33m Truly failed: " + str(trulyFailedPeriod) + "\033[1;m")
		("\033[1;33m MDS failed: " + str(mdsFailedPeriod) + "\033[1;m")
	    periodStart = readTimestamps[ident]
	    trulyFailedPeriod = 0
	    mdsFailedPeriod = 0
	if ident in failedReads:
	    print("[" + readTimestamps[ident].strftime("%H:%M:%S") + "] Read of object with address " + objectJobIds[ident] + " failed.")
	else:
	    print("[" + readTimestamps[ident].strftime("%H:%M:%S") + "] Read of object with address " + objectJobIds[ident] + " successfull.")
	print(readTimestamps[ident])
	appKey = objectAppKeys[ident]
	owner = appKey[0:len(appKey)/2]
	try:
	    ownerReps = replicators[owner][bisect(replicators[owner], (readTimestamps[ident], [])) - 1][1]
	except KeyError:
	    print("Error: no replicators of peer " + owner + " found")
	    continue
	print("Replicators of owner: " + str(ownerReps))
	print(str(readTimestamps[ident]))
	availabilityMask = []
	availabilityStr = []
	for replicator in ownerReps:
	    '''print "Replicator " + str(replicator) + ": "
	    print "Activity periods: "
	    pp.pprint(activityPeriods[replicator])
	    print "Bisect: " + str(bisect(activityPeriods[replicator], readTimestamps[abbrJobId]))'''
	    try:
		periodNum = bisect(activityPeriods[replicator], readTimestamps[ident])
		nearPeriodNum = bisect(activityPeriods[replicator], readTimestamps[ident] + timedelta(seconds=2))
		if periodNum % 2 == 0:
		    availabilityMask.append(False)
		    availabilityStr.append(str(replicator) + ": False (" +
			str((activityPeriods[replicator][periodNum] - readTimestamps[ident]).total_seconds()) + ")")
		elif nearPeriodNum % 2 == 0:
		    availabilityMask.append(False)
		    availabilityStr.append(str(replicator) + ": False ( - " +
			str((activityPeriods[replicator][nearPeriodNum] - readTimestamps[ident]).total_seconds()) + ")")
		else:
		    availabilityStr.append(str(replicator) + ": True (" +
			str((activityPeriods[replicator][periodNum] - readTimestamps[ident]).total_seconds()) + ")")
		    availabilityMask.append(True)
	    except KeyError:
		availabilityMask.append(None)
	
	result = str(availabilityStr[:GLOBAL_PARTS_NUMBER]) + ", "
	mdsRecoverable = len(filter(lambda x : x, availabilityMask))
	recoverableFragments = len(filter(lambda x : x, availabilityMask[:GLOBAL_PARTS_NUMBER]))
	index = GLOBAL_PARTS_NUMBER
	while index < len(availabilityMask):
	    result += str(availabilityStr[index:index + LOCAL_PARTS_NUMBER]) + ", "
	    recoverableFragments += 1 if (len(filter(lambda x : x, availabilityMask[index:index + LOCAL_PARTS_NUMBER])) > 0 and not availabilityMask[(index - GLOBAL_PARTS_NUMBER) / LOCAL_PARTS_NUMBER]) else 0
	    index += LOCAL_PARTS_NUMBER
	maskStringPrefix = ""
	maskStringPostfix = ""
	if recoverableFragments < IN_SYMBOLS_NUMBER:
	    trulyFailed += 1
	    trulyFailedPeriod += 1
	else:
	    maskStringPrefix += "\033[1;31m"
	    maskStringPostfix += "\033[1;m"
	if mdsRecoverable < IN_SYMBOLS_NUMBER:
	    mdsFailed += 1
	    mdsFailedPeriod += 1
	print(maskStringPrefix + "Availability mask: " + result + maskStringPostfix)
    
    if periodStart is not None:
	print("\033[1;33m Next period ended: " + str(periodStart) + " - " + str(periodStart + PERIOD_LENGTH) + "\033[1;m")
	print("\033[1;33m Truly failed: " + str(trulyFailedPeriod) + "\033[1;m")
	print("\033[1;33m MDS failed: " + str(mdsFailedPeriod) + "\033[1;m")

    lastObjectId = None
    truePositivesNumber = 0
    trueNegativesNumber = 0
    falsePositivesNumber = 0
    falseNegativesNumber = 0
    for ident in sorted(readTimestamps.keys(), key=lambda x : readTimestamps[x]):
	appKey = objectAppKeys[ident]
	owner = appKey[0:len(appKey)/2]
	objectId = int(objectJobIds[ident].split("[")[1].split("]")[0])
	if lastObjectId is not None and objectId > lastObjectId and objectId <= lastObjectId + 5:
	    continue
	lastObjectId = objectId
	print("Next read: " + str(readTimestamps[ident]) + ", peer: " + owner)
	try:
	    ownerReps = replicators[owner][bisect(replicators[owner], (readTimestamps[ident], [])) - 1][1]
	except KeyError:
	    continue
	availabilityMask = []
	for replicator in ownerReps:
	    try:
		if bisect(activityPeriods[replicator], readTimestamps[ident]) % 2 == 0: #or\
		    #bisect(activityPeriods[replicator], readTimestamps[abbrJobId] + timedelta(seconds=2)) % 2 == 0:
		    availabilityMask.append(False)
		else:
		    availabilityMask.append(True)
	    except KeyError:
		availabilityMask.append(None)
	
	recoverableFragments = len(filter(lambda x : x, availabilityMask[:GLOBAL_PARTS_NUMBER]))
	index = GLOBAL_PARTS_NUMBER
	while index < len(availabilityMask):
	    recoverableFragments += 1 if (len(filter(lambda x : x, availabilityMask[index:index + LOCAL_PARTS_NUMBER])) > 0 and not availabilityMask[(index - GLOBAL_PARTS_NUMBER) / LOCAL_PARTS_NUMBER]) else 0
	    index += LOCAL_PARTS_NUMBER
	if recoverableFragments < IN_SYMBOLS_NUMBER:
	    print("Failure")
	    if ident in failedReads:
		trueNegativesNumber += 1
	    else:
		falseNegativesNumber += 1
		print(availabilityMask)
	else:
	    print("Success") 
	    if ident in failedReads:
		falsePositivesNumber += 1
	    else:
		truePositivesNumber += 1
    print("True positives number: " + str(truePositivesNumber))
    print("True negatives number: " + str(trueNegativesNumber))
    print("False positives number: " + str(falsePositivesNumber))
    print("False negatives number: " + str(falseNegativesNumber))
    print("1st read prob.: " + "{0:.2f}".format(100 * float(truePositivesNumber + falseNegativesNumber)/
	(truePositivesNumber + trueNegativesNumber + falsePositivesNumber + falseNegativesNumber)) + "%")

    print("Truly failed: " + str(trulyFailed) + "/" + str(len(failedReads)))
    print("MDS failed: " + str(mdsFailed) + "/" + str(len(failedReads)))
    print("All reads: " + str(len(objectAppKeys.keys())))

def calcCombinationsAvail(c):
    global testStartTimestamp
    global testEndTimestamp
    calcReplicators()
    allReads = []

    date = dt.today()
    lastTime = None

    startTime = None
    lastReadTime = None
    order = []
    testingLog = open(os.path.join(rootDir, "0", "logs", "testing.log"), "r")
    for line in testingLog:
	try:
	    time = timeFromLine(line)
	except ValueError:
	    pass
	if lastTime is not None and time is not None and lastTime.hour > time.hour:
	    date = date + timedelta(days=1)
	if re.search(re.compile("Fetching next file"), line) is not None:
	    if startTime is None:
		startTime = datetimeFromLine(line, date)
	    appKey = line.split("[")[2].split("]")[0]
	    peerNum = appKey[0:len(appKey)/2]
	    order.append(peerNum)
	    if len(allReads) == 0 or datetimeFromLine(line, date) - allReads[-1] >= timedelta(seconds=1):
		allReads.append(datetimeFromLine(line, date))
	    lastReadTime = time
	lastTime = time
    
    startTime = allReads[0]#testStartTimestamp 
    allReads = []
    time = startTime + timedelta(hours=0)
    print(startTime)
    print(testEndTimestamp)
    while time < testEndTimestamp - timedelta(hours=0):
	allReads.append(time)
	time = time + timedelta(seconds=0.5)
    
    overallSuccNum = 0
    overallReads = 0
    checkedCombinations = 0
    combs = []
    avail = {}

    for peer in replicators.keys():
	succNum = 0
	for read in allReads:
	    availNum = 0
	    if (bisect(activityPeriods[peer], read)) % 2 == 1:
		#print "Available peer: " + str(rep)
		availNum += 1
	    if availNum >= 1:
		succNum += 1
	    #else:
		#print("Fail!")
	avail[peer] = float(succNum)/len(allReads)
    for i in xrange(250):
	combs.append(random.sample(replicators.keys(), int(c[0])))

    availNums = []
    i = 0
    for reps in combs:#map(lambda x : x[0][1], replicators.values()):#combs:
    #print("Next combination: " + str(reps))
	succNum = 0
	for read in allReads:
	    #print("Next read: " + str(read) + ", peer: " + str(order[i]))
	    availNum = 0
	    res = "Availability mask: ["
	    for rep in reps:#replicators[str(order[i])][0][1]:#reps:#combs[random.randint(0, len(combs) - 1)]:#replicators[replicators.keys()[random.randint(0, len(replicators.keys()) - 1)]][0][1]:#reps:#replicators[order[i]][0][1]:#reps:
		periodNum = bisect(activityPeriods[rep], read)
		if periodNum % 2 == 1: #and bisect(activityPeriods[rep], read + timedelta(seconds=2)) % 2 == 1:
		#if random.randint(0, 1000) < avail[rep] * 1000:
		    #print "Available peer: " + str(rep)
		    availNum += 1
		    res += str(rep) + ": True (" + str((activityPeriods[rep][periodNum] - read).total_seconds()) + "), "
		else:
		    res += str(rep) + ": False (" + str((activityPeriods[rep][periodNum] - read).total_seconds()) + "), "
	    availNums.append(availNum)
	    res += "]"
	    #print(res)
	    if availNum >= int(c[1]):
		#print("Success")
		#print(str(read) + "\ttrue")
		succNum += 1
	    #else:
		#print("Failure")
		#print(str(read) + "\tfalse")
		#print("Fail at " + str(read) + " with " + str(availNum) + "/" + c[1])
	i+= 1
	overallSuccNum += succNum
	overallReads += len(allReads)
	#print("Num: " + str(i), end='\r')
	print("Num: " + str(i))
	print("Succ num: " + str(succNum))
	print("All reads: " + str(len(allReads)))
	print("Subresult: " + "{0:.3f}".format(float(succNum)/len(allReads)))
	#print("Combination: " + str(reps) + ", success prob.: " + "{0:.3f}".format(float(succNum)/len(allReads)))
	#print("Prob: " + "{0:.3f}".format(float(succNum)/len(allReads)))
    #print(str(float(sum(availNums))/len(availNums)))
    #print("OVERALL succ count: " + str(overallSuccNum))
    #print("Overall reads: " + str(overallReads))
    print("{0:.2f}".format(100 * float(overallSuccNum)/overallReads))

def commonPart():
    overallCommonPartLength = 0
    overallTime = 0
    for peer, periods in activityPeriods.iteritems():
	for peer2, periods2 in activityPeriods.iteritems():
	    if peer == peer2:
		continue
	    i =0
	    j = 0
	    state1 = 0
	    state2 = 0
	    commonPartLength = 0
	    commonPartStart = None
	    while i < len(periods) or j < len(periods2):
		if i < len(periods) and (j == len(periods2) or periods[i] <= periods[j]):
		    state1 = (state1 + 1) % 2
		    if state1 == 1 and state2 == 1:
			commonPartStart = periods[i]
		    elif state1 == 0 and state2 == 1:
			commonPartLength += (periods[i] - commonPartStart).total_seconds()
		    i += 1
		elif j < len(periods2) and (i == len(periods) or periods[i] > periods[j]):
		    state2 = (state2 + 1) % 2
		    if state2 == 1 and state1 == 1:
			commonPartStart = periods[j]
		    elif state2 == 0 and state1 == 1:
			commonPartLength += (periods[j] - commonPartStart).total_seconds()
		    j += 1
	    overallCommonPartLength += commonPartLength
	    overallTime += (periods[-1] - periods[0]).total_seconds()
    print(overallCommonPartLength/overallTime)

def writeFailReason():
    for peer in activityPeriods.keys():
	recipients = {}
	writeTimestamps = {}
	failedWrites = set()
	print("-------------------------------------- PEER " + str(peer) + ": ---------------------------" )
	apiLog = open(os.path.join(rootDir, peer, "logs", "api.log"), "r")
	lastTime = None
	date = dt.today()
	for line in apiLog:
	    try:
		time = timeFromLine(line)
	    except ValueError:
		pass
	    if lastTime is not None and time is not None and lastTime.hour > time.hour:
		date = date + timedelta(days=1)
	    if re.search(re.compile("added recipient"), line) is not None:
		recipient = peerFromCommAddress(line.split("recipient: ")[1].split("\n")[0])
		time = timeFromLine(line)
		abbrJobId = line.split("Module:")[1].split(" ")[0]
		if not recipients.has_key(abbrJobId):
		    recipients[abbrJobId] = []
		recipients[abbrJobId].append(recipient)
		if not writeTimestamps.has_key(abbrJobId):
		    writeTimestamps[abbrJobId] = datetimeFromLine(line, date)
	    if re.search(re.compile("Write.*borting"), line) is not None:
		abbrJobId = line.split("Module:")[1].split(" ")[0]
		failedWrites.add(abbrJobId)
	    lastTime = time

	for abbrJobId in failedWrites:
	    if abbrJobId in failedWrites:
		print("[" + writeTimestamps[abbrJobId].strftime("%H:%M:%S") + "] Write with abbrJobId " + abbrJobId + " failed.")
	    else:
		print("[" + writeTimestamps[abbrJobId].strftime("%H:%M:%S") + "] Write with abbrJobId " + abbrJobId + " successfull.")
	    #appKey = objectJobIds[abbrJobId].split("AppKey[")[1].split("]")[0]
	    #owner = appKey[0:len(appKey)/2]
	    #ownerReps = replicators[owner][bisect(replicators[owner], (readTimestamps[abbrJobId], [])) - 1][1]
	    ownerReps = recipients[abbrJobId]
	    print("Replicators of owner: " + str(ownerReps))
	    availabilityMask = []
	    for replicator in ownerReps:
		if (bisect(activityPeriods[replicator], writeTimestamps[abbrJobId]) - 1) % 2 == 0:
		    availabilityMask.append(True)
		else:
		    availabilityMask.append(False)
	    
	    print("Availability mask: " + str(availabilityMask))

def calcCombinedAvailability():
    calcActivityPeriods()

def calcTestStartEndTimestamps():
    global testStartTimestamp
    global testEndTimestamp
    global runStartTimestamp
    testingLog = open(os.path.join(rootDir, "0", "logs", "testing.log"))
    date = dt.today()
    lastTime = None
    first = True
    for line in testingLog:
	try:
	    time = timeFromLine(line)
	except ValueError:
	    pass
	if lastTime is not None and time is not None and lastTime.hour > time.hour:
	    date = date + timedelta(days=1)
	lastTime = time
	if first:
	    runStartTimestamp = datetimeFromLine(line, date)
	    first = False
	if re.search(re.compile("Starting reader mode"), line) is not None:
	    if testStartTimestamp is None:
		testStartTimestamp = datetimeFromLine(line, date)
	elif re.search(re.compile("Stopping reader mode"), line) is not None:
	    if testEndTimestamp is None:
		testEndTimestamp = datetimeFromLine(line, date)
    testingLog.close()
    print(testStartTimestamp)
    print(testEndTimestamp)

class ActivityPeriod:
    def __init__(self, startPeriod):
	self.startPeriod = startPeriod
	self.period = None
	if self.startPeriod:
	    self.startUp = None
	    self.startChangeCalc = None
	else:
	    self.stopChangeCalc = None
	    self.shutDown = None
	    self.completeStop = None

    def __str__(self):
	if self.startPeriod:
	    return ("Start period:\
		\n\tstartUp: " + str(self.startUp) +
		"\n\tstartChangeCalc: " + str(self.startChangeCalc) +
		"\n\tperiod: " + str(self.period) + "\n")
	else:
	    return ("Stop period:\
		\n\tstopChangeCalc: " + str(self.stopChangeCalc) +
		"\n\tshutDown: " + str(self.shutDown) +
		"\n\tcompleteStop" + str(self.completeStop) +
		"\n\tperiod: " + str(self.period) + "\n")

    def __repr__(self):
	return self.__str__()

def calcPlannedPeriods(mainAppLog):
    mainTestingLog = open(mainAppLog)
    plannedPeriods = {}
    poolThreads = {}
    mainPhaseStarted = False
    date = dt.today()
    lastTime = None
    for line in mainTestingLog:
	try:
	    time = timeFromLine(line)
	except ValueError:
	    pass
	if lastTime is not None and time is not None and lastTime.hour > time.hour:
	    date = date + timedelta(days=1)
	lastTime = time
	if not mainPhaseStarted:
	    if re.search(re.compile("Starting main phase"), line) is not None:
		mainPhaseStarted = True
	else:
	    if re.search(re.compile("pool.*isRunning.* for "), line) is not None:
		threadNum = int(line.split("thread-")[1].split(" ")[0])
		peer = line.split("app ")[1].split("\n")[0]
		poolThreads[threadNum] = peer
	    elif re.search(re.compile("pool.*startUp"), line) is not None:
		peer = line.split("[")[2].split("]")[0]
		if not plannedPeriods.has_key(peer):
		    plannedPeriods[peer] = []
		plannedPeriods[peer].append(ActivityPeriod(True))
		plannedPeriods[peer][-1].startUp = datetimeFromLine(line, date) - timedelta(hours=2)
	    elif re.search(re.compile("pool.*Period"), line) is not None:
		period = float(line.split("Period: ")[1].split("\n")[0])
		threadNum = int(line.split("thread-")[1].split(" ")[0])
		peer = poolThreads[threadNum]
		if not plannedPeriods.has_key(peer):
		    plannedPeriods[peer] = []
		periods = plannedPeriods[peer]
		if len(periods) > 0 and periods[-1].startChangeCalc is None:
		    periods[-1].startChangeCalc = datetimeFromLine(line, date) - timedelta(hours=2)
		else:
		    periods.append(ActivityPeriod(False))
		    periods[-1].stopChangeCalc = datetimeFromLine(line, date) - timedelta(hours=2)
		periods[-1].period = period
	    elif re.search(re.compile("pool.*shutDown"), line) is not None:
		peer = line.split("[")[2].split("]")[0]
		if peer != "0":
		    plannedPeriods[peer][-1].shutDown = datetimeFromLine(line, date) - timedelta(hours=2)
	    elif re.search(re.compile("after waitFor"), line) is not None:
		threadNum = int(line.split("thread-")[1].split(" ")[0])
		peer = poolThreads[threadNum]
		if peer != "0":
		    plannedPeriods[peer][-1].completeStop = datetimeFromLine(line, date) - timedelta(hours=2)
		
    mainTestingLog.close()
    return plannedPeriods

def calcSimulationStats(activityPeriods, plannedPeriods):
    global testStartTimestamp
    global testEndTimestamp
    startDelays = {}
    afterStartDelays = {}
    shutDownDelays = {}
    complStopDelays = {}
    periodsMap = {}
    activityDelays = {}
    inactivityDelays = {}

    overallActivityTime = 0
    overallWholeTime = 0
    for peer in plannedPeriods.keys():
	print("Peer: " + str(peer))
	startDelays[peer] = []
	afterStartDelays[peer] = []
	shutDownDelays[peer] = []
	complStopDelays[peer] = []
	periodsMap[peer] = []
	activityDelays[peer] = []
	inactivityDelays[peer] = []

	periods = activityPeriods[peer]
	pPeriods = plannedPeriods[peer]

	periodToSearchFor = None
	pPeriodIx = 0
	if pPeriods[pPeriodIx].startPeriod:
	    periodToSearchFor = pPeriods[pPeriodIx].startUp
	else:
	    periodToSearchFor = pPeriods[pPeriodIx].stopChangeCalc
	periodIx = 0
	while periods[periodIx] + timedelta(seconds=0.5) < periodToSearchFor:
	    periodIx += 1

	activityTime = 0
	wholeTime = 0
	periodSum = 0
	activityPeriodsSum = 0
	allActivityDelaysSum = 0
	lastPeriod = testStartTimestamp
	while pPeriodIx < len(pPeriods):
	    activityData = pPeriods[pPeriodIx]
	    if activityData.period is not None:
		if activityData.startPeriod:
		    periodsMap[peer].append((activityData.startUp, activityData.period))
		else:
		    periodsMap[peer].append((activityData.shutDown, -activityData.period))
	    if activityData.startPeriod:
		if pPeriods[pPeriodIx - 1].period is not None:
		    print("inactivity delay: " + str((periods[periodIx] - periods[periodIx - 1]).total_seconds() - pPeriods[pPeriodIx - 1].period * 60) + ", " + str(periods[periodIx - 1]) + " - " + str(activityData.startUp) + "; " + str(pPeriods[pPeriodIx - 1].period * 60))

		    inactivityDelays[peer].append((periods[periodIx] - periods[periodIx - 1]).total_seconds() - pPeriods[pPeriodIx - 1].period * 60)
		startDelays[peer].append((periods[periodIx] - activityData.startUp).total_seconds())
		if activityData.startChangeCalc is not None:
		    afterStartDelays[peer].append((activityData.startChangeCalc - periods[periodIx]).total_seconds())
		    print("Added new after start delay: " + str((activityData.startChangeCalc - periods[periodIx]).total_seconds()))
		if pPeriods[pPeriodIx].period is not None:
		    activityPeriodsSum += pPeriods[pPeriodIx].period
		    print("Added activity period: " + str(pPeriods[pPeriodIx].period) + ", sum: " + str(activityPeriodsSum))
	    else:
		if activityData.shutDown is not None:
		    shutDownDelays[peer].append((activityData.shutDown - activityData.stopChangeCalc).total_seconds())
		    complStopDelays[peer].append((activityData.completeStop - activityData.stopChangeCalc).total_seconds())
		    if pPeriods[pPeriodIx - 1].period is not None:
			print("activity delay: " + str((periods[periodIx] - periods[periodIx - 1]).total_seconds() - pPeriods[pPeriodIx - 1].period * 60) + ", " + str(periods[periodIx - 1]) + " - " + str(activityData.shutDown) + "; " + str(pPeriods[pPeriodIx - 1].period * 60))
			allActivityDelaysSum += (periods[periodIx] - periods[periodIx - 1]).total_seconds() - pPeriods[pPeriodIx - 1].period * 60
			activityDelays[peer].append((periods[periodIx] - periods[periodIx - 1]).total_seconds() - pPeriods[pPeriodIx - 1].period * 60)
		#print(str(lastPeriod) + " - " + str(periods[periodIx]))
		if periodIx > 0:
		    activityTime += (periods[periodIx] - lastPeriod).total_seconds()
		    print("Increased activity time: " + str((periods[periodIx] - lastPeriod).total_seconds()) + ", current: " + str(activityTime))
	    if periodIx > 0:
		wholeTime += (periods[periodIx] - lastPeriod).total_seconds()
	    if pPeriods[pPeriodIx].period is not None:
		periodSum += pPeriods[pPeriodIx].period
	    lastPeriod = periods[periodIx]
	    pPeriodIx += 1
	    periodIx += 1
	activityData = pPeriods[pPeriodIx - 1]
	if activityData.startPeriod:
	    activityTime += (testEndTimestamp - periods[periodIx - 1]).total_seconds()
	wholeTime += (testEndTimestamp - periods[periodIx - 1]).total_seconds()
	print("Activity periods time: " + str(activityPeriodsSum * 60))
	print("Periods time: " + str(periodSum * 60))
	print("Start delays sum: " + str(sum(startDelays[peer])))
	print("After start delays sum: " + str(sum(afterStartDelays[peer])))
	print("All activity delays sum: " + str(allActivityDelaysSum))
	print("Activity time: " + str(activityTime))
	print("Whole time: " + str(wholeTime))
	print("Activity: " + str(float(activityTime) / wholeTime))
	print(afterStartDelays[peer])
	print(sum(afterStartDelays[peer])/len(afterStartDelays[peer]))
	overallActivityTime += activityTime
	overallWholeTime += wholeTime
    print("Overall activity: " + str(float(overallActivityTime) / overallWholeTime))

    #pp.pprint(startDelays)
    #pp.pprint(afterStartDelays)
    #pp.pprint(shutDownDelays)
    #pp.pprint(complStopDelays)
    #pp.pprint(periodsMap)

    print("Average start delay:")
    allStartDelays = reduce(lambda x, y: x + y, startDelays.values())
    print(float(sum(allStartDelays))/len(allStartDelays))
 
    print("Average after start delay:")
    allAfterStartDelays = reduce(lambda x, y: x + y, afterStartDelays.values())
    print(float(sum(allAfterStartDelays))/len(allAfterStartDelays))

    print("Average shutDown delay:")
    allShutDownDelays = reduce(lambda x, y: x + y, shutDownDelays.values())
    print(float(sum(allShutDownDelays))/len(allShutDownDelays))

    print("Average complStop delay:")
    allComplStopDelays = reduce(lambda x, y: x + y, complStopDelays.values())
    print(float(sum(allComplStopDelays))/len(allComplStopDelays))

    print("Average activity delay:")
    allActivityDelays = reduce(lambda x, y: x + y, activityDelays.values())
    print(float(sum(allActivityDelays))/len(allActivityDelays))

    print("Average inactivity delay:")
    allInactivityDelays = reduce(lambda x, y: x + y, inactivityDelays.values())
    print(float(sum(allInactivityDelays))/len(allInactivityDelays))

    for peer in periodsMap.keys():
	print("Peer " + str(peer) + ": ")
	periods = periodsMap[peer]
	startTime = periods[0][0]
	periodIx = 0
	time = startTime
	nextChangeTime = None
	while periodIx < len(periods):
	    if nextChangeTime is None or time >= nextChangeTime:
		period = periods[periodIx][1]
		print("-------Calculated time: " + str(time))
		print("------Real time: " + str(periods[periodIx][0]))
		print("Period: " + str(periods[periodIx][1]))
		if period > 0:
		    time += timedelta(seconds=2.608)
		    time += timedelta(seconds=3.800)
		    nextChangeTime = time + timedelta(seconds=period*60)
		else:
		    period = -period
		    nextChangeTime = time + timedelta(seconds=period*60)
		    time += timedelta(seconds=30)
		periodIx += 1
	    time += timedelta(seconds=15.01)
	print("End time: " + str(time))
	print("Real end time: " + str(periods[-1][0]))

def calcFanInStats(rootDir):
    allFanIns = []
    for _, dirs, _ in walklevel(rootDir, 0):
	for dr in dirs:
	    fanIns = {}
	    testingLog = open(os.path.join(rootDir, dr, "logs", "testing.log"), "r")
	    for line in testingLog:
		if re.search(re.compile("RecreateObject.*Received"), line) is not None:
		    abbrJobId = line.split(":")[3].split(" ")[0]
		    if not fanIns.has_key(abbrJobId):
			fanIns[abbrJobId] = {}
		    sizes = line.split("objectsSizes_={")[1].split("}")[0]
		    objects = map(lambda x : x.split("[")[1].split("]")[0], sizes.split(","))
		    for obj in objects:
			if not fanIns[abbrJobId].has_key(obj):
			    fanIns[abbrJobId][obj] = 0
			fanIns[abbrJobId][obj] += 1
		elif re.search(re.compile("Ending module"), line) is not None:
		    lastAbbrJobId = line.split(":")[3].split(" ")[0]
		elif re.search(re.compile("recreation of.*number of objects:"), line) is not None:
		    numberOfObjects = int(line.split("objects: ")[1].split("\n")[0])
		    if numberOfObjects > 0 and fanIns.has_key(lastAbbrJobId):
			print("Fan in of peer " + dr + "; value: " + str(fanIns[lastAbbrJobId]) + "; abbrJobId: " + lastAbbrJobId)
			allFanIns += fanIns[lastAbbrJobId].values()
    print("Average fan in: " + str(float(sum(allFanIns))/len(allFanIns) if len(allFanIns) > 0 else 0))
    print("Occurences of 1: " + str(allFanIns.count(1)) + "; of 5: " + str(allFanIns.count(5)))
    allFanIns.sort()
    #pp.pprint(allFanIns)

parser = argparse.ArgumentParser(description='Analyze NebuloStore logs for asynchronous messages test.')
parser.add_argument("rootDir", metavar='rootDir', help='Logs root directory')
parser.add_argument("-l", metavar="mainAppLog", nargs=1, help="Main app testing log file path")
parser.add_argument("-a", action="store_true", help="Calculate peers' availability")
parser.add_argument("-f", action="store", nargs=3, help="Calculate fail reasons for reads; arguments: gl. parts number, local parts number, is symbols number")
parser.add_argument("-p", metavar="peer", action="store", nargs="*", help="Calculate list of available \
    peers in each of the periods. The arguments mean peers that should be included in summary")
parser.add_argument("-i", action="store_true", help="Calculate fan-in statistics")
parser.add_argument("-v", action="store_true")
parser.add_argument("-c", action="store", nargs=2)
parser.add_argument("-m", action="store_true")
parser.add_argument("-w", action="store_true")
parser.add_argument("-A", action="store_true")

args = parser.parse_args()

rootDir = args.rootDir

unavailSPReason = []
activityPeriods = {}
inactiveReplicators = {}
startTimes = {}
sentMessages = []

calcTestStartEndTimestamps()
calcActivityPeriods()

if args.l:
    plannedPeriods = calcPlannedPeriods(args.l[0])
    pp.pprint(plannedPeriods)
    calcSimulationStats(activityPeriods, plannedPeriods)

if args.a:
    calcPeersAvailability()

#Analyze fail reasons
if args.f:
    calcFailReasons(args.f)

#Calculate history of peers' availability
if args.p is not None:
    calcSynchroAvailability()

if args.c:
    calcCombinationsAvail(args.c)

if args.m:
    commonPart()

if args.w:
    writeFailReason()

if args.i:
    calcFanInStats(rootDir)

#pp.pprint({k : (lambda x : reduce(lambda c, y : (y, c[1] + [round((y - c[0]).total_seconds()/60)]), x[1:], (x[0], [])))(v) for k, v in activityPeriods.iteritems()})
