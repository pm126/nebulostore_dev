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

def calcActivityPeriods():
    for _, dirs, _ in walklevel(rootDir, 0):
	for dr in dirs:
	    try:
		peersLog = open(os.path.join(rootDir, dr, "logs", "peers.log"), "r")
	    except IOError:
		print "Could not read the peers log for peer " + dr
		continue
	    activityPeriods[dr] = []
	    closed = False
	    first = True
	    date = dt.today()
	    for line in peersLog:
		try:
		    if first:
			first = False
			startTime = datetimeFromLine(line, date)
			startTimes[dr] = startTime
			activityPeriods[dr].append(startTime)
		    if closed:
			tmpTime = timeFromLine(line)
			if (tmpTime.hour < activityPeriods[dr][-1].time().hour):
			    date = date + timedelta(days = 1)
			activityPeriods[dr].append(datetimeFromLine(line, date))
			closed = False
		    elif re.search(re.compile("Finished quitNebuloStore.*"), line) is not None:
			tmpTime = timeFromLine(line)
			if (tmpTime < activityPeriods[dr][-1].time()):
			    date = date + timedelta(days = 1)
			activityPeriods[dr].append(datetimeFromLine(line, date))
			closed = True
		except (IndexError, ValueError):
		    continue
	    peersLog.close()
	    #Calculate inactive replicators for given jobId of SendAsynchronousMessageForPeerModule
	    try:
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
	    commLog.close()

def calcSynchroPeers():
    global synchroPeers
    if synchroPeers is not None:
	return
    synchroPeers = {}
    for peer in activityPeriods.keys():
	if peer == "0":
	    continue
	synchroPeers[peer] = []
	commLog = open(os.path.join(rootDir, peer, "logs", "comm.log"))
	found = False
	for line in commLog:
	    if re.search(re.compile("\[" + peer + peer + "\]"), line) is not None:
		found = True
	    if re.search(re.compile("\tSynchroGroup: \["), line) is not None and found:
		found = False
		sps = line.split("[")[1].split("]")[0]
		sps = sps.split(", ")
		for sp in sps:
		    if sp != "":
			synchroPeers[peer].append(int(peerFromCommAddress(sp)))
	if args.v:
	    print "Peer " + peer + ":"
	    print synchroPeers[peer]

def calcPeersAvailability():
    overallWholeTime = 0
    overallActivityTime = 0
    #Calculate peers' availability
    for peer, periods in activityPeriods.iteritems():
	activityTime = 0
	even = True
	last = 0
	for period in periods:
	    if even:
		last = period
		even = False
	    else:
		activityTime += (period - last).total_seconds()
		even = True

	wholeTime = (periods[-1] - periods[0]).total_seconds() - 1500
	if peer != "0":
	    overallWholeTime += wholeTime
	    overallActivityTime += (activityTime - 1500)
	print "Peer " + peer + ":"
	print "Activity time: " + str(activityTime - 1500)
	print "Whole time: " + str(wholeTime)
	print "Activity percentage: " + "{0:.2f}".format((activityTime - 1500)/float(wholeTime) * 100) + "%"

    print "Overall activity:"
    print "Activity time: " + str(overallActivityTime)
    print "Whole time: " + str(overallWholeTime)
    print "Activity percentage: " + "{0:.2f}".format((overallActivityTime)/float(overallWholeTime) * 100) + "%"


def calcFailReasons():
    report = open(os.path.join(rootDir, "report.txt"), "r")

    peerNum = -1
    sendTimes = {}
    asyncMessages = {}
    lostMessages = {}
    lMessages = []
    synchroPeers = {}
    abbrJobIds = {}
    msgsJobIds = {}
    for line in report:
	if re.search(re.compile("Peer 0000.*:"), line) is not None:
	    peerNum = peerFromCommAddress(line.split()[2])
	    peerLogPath = os.path.join(rootDir, peerNum, "logs")
	elif re.search(re.compile("Lost.*ids:"), line) is not None:
	    idsString = line.split(":")[1][2:].rstrip("\n]")
	    messages = idsString.split(", ")
	    lostMessages[peerNum] =  [m for m in messages if m != ""]
	    try:
		asyncLog = open(os.path.join(peerLogPath, "async.log"))
		for asyncLogLine in asyncLog:
		    #Find messages that were sent asynchronously with abbreviation of jobId of sending module
		    if re.search(re.compile("SendAsynchronousMessages.*Starting.*for:"), asyncLogLine) is not None:
			destinationPeer = peerFromCommAddress(asyncLogLine.split("for: ")[1])
			messageId = asyncLogLine.split("e id: ")[1].split("}")[0]
			if not asyncMessages.has_key(destinationPeer):
			    asyncMessages[destinationPeer] = []
			asyncMessages[destinationPeer].append(messageId)
			abbrJobId = asyncLogLine.split("SendAsynchronousMessagesForPeerModule:")[1].split(" |")[0]
			aMessage = TestMessage(messageId, peerNum, destinationPeer, abbrJobId)
			lMessages.append(aMessage)
			abbrJobIds[abbrJobId] = aMessage
		    #Find synchro-peers of the peer
		    elif re.search(re.compile("ChangeSynchroPeerSetModule.*with synchro-peers to add"),\
			    asyncLogLine) is not None:
			synchroPeer = peerFromCommAddress(asyncLogLine.split("add: [")[1].split("]")[0])
			if not synchroPeers.has_key(peerNum):
			    synchroPeers[peerNum] = []
			synchroPeers[peerNum].append(synchroPeer)
		    #Find full jobId of sending module and match it with proper message
		    elif re.search(re.compile("SendAsynchronousMessages.*Received.*ValueDHTMessage"), asyncLogLine) is not None:
			abbrJobId = asyncLogLine.split("SendAsynchronousMessagesForPeerModule:")[1].split(" |")[0]
			jobId = asyncLogLine.split("jobId_='")[1].split("',")[0]
			msgsJobIds[jobId] = abbrJobIds[abbrJobId]
			abbrJobIds[abbrJobId].jobId = jobId
		asyncLog.close()
	    except IOError:
		print("Could not open logs for " + os.path.join(peerLogPath, "comm.log") + ", skipping")

    #Generate set of all lost messages' ids
    lostM2 = set(reduce(operator.add, lostMessages.values(), []))
    lostNumber = 0
    #Finding send timestamps
    for _, dirs, _ in walklevel(rootDir, 0):
	for dr in dirs:
	    peerLogPath = os.path.join(rootDir, dr, "logs")
	    try:
		commLog = open(os.path.join(peerLogPath, "comm.log"), "r")
		date = dt.today()
		lastTime = None
		for line in commLog:
		    if re.search(re.compile("MessageSenderCallable.call.*with.*AsyncTestMessage"), line) is not None:
			messageId = line.split("id_ ='")[1].split("'")[0]
			if messageId in map(lambda m : m.idn, lMessages):
			    if lastTime is not None and timeFromLine(line) < lastTime:
				date = date + timedelta(days = 1)
			    sendTimes[messageId] = datetimeFromLine(line, date)
			    lastTime = timeFromLine(line)
		commLog.close()

		asyncLog = open(os.path.join(peerLogPath, "async.log"), "r")
		for line in asyncLog:
		    if re.search(re.compile("SendAsynchronousMessages.*Starting"), line):
			messageId = line.split("e id: ")[1].split("}")[0]
			if  messageId in lostM2:
			    lostNumber += 1
	    except IOError:
		print "Could not open comm logs for peer " + str(peerNum)

    #Finding messages lost despite availability of the destination peer
    strangeMessages = []
    for message in lMessages:
	if sendTimes.has_key(message.idn):
	    sendTime = sendTimes[message.idn]
	    periods = activityPeriods[message.receiver]
	    periodNumber = bisect(periods, sendTime)
	    if periodNumber % 2 == 1:
		backPeriod = bisect(periods, sendTime + timedelta(seconds=90))
		forwardPeriod = bisect(periods, sendTime - timedelta(seconds=90))
		if (backPeriod == periodNumber) and (forwardPeriod == periodNumber):
		    strangeMessages.append(message)

    #unavailable synchro-peers

    for message in lMessages:
	if sendTimes.has_key(message.idn) and message.idn in lostM2:
	    sendTime = sendTimes[message.idn]
	    if synchroPeers.has_key(message.receiver):
		sps = synchroPeers[message.receiver]
		activeSps = []
		for sp in sps:
		    periods = activityPeriods[sp]
		    periodNumber = bisect(periods, sendTime)
		    backPeriod = bisect(periods, sendTime + timedelta(seconds=90))
		    forwardPeriod = bisect(periods, sendTime - timedelta(seconds=90))
		    if periodNumber % 2 == 1 and backPeriod == periodNumber and\
			    forwardPeriod == periodNumber:
			activeSps.append(sp)
		if len(activeSps) == 0:
		    unavailSPReason.append(message.idn)
	    else:
		print message.receiver + " not in synchroPeers!"

    #alternative unavail sps
    unavAlt = []
    for message in lMessages:
	if message.idn in lostM2 and message.jobId != "":
	    if inactiveReplicators.has_key(message.jobId) and inactiveReplicators[message.jobId] >= 5:
		unavAlt.append(message.idn)

    print synchroPeers
    lostM = set(reduce(operator.add, asyncMessages.values(), []))
    print str(len(sentMessages)) + " messages were sent"
    print str(len(lostM2)) + " messages were lost"
    print str(len(lostM)) + " messages were sent asynchronously"
    print str(len(lostM2 - lostM)) + " messages were not sent asynchronously:"
    print random.sample(lostM2 - lostM, min(5, len(lostM2 - lostM)))
    print str(len(lostM2 - (lostM2 - lostM))) + " messages were lost during this part"
    #print lostM2 - lostM
    print str(len(lostM - lostM2)) + " messages sent asynchronously were received"
    #print lostM - lostM2
    print str(len(strangeMessages)) + " strange messages found:"
    print random.sample(strangeMessages, min(5, len(strangeMessages)))
    print str(len(unavailSPReason)) + " messages were not sent because of unavailability of the synchro-peers (first reason):"
    print random.sample(unavailSPReason, min(5, len(unavailSPReason)))
    print str(len(unavAlt)) + " messages were not sent because of unavailability of the synchro-peers (second reason):"
    print unavAlt[0:min(5, len(unavAlt))]
    unavailOnlyAltReason = set(unavAlt) - set(unavailSPReason)
    print str(len(unavailOnlyAltReason)) + " messages were not sent because of only second type of synchro-peers' unavailability:"
    print random.sample(unavailOnlyAltReason, min(5, len(unavailOnlyAltReason)))
    unavailOnlyFirstReason = set(unavailSPReason) - set(unavAlt)
    print str(len(unavailOnlyFirstReason)) + " messages were not sent because of only first type of synchro-peers' unavailability:"
    print random.sample(unavailOnlyFirstReason, min(5, len(unavailOnlyFirstReason)))
    unavailSPReason = set(unavailSPReason) | set(unavAlt)
    print str(len(unavailSPReason)) + " messages were not sent because of unavailability of the synchro-peers (overall):"
    print random.sample(unavailSPReason, min(5, len(unavailSPReason)))
    unknownReason = lostM2 - (lostM2 - lostM) - unavailSPReason
    print str(len(unknownReason)) + " messages were lost because of an unknown reason:"
    print random.sample(unknownReason, min(5, len(unknownReason)))

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
	print "------------------------------ NEXT PEER TO CHECK: " + str(peerToCheck) + " ------------------------"
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
			    print "Whole: " + str(wholeTime)
			    print "Avail: " + str(availabilityTime)
		else:
		    if len(filter(lambda x : x in tempP, availablePeers)) == 0 and int(minPeer) in tempP:
			currentTime = 0
		    availablePeers.append(int(minPeer))
		availablePeers.sort()
		if int(minPeer) in tempP and args.v:
		    print "Period: " + nextPeriod.strftime("%H:%M:%S")
		    print filter(lambda x : x in tempP, availablePeers)
	if len(filter(lambda x : x in tempP, availablePeers)) > 0:
	    availabilityTime += currentTime
	print "Availability time: " + str(availabilityTime)
	print "Whole time: " + str(wholeTime)
	print "Average availability: " + "{0:.2f}".format((availabilityTime - 1500)/float(wholeTime - 1500) * 100) + "%"
	overallTime += wholeTime - 1500
	overallActivityTime += availabilityTime - 1500
    print "Average overall availability: " + "{0:.2f}".format((overallActivityTime)/float(overallTime) * 100) + "%"

def calcSynchronizationStats():
    calcSynchroPeers()
    commonPeriodsMap = {}
    fullSyncMap = {}

    for peer, syncPeers in synchroPeers.iteritems():
	for peer1 in set(syncPeers) | {int(peer)}:
	    for peer2 in set(syncPeers) | {int(peer)}:
		if peer1 == peer2 or peer1 == 0 or peer2 == 0:
		    continue

		print "Searching for periods for peers " + str(peer1) + " and " + str(peer2)

		periods1 = copy.deepcopy(activityPeriods[str(peer1)])
		periods2 = copy.deepcopy(activityPeriods[str(peer2)])

		state1 = False
		state2 = False

		nextPeriod = 0
		commonPeriodStart = None
		commonPeriods = []
		while len(periods1) > 0 or len(periods2) > 0:
		    if len(periods2) == 0 or (len(periods1) > 0 and periods1[0] < periods2[0]):
			nextPeriod = periods1.pop(0)
			state1 = not state1
			if state1 == False and commonPeriodStart is not None:
			    commonPeriods.append((commonPeriodStart, nextPeriod))
			    commonPeriodStart = None
		    else:
			nextPeriod = periods2.pop(0)
			state2 = not state2
			if state2 == False and commonPeriodStart is not None:
			    commonPeriods.append((commonPeriodStart, nextPeriod))
			    commonPeriodStart = None
		    if state1 and state2:
			commonPeriodStart = nextPeriod
		    commonPeriodsMap[(peer1, peer2)] = commonPeriods


    startedRequests = {}
    secondStepRequests = {}
    thirdStepRequests = {}
    finishedRequests = {}
    abbrJobIdsMap = {}
    groupOwners = {}
    for peer, syncPeers in synchroPeers.iteritems():
	date = dt.today()
	lastTime = None
	asyncLog = open(os.path.join(rootDir, str(peer), "logs", "async.log"), "r")
	for line in asyncLog:
	    try:
		time = timeFromLine(line)
		if lastTime is not None and lastTime.hour > time.hour:
		    date = date + timedelta(days = 1)
		lastTime = time
	    except ValueError:
		continue
	    if re.search(re.compile("Sending get as"), line) is not None:
		groupOwner = int(peerFromCommAddress(line.split("for ")[1].split(" to")[0]))
		peerToAsk = int(peerFromCommAddress(line.split("to peer ")[1].split(" as the ")[0]))
		if peerToAsk == 0:
		    continue
		jobId = line.split("jobId_ = ")[1].split(", ")[0]
		abbrJobId = line.split("e:")[1].split(" |")[0]
		abbrJobIdsMap[abbrJobId] = jobId
		groupOwners[abbrJobId] = groupOwner
		startedRequests[(abbrJobId, int(peer), peerToAsk)] = datetimeFromLine(line, date)
	    if re.search(re.compile("Retrieved next messages"), line) is not None:
		abbrJobId = line.split("e:")[1].split(" |")[0]
		sender = int(peerFromCommAddress(line.split("from ")[1].split("\n")[0]))
		if sender == 0:
		    continue
		thirdStepRequests[(abbrJobId, int(peer), sender)] = \
		    (startedRequests[(abbrJobId, int(peer), sender)], datetimeFromLine(line, date))
	asyncLog.close()
	    
    for peer, syncPeers in synchroPeers.iteritems():
	date = dt.today()
	lastTime = None
	asyncLog = open(os.path.join(rootDir, str(peer), "logs", "async.log"), "r")
	for line in asyncLog:
	    try:
		time = timeFromLine(line)
		if lastTime is not None and lastTime.hour > time.hour:
		    date = date + timedelta(days = 1)
		lastTime = time
	    except ValueError:
		continue
	    if re.search(re.compile("Received request for async"), line) is not None:
		abbrJobId = line.split("e:")[1].split(" |")[0]
		requestingPeer = int(peerFromCommAddress(line.split("from ")[1].split(" for")[0]))
		if requestingPeer == 0:
		    continue
		key = (abbrJobId, requestingPeer, int(peer))
		if thirdStepRequests.has_key(key):
		    thirdStepPeriods = thirdStepRequests[key]
		    thirdStepRequests[key] = (thirdStepPeriods[0], datetimeFromLine(line, date),
			thirdStepPeriods[1])
		secondStepRequests[key] = (startedRequests[key], datetimeFromLine(line, date))
	    if re.search(re.compile("Received asynchronous messages "), line) is not None:
		abbrJobId = line.split("e:")[1].split(" |")[0]
		requestingPeer = int(peerFromCommAddress(line.split("from ")[1].split(" for")[0]))
		if requestingPeer == 0:
		    continue
		key = (abbrJobId, requestingPeer, int(peer))
		finishedRequests[key] = thirdStepRequests[key] + (datetimeFromLine(line, date),)
	asyncLog.close()

    goodPeriods = set()
    for key, value in finishedRequests.iteritems():
	commonPeriods = None
	if commonPeriodsMap.has_key((key[1], key[2])):
	    commonPeriods = commonPeriodsMap[(key[1], key[2])]
	elif commonPeriodsMap.has_key((key[2], key[1])):
	    commonPeriods = commonPeriodsMap[(key[2], key[1])]
	else:
	    continue
	period = None
	for (start, end) in commonPeriods:
	    if start <= value[1] and end >= value[3]:
		period = (start, end)
		break
	if period is not None:
	    if key[1] < key[2]:
		peer1 = key[1]
		peer2 = key[2]
	    else:
		peer1 = key[2]
		peer2 = key[1]

	    goodPeriods.add((peer1, peer2, period[0], period[1]))

    badPeriods = set()
    for key, value in commonPeriodsMap.iteritems():
	peer1 = key[0]
	peer2 = key[1]
	if key[0] > key[1]:
	    peer1 = key[1]
	    peer2 = key[0]

	for (start, end) in value:
	    if not (peer1, peer2, start, end) in goodPeriods:
		badPeriods.add((peer1, peer2, start, end))

    for (peer1, peer2, start, end) in badPeriods:
	if math.floor((end - start).total_seconds()/30) == 31.0:
	    pp.pprint((peer1, peer2, start, end))

    periodsOverall = {}
    successfulPeriods = {}
    for (peer1, peer2, start, end) in goodPeriods:
	key = math.floor((end - start).total_seconds()/30)
	if not periodsOverall.has_key(key):
	    periodsOverall[key] = 0
	periodsOverall[key] = periodsOverall[key] + 1
	if not successfulPeriods.has_key(key):
	    successfulPeriods[key] = 0
	successfulPeriods[key] = successfulPeriods[key] + 1

    for (peer1, peer2, start, end) in badPeriods:
	key = math.floor((end - start).total_seconds()/30)
	if not periodsOverall.has_key(key):
	    periodsOverall[key] = 0
	periodsOverall[key] = periodsOverall[key] + 1

    print periodsOverall
    print successfulPeriods

    for key, value in periodsOverall.iteritems():
	succPeriodsNum = 0
	if successfulPeriods.has_key(key):
	    succPeriodsNum = successfulPeriods[key]
	print "Period " + str(key)
	print "\t" + "{0:.2f}".format((succPeriodsNum)/float(value) * 100) + "%"

    

parser = argparse.ArgumentParser(description='Analyze NebuloStore logs for asynchronous messages test.')
parser.add_argument("rootDir", metavar='rootDir', help='Logs root directory')
parser.add_argument("-a", action="store_true", help="Calculate peers' availability")
parser.add_argument("-f", action="store_true", help="Calculate fail resons for messages")
parser.add_argument("-p", metavar="peer", action="store", nargs="*", help="Calculate list of available \
    peers in each of the periods. The arguments mean peers that should be included in summary")
parser.add_argument("-s", action="store_true")
parser.add_argument("-v", action="store_true")

args = parser.parse_args()

rootDir = args.rootDir

unavailSPReason = []
activityPeriods = {}
inactiveReplicators = {}
startTimes = {}
sentMessages = []

calcActivityPeriods()

if args.a:
    calcPeersAvailability()

#Analyze fail reasons
if args.f:
    calcFailReasons()

#Calculate history of peers' availability
if args.p is not None:
    calcSynchroAvailability()

#Calculate synchronization statistics
if args.s:
    calcSynchronizationStats()
