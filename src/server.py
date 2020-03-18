from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
import xmlrpc.client
import hashlib
import argparse
import random
import threading

FOLLOWER, CANDIDATE, LEADER = 0, 1, 2
HEATBEATTIME = 0.2



class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass



# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""

    blockData = BlockStore[h]
    return blockData

# Puts a block
def putblock(b):
    """Puts a block"""
    b = b.data
    global BlockStore, BlockStore_lock
    BlockStore_lock.acquire()
    hash_value = hashlib.sha256(b).hexdigest()
    BlockStore[hash_value] = b


    BlockStore_lock.release()
    return True

# Given a list of blocks, return the subset that are on this server
def hasblocks(blocklist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")

    return blocklist

def hasblock(hash_value):
    global BlockStore
    return hash_value in BlockStore.keys()

# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    if isCrashed():
        raise isCrashedError

    global NotMajorityWorkBlock, server_status

    if not server_status == LEADER:
        raise isNotLeader

    NotMajorityWorkBlock.wait()


    global FileInfoMap

    return FileInfoMap

def getonefileinfomap(filename):
    if isCrashed():
        raise isCrashedError

    if not server_status == LEADER:
        raise isNotLeader

    global FileInfoMap

    return FileInfoMap[filename]

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    if isCrashed():
        raise isCrashedError

    global NotMajorityWorkBlock,server_status

    if not server_status == LEADER:
        raise isNotLeader


    NotMajorityWorkBlock.wait()


    global FileInfoMap_lock, FileInfoMap

    if filename in FileInfoMap.keys() and not FileInfoMap[filename][0]+1==version:
        return False

    global EventDict, log, currentTerm,log_Lock
    log_Lock.acquire()
    log.append( (  (filename, version, hashlist), currentTerm  ) )
    log_idx = len(log)-1
    log_Lock.release()

    file_event = threading.Event()
    EventDict[log_idx] = [file_event,True]

    global LogRepTimer,len_other_servers
    LogRepTimer = []
    for i in range(len_other_servers):
        logtimer = threading.Timer(0, ApplyAppendEntries, args=[i, log_idx])
        logtimer.setDaemon(True)
        logtimer.start()
        LogRepTimer.append(logtimer)

    file_event.wait()

    if not EventDict[log_idx]:
        del EventDict[log_idx]
        return False

    del EventDict[log_idx]

    FileInfoMap_lock.acquire()
    FileInfoMap[filename] = [version, hashlist]
    FileInfoMap_lock.release()
    return True
    
    

# PROJECT 3 APIs below
def ApplyAppendEntries(server_idx, log_idx):
    global serverlist, log
    global currentTerm, servernum
    global commitIndex, nextIndex, matchIndex
    ret = False
    global LogRepTimer
    try:
        if len(log) - 1>=nextIndex[server_idx]:
            server = xmlrpc.client.ServerProxy(serverlist[server_idx])
            ret = server.surfstore.appendEntries(servernum, currentTerm, log, log_idx, log[log_idx][1], commitIndex)
            if ret:
                nextIndex[server_idx] = len(log)
                matchIndex[server_idx] = len(log) - 1
            else:
                logtimer = threading.Timer(0, ApplyAppendEntries, args=[server_idx,log_idx-1])
                logtimer.setDaemon(True)
                logtimer.start()
                LogRepTimer[server_idx] = logtimer

    except:
        logtimer = threading.Timer(0.5, ApplyAppendEntries, args=[server_idx, log_idx])
        logtimer.setDaemon(True)
        logtimer.start()
        LogRepTimer[server_idx] = logtimer

    if ret:
        for N in range(commitIndex+1, len(log)):
            if sum([1 if x>=N else 0 for x in matchIndex]) >= int((len_other_servers+1)/2)\
                    and log[N][1] == currentTerm:
                commitIndex = N
                EventDict[N][1] = True
                EventDict[N][0].set()





class isCrashedError(Exception):
    pass

class isNotLeader(Exception):
    pass

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    global server_status
    return server_status == LEADER

def noLongerLeader():
    global HeaetBeatTimer
    HeaetBeatTimer.cancel()

    global LogRepTimer
    for x in LogRepTimer:
        x.cancel()

    global EventDict
    for key in EventDict.keys():
        EventDict[key][1] = False
        EventDict[key][0].set()


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global server_status,HeaetBeatTimer,State_lock
    if server_status == LEADER:
        noLongerLeader()

    State_lock.acquire()
    server_status = FOLLOWER
    State_lock.release()

    global timer
    timer.cancel()


    global CrashedStatus
    CrashedStatus = True


    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    global CrashedStatus
    if CrashedStatus:
        global timer

        timer = threading.Timer(timeout_time / 1000, TimeLoop)
        timer.setDaemon(True)
        timer.start()

    CrashedStatus = False
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    global CrashedStatus
    return CrashedStatus

# Requests vote from this server to become the leader
def requestVote(serverid, term, lastLogIndex, lastLogTerm):
    """Requests vote to be the leader"""
    if isCrashed():
        raise isCrashedError

    global server_status
    global currentTerm,votedFor
    #print("request",currentTerm,term,"status",server_status,serverid)
    if term<currentTerm:
        return False

    if term in votedFor.keys() and not votedFor[term] == serverid:
        return False

    global log
    if lastLogTerm < log[-1][1]:
        currentTerm = term
        return False

    if lastLogTerm == log[-1][1] and lastLogIndex<len(log)-1:
        currentTerm = term
        return False


    global timer, timeout_time
    timer.cancel()



    State_lock.acquire()
    if server_status == LEADER:
        noLongerLeader()

    if term>currentTerm:
        server_status = FOLLOWER


    currentTerm = term
    votedFor[term] = serverid
    State_lock.release()


    timer = threading.Timer(timeout_time / 1000, TimeLoop)
    timer.setDaemon(True)
    timer.start()
    return True

# Updates fileinfomap
def appendEntries(serverid, term, entries, prevLogIndex, prevLogTerm,leaderCommit):
    """Updates fileinfomap to match that of the leader"""

    if isCrashed():
        raise isCrashedError

    global currentTerm, server_status
    #print("Append", currentTerm, term, serverid,"server" ,server_status)
    if term < currentTerm:
        return False

    if server_status == LEADER:
        noLongerLeader()

    if entries == "": ## HeartBeat

        global timer, timeout_time,leaderId
        timer.cancel()

        State_lock.acquire()
        currentTerm = term
        leaderId = serverid
        server_status = FOLLOWER
        State_lock.release()


        timer = threading.Timer(timeout_time / 1000, TimeLoop)
        timer.setDaemon(True)
        timer.start()

    else:
        global log
        if len(log)<=prevLogIndex or not log[prevLogIndex][1] == prevLogTerm:
            log = log[:prevLogIndex]
            return False
        for e in entries[prevLogIndex+1:]:
            log.append(e)
            global FileInfoMap
            (filename, version, hashlist) = e[0]
            FileInfoMap_lock.acquire()
            FileInfoMap[filename] = [version, hashlist]
            FileInfoMap_lock.release()


        global commitIndex
        if leaderCommit > commitIndex:
            commitIndex = min(leaderCommit, len(log)-1)

    #print(log)
    return True

def tester_getversion(filename):
    global FileInfoMap
    return FileInfoMap[filename][0]

# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)


    return maxnum, host, port




def ApplyRequestVote(i, servernum, currentTerm, RequestVoteResults):
    global serverlist,log
    try:
        server = xmlrpc.client.ServerProxy(serverlist[i])
        ret = server.surfstore.requestVote(servernum, currentTerm, len(log)-1, log[-1][1])
        RequestVoteResults[i] = 1 if ret else 0
    #except ConnectionRefusedError or xmlrpc.client.Fault:
    except:
        pass
    return


def ApplyHeartBeat(i,AliveNodes):
    import time
    global currentTerm, servernum
    global serverlist
    try:
        server = xmlrpc.client.ServerProxy(serverlist[i])
        ret = server.surfstore.appendEntries(servernum, currentTerm, "", 0, 0, 0)
        AliveNodes[i] = 1 if ret else 0
    #except ConnectionRefusedError or xmlrpc.client.Fault:
    except:
        pass

def HeartBeat():
    import time
    #(time.time(),"heartbeat")
    global HeaetBeatTimer
    #print("ApplyHeartBeat", "from", servernum, "term", currentTerm)
    HeaetBeatTimer = threading.Timer(HEATBEATTIME, HeartBeat)
    HeaetBeatTimer.setDaemon(True)
    HeaetBeatTimer.start()

    AliveNodes = [0] * len_other_servers
    thread_list = []
    for i in range(len(serverlist)):
        t = threading.Thread(target=ApplyHeartBeat, args=[i,AliveNodes])
        thread_list.append(t)
        t.setDaemon(True)
        t.start()
    #print("ApplyHeartBeat", "from", servernum, "term", currentTerm,"finish")
    for i in range(len(serverlist)):
        thread_list[i].join()


    #print(AliveNodes)
    global WorkingNode_lock
    global workingnodesnum
    workingnodesnum = sum(AliveNodes)
    if workingnodesnum>=int((len_other_servers+1)/2):
        NotMajorityWorkBlock.set()
    else:
        NotMajorityWorkBlock.clear()



def TimeLoop():
    import time
    #print(time.time(), "timeloop")
    global log
    global server_status, currentTerm, votedFor, State_lock
    #print(server_status, "status", currentTerm, "Term", log)
    global timeout_time,timer

    timer = threading.Timer(timeout_time / 1000, TimeLoop)
    timer.setDaemon(True)
    timer.start()


    if server_status == LEADER:
        return

    #print(server_status, "status")
    State_lock.acquire()
    server_status = CANDIDATE
    currentTerm += 1


    if not currentTerm in votedFor.keys():
        votedFor[currentTerm] = servernum
    elif not  votedFor[currentTerm] == servernum:
        return False
    State_lock.release()


    thread_list = []

    global len_other_servers
    RequestVoteResults = [0]*len_other_servers
    for i in range(len_other_servers):
        t = threading.Thread(target=ApplyRequestVote, args= (i, servernum, currentTerm, RequestVoteResults))
        t.start()
        thread_list.append(t)
    for i in range(len_other_servers):
        thread_list[i].join()


    votes = sum(RequestVoteResults)+ 1 if currentTerm in votedFor.keys() and\
                                          votedFor[currentTerm] == servernum else 0
    #print(RequestVoteResults, votes)
    MajorityNum = int((len_other_servers+1)/2)

    global HeaetBeatTimer

    if votes>MajorityNum:
        server_status = LEADER
        HeaetBeatTimer = threading.Timer(0, HeartBeat)
        HeaetBeatTimer.setDaemon(True)
        HeaetBeatTimer.start()
        global nextIndex, matchIndex
        nextIndex = [len(log) for _ in range(len_other_servers)]
        matchIndex = [0 for _ in range(len_other_servers)]











if __name__ == "__main__":
    global FileInfoMap, FileInfoMap_lock, BlockStore_lock, State_lock,BlockStore, WorkingNode_lock
    global log_Lock
    FileInfoMap = {}


    FileInfoMap_lock = threading.Lock()
    BlockStore_lock = threading.Lock()
    State_lock = threading.Lock()
    WorkingNode_lock = threading.Lock()
    log_Lock = threading.Lock()
    BlockStore = {}

    global timeout_time,server_status,currentTerm,votedFor, leaderId


    timeout_time = random.randint(400,800)
    print(timeout_time,HEATBEATTIME)


    server_status = FOLLOWER
    currentTerm = 0
    votedFor = {}

    global CrashedStatus
    CrashedStatus = False

    leaderId = -1

    global NotMajorityWorkBlock
    NotMajorityWorkBlock = threading.Event()

    global log, lastApplied, commitIndex
    log = [(0,0)]
    lastApplied = 0
    commitIndex = 0

    global EventDict
    EventDict = {}

    global LogRepTimer
    LogRepTimer = []


    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        global servernum, len_other_servers,  rpc_server_list

        config = args.config
        servernum = args.servernum

        # server list has list of other servers
        serverlist = []


        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum)

        len_other_servers = len(serverlist)
        serverlist = [ "http://" +x if not x.startswith("http://") else x for x in serverlist]

        global workingnodesnum
        workingnodesnum = 0








        print("Attempting to start XML-RPC Server...")
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        server.register_function(updatefile, "surfstore.getonefileinfomap")
        server.register_function(hasblock, "surfstore.hasblock")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        global timer
        timer = threading.Timer(timeout_time/1000, TimeLoop)
        timer.setDaemon(True)
        timer.start()

        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
