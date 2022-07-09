package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"mit6824/src/labgob"
	"mit6824/src/labrpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// raft节点状态
type RaftState uint8

const (
	FOLLOWER RaftState = iota
	CANDIDATE
	LEADER
)

// raft Leader心跳间隔
const BEAT_HEART_INTERVAL = 100

const LEASE_INTERVAL = 5 * BEAT_HEART_INTERVAL

const LEADER_DOWN_INTERVAL = 4 * BEAT_HEART_INTERVAL

const TRY_COUNT = 5

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	votedFor  int                 // CandidateId that received vote in current term (or null if none)
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	logIndex int

	state RaftState
	term  int
	// 单位是秒
	lastLeaderAct int64

	commitIndex int // 日志提交logIndex
	logEntrys   []*LogEntry
	applyCh     chan ApplyMsg

	// The leader maintains a nextIndex for each follower,
	// which is the index of the next log entry the leader will send to that follower.
	nextAppendIndexOfPeer []int
	lastRespTimePeers     []int64
	// 租约，为解决假主问题
	leaseLastUpdateTm     int64
	appendEventChan       chan AppendEvent
	rfCompleteCommitIndex int
	checkLeaseCount       int

	commitEventChan   chan CommitEvent
	commitIndexOfPeer []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	//DPrintf("{%d}GetState:%v--%v", rf.me, rf.term, rf.state)
	return rf.term, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	// raft state
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(rf.term)
	encoder.Encode(rf.logEntrys)
	rf.persister.SaveRaftState(buf.Bytes())
	// DPrintf("持久化：len=%d", len(rf.logEntrys))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var term int
	var logEntries = make([]*LogEntry, 0)
	if err := decoder.Decode(&term); err != nil {
		// DPrintf("read Persist error, %v", err.Error())
		return
	}
	if err := decoder.Decode(&logEntries); err != nil {
		// DPrintf("read Persist error, %v", err.Error())
		return
	}

	rf.term = term
	rf.logEntrys = logEntries
	// DPrintf("{%d}读取快照！！", rf.me)
}

type LogEntry struct {
	Command  interface{} `command`
	LogIndex int         `logIndex`
	Term     int         `term`
}

type AppendEntriesArgs struct {
	Term         int         `json:"term"`
	LeaderId     string      `json:"leader_id"`
	PrevLogIndex int         `json:"prev_log_index"`
	PrevLogTerm  int         `json:"prev_log_term"`
	Entries      []*LogEntry `json:"entries"`
	RequestType  requestType `json:"append_entry"`
	CommitIndex  int         `json:"commit_index"`
	AppendIndex  int         `json:"append_index"`
	Peer         int         `json:"peer"`
}

type AppendEntriesReply struct {
	Term        int             `json:"term"`
	LogIndex    int             `json:"log_index"`
	CommitIndex int             `json:"commit_index"`
	Desc        string          `json:"Desc"`
	ErrorCode   AppendErrorCode `json:"error_code"`
}

type AppendErrorCode uint8

const (
	SUCCESS = 3
	FAIL    = 1
	EXPIRE  = 2
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int    `json:"term"`
	CandidateId  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int    `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	Desc        string `json:"Desc"`
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		// DPrintf("选举{%d，%d, %d}投票：%v-%v", rf.me, rf.term, rf.state, args.Term, reply)
		if rf.term < args.Term {
			rf.term = args.Term
		}
		if rf.lastLeaderAct+LEADER_DOWN_INTERVAL > time.Now().UnixNano() {
			reply.Desc = "我已经接到leader的通知了"
			reply.VoteGranted = false
		}
		if reply.VoteGranted {
			rf.lastLeaderAct = time.Now().UnixMilli()
		}
		rf.mu.Unlock()
	}()
	if rf.state == LEADER {
		reply.Desc = "我就是Leader"
		reply.VoteGranted = false
		if rf.term < args.Term {
			rf.syncMaxTerm(args.Term)
		}
		return
	}
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.logEntrys) > 0 {
		lastLogIndex = rf.logEntrys[len(rf.logEntrys)-1].LogIndex
		lastLogTerm = rf.logEntrys[len(rf.logEntrys)-1].Term
	}
	// DPrintf("{%d}args=%v, rf.term=%d,lastLogIndex=%d,lastLogTerm=%d", rf.me, args, rf.term, lastLogIndex, lastLogTerm)
	switch rf.state {
	case FOLLOWER:
		if args.Term <= rf.term || args.LastLogTerm < lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			reply.Desc = fmt.Sprintf("我是FOLLOWER,%d,%d,%d,%d,%d,%d,%d,%d", args.Term, rf.term, args.LastLogTerm, lastLogTerm,
				args.LastLogTerm, lastLogTerm, args.LastLogIndex, lastLogIndex)
			reply.VoteGranted = false
			reply.Term = rf.term
			return
		} else {
			reply.VoteGranted = true
			reply.Term = args.Term
		}
	case CANDIDATE:
		if args.Term <= rf.term || args.LastLogTerm < lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			reply.Term = rf.term
			reply.Desc = "我是CANDIDATE"
			reply.VoteGranted = false
		} else {
			rf.setStateWithConditionUnLock(CANDIDATE, FOLLOWER)
			reply.Term = args.Term
			reply.VoteGranted = true
		}
	case LEADER:
		if args.Term <= rf.term || args.LastLogTerm < lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			reply.Desc = "我是LEADER"
			reply.VoteGranted = false
			reply.Term = rf.term
		} else {
			rf.setStateWithConditionUnLock(LEADER, FOLLOWER)
			reply.Term = args.Term
			reply.VoteGranted = true
		}
	}
}

type requestType int

const (
	AppendEntries requestType = 1
	CommitEntries requestType = 2
	NoticeLeader  requestType = 3
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("START AppendEntries")
	rf.mu.Lock()
	startTs := time.Now().UnixMilli()
	defer func() {
		rf.mu.Unlock()
		// DPrintf("{%d}append log over,%d,%d,%d,%d", rf.me, reply.CommitIndex, reply.LogIndex, reply.Term, len(rf.logEntrys))
		if reply.ErrorCode == SUCCESS {
			rf.lastLeaderAct = time.Now().UnixNano() / 1e6
		}
		DPrintf("%d-%d END AppendEntries cost %d ms,reply=%s", args.Peer, rf.me, time.Now().UnixMilli()-startTs, getJson(reply))
	}()
	// Reply false if term < currentTerm
	if args.Term < rf.term {
		reply.Term = rf.term
		// DPrintf("follower's term is %d, leader's term is %d", rf.term, args.Term)
		//return
	} else {
		rf.term = args.Term
	}
	if len(rf.logEntrys) > 0 && args.Term < rf.logEntrys[len(rf.logEntrys)-1].Term {
		reply.ErrorCode = EXPIRE
		return
	}
	reply.ErrorCode = SUCCESS
	reply.Term = args.Term

	if args.RequestType == AppendEntries {
		// 一致性检查
		if args.PrevLogIndex > 0 {
			if len(rf.logEntrys) == 0 {
				reply.Term = args.Term
				reply.LogIndex = 0
				return
			}
			// Reply false if log doesn’t contain an entry at prevLogIndex
			// whose term matches prevLogTerm
			logOffset, exists := rf.findLogEntryByIndex(args.PrevLogIndex)
			if !exists || rf.logEntrys[logOffset].Term != args.PrevLogTerm {
				reply.Term = args.Term
				offset := rf.findLastLogEntryOfLastTerm()
				if offset >= 0 {
					reply.LogIndex = rf.logEntrys[offset].LogIndex
				}
				return
			}

			rf.logEntrys = append(rf.logEntrys[0:logOffset+1], args.Entries...)
			rf.logIndex = rf.logEntrys[len(rf.logEntrys)-1].LogIndex
			//DPrintf("{%d}logEntrys size:%d", rf.me, len(rf.logEntrys))
			reply.LogIndex = rf.logIndex
			// 持久化
			rf.persist()
			// DPrintf("{%d}append log Success, logIndex=%d", rf.me, rf.logIndex)
			return
		} else {
			rf.logEntrys = append(rf.logEntrys[0:0], args.Entries...)
			rf.logIndex = rf.logEntrys[len(rf.logEntrys)-1].LogIndex
			reply.LogIndex = rf.logIndex
			// 持久化
			rf.persist()
			// DPrintf("{%d}append log success2, logIndex=%d", rf.me, rf.logIndex)
			return
		}
	} else if args.RequestType == CommitEntries {
		if args.CommitIndex <= rf.commitIndex {
			return
		}
		DPrintf("{%d}commit STRAT,args=%s", rf.me, getJson(args))
		logOffset, exists := rf.findLogEntryByIndex(rf.commitIndex + 1)
		if exists {
			for logOffset < len(rf.logEntrys) && rf.logEntrys[logOffset].LogIndex <= args.CommitIndex {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntrys[logOffset].Command,
					CommandIndex: rf.logEntrys[logOffset].LogIndex,
				}
				DPrintf("{%d} follower apply log %d,%v", rf.me, rf.logEntrys[logOffset].LogIndex, rf.logEntrys[logOffset].Command)
				rf.commitIndex = rf.logEntrys[logOffset].LogIndex
				reply.CommitIndex = rf.commitIndex
				DPrintf("{%d}apply log %d,leaderCommitIndex=%d,%v", rf.me, rf.commitIndex, args.CommitIndex, rf.logEntrys[logOffset].Command)
				logOffset++
				// fmt.Printf("{%d}apply log %d,cmd=%v,term=%d\n", rf.me, rf.commitIndex, rf.logEntrys[logOffset-1].Command, rf.logEntrys[logOffset-1].Term)
			}
		}
	} else if args.RequestType == NoticeLeader {
		return
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	tryCount := 0
	success := false
	for tryCount < TRY_COUNT {
		success = func() bool {
			done := make(chan struct{}, 1)
			var ok bool
			go func() {
				ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
				done <- struct{}{}
			}()
			select {
			case <-done:
				return ok
			case <-time.After(time.Millisecond * 10):
				return ok
			}
		}()
		if success {
			break
		}
		tryCount++
	}
	return success
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index, term int, isLeader bool) {
	defer func() {
		DPrintf("{%d}START log, cmd=%s,result=%d,%d,%t", rf.me, command, index, term, isLeader)
	}()
	index = rf.commitIndex
	term = rf.term
	isLeader = rf.isLeader()

	// Your code here (2B).
	// 如果不是Leader则直接返回
	if !isLeader {
		isLeader = false
		return index, term, isLeader
	}
	// DPrintf("{%d}START-%v", rf.me, command)

	// 发送
	logEntry := &LogEntry{}
	logEntry.Command = command
	logEntry.Term = rf.term
	// 追加到本地日志
	rf.appendLocalLogEntry(logEntry)
	// 持久化
	rf.persist()
	// 提交至follower
	// ok, term := rf.appendLogEntry()
	appendEvent := AppendEvent{
		appendResultChan: make(chan *AppendResult, 1),
	}
	rf.appendEventChan <- appendEvent
	appendResult := <-appendEvent.appendResultChan
	DPrintf("{%d}appendResult=%s,cmd=%s", rf.me, getJson(appendResult), command)
	if appendResult.Success && appendResult.MaxTerm > rf.term {
		rf.setStateWithCondition(LEADER, FOLLOWER, 3)
		return 0, 0, false
	}
	if appendResult.Success {
		DPrintf("%d append Over!!!,%v,%d,%t,%d,%d,Desc=%s,isLeader=%t", rf.me, command, logEntry.LogIndex, appendResult.Success, appendResult.MaxTerm, rf.term, appendResult.Desc, rf.isLeader())

		commitEvent := CommitEvent{
			commitResultChan: make(chan *CommitResult, 1),
			logIndex:         logEntry.LogIndex,
			peer:             rf.me,
		}
		rf.commitEventChan <- commitEvent
		<-commitEvent.commitResultChan
	}
	DPrintf("{%d}==========%d,%v,%t", rf.me, logEntry.LogIndex, command, isLeader)
	return logEntry.LogIndex, rf.term, isLeader
}

func (rf *Raft) consumeAppendEvent() {
	for {
		if appendEvent, isOpen := <-rf.appendEventChan; isOpen {
			// 追加
			ok, term, desc := rf.appendLogEntry()
			DPrintf("{%d}consumeAppendEvent ok=%t,term=%d,Desc=%s", rf.me, ok, term, desc)
			appendEvent.appendResultChan <- &AppendResult{
				Success: ok,
				MaxTerm: term,
				Desc:    desc,
			}
		}
	}
}

func (rf *Raft) consumeCommitEvent() {
	for {
		if commitEvent, isOpen := <-rf.commitEventChan; isOpen {
			isCommit := (commitEvent.peer == rf.me && commitEvent.logIndex > rf.commitIndex) || commitEvent.logIndex > rf.commitIndexOfPeer[commitEvent.peer]
			// 提交
			if isCommit {
				DPrintf("{%d}commit START,commitEvent.logIndex=%d, peer=%d", rf.me, commitEvent.logIndex, commitEvent.peer)
				commitSuccess := rf.commitLogEntries(commitEvent.logIndex, commitEvent.peer)
				DPrintf("commitSuccess=%t,commitEvent.logIndex=%d", commitSuccess, commitEvent.logIndex)
				offset, exists := rf.findLogEntryByIndex(rf.commitIndex + 1)
				if commitEvent.peer == rf.me && commitSuccess && exists {
					for i := offset; i < len(rf.logEntrys) && rf.logEntrys[i].LogIndex <= commitEvent.logIndex; i++ {
						commitLog := rf.logEntrys[i]
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      commitLog.Command,
							CommandIndex: commitLog.LogIndex,
						}
						DPrintf("{%d} leader apply log %d,%v", rf.me, commitLog.LogIndex, commitLog.Command)
						rf.commitIndex = commitLog.LogIndex
						// fmt.Printf("{%d}Leader apply {%d},cmd={%v},%d\n", rf.me, commitLog.LogIndex, commitLog.Command, commitLog.Term)
					}
				}
				commitEvent.commitResultChan <- &CommitResult{
					success: true,
				}
			}
		}
	}
}

func (rf *Raft) checkLease() {
	if rf.isLeader() {
		now := time.Now().UnixMilli()
		threshold := now - LEASE_INTERVAL
		aliveCount := 1
		for idx, _ := range rf.lastRespTimePeers {
			lastRespTime := rf.lastRespTimePeers[idx]
			if lastRespTime >= threshold {
				aliveCount++
			}
		}
		DPrintf("{%d}checkLease，aliveCount=%d,lastRespTimePeers=%v", rf.me, aliveCount, rf.lastRespTimePeers)
		if aliveCount <= len(rf.peers)/2 {
			rf.setStateWithCondition(LEADER, FOLLOWER, 1)
		}
	}
}

func (rf *Raft) checkLeaderAlive() {
	for {
		select {
		case <-time.NewTicker(BEAT_HEART_INTERVAL * time.Millisecond).C:
			isConntinue := true
			for isConntinue && rf.state != LEADER && time.Now().UnixMilli() > rf.lastLeaderAct+LEADER_DOWN_INTERVAL {
				// DPrintf("{%d}Leader凉凉", rf.me)
				// leader 已死
				time.Sleep(time.Duration(650+rand.Intn(300)) * time.Millisecond)
				isConntinue = func() bool {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if time.Now().UnixMilli() < rf.lastLeaderAct+LEADER_DOWN_INTERVAL {
						return false
					}
					rf.setStateWithConditionUnLock(FOLLOWER, CANDIDATE)
					if !rf.addTerm() {
						return false
					}

					DPrintf("{%d}开始选举,%d", rf.me, rf.term)
					var ok bool
					var maxTerm int
					ok, maxTerm = rf.requireVotes()
					DPrintf("选举{%d}选票结果，curTerm=%d,ok=%v,MaxTerm=%v", rf.me, rf.term, ok, maxTerm)
					if ok {
						// 通知其他节点选主成功
						ok, maxTerm = rf.noticeLeaderElectionSuccess()
						DPrintf("{%d}通知其他节点选主完成%v,%v", rf.me, ok, maxTerm)
						if ok {
							// 超过半数同意
							if rf.setStateWithConditionUnLock(CANDIDATE, LEADER) {
								DPrintf("选举%d成为Leader, %d, %v", rf.me, rf.state, rf.lastRespTimePeers)
								rf.maintainLeaderAuthority()
								// 初始化commitIndexOfPeer
								rf.commitIndexOfPeer = make([]int, len(rf.peers))
								return false
							}
						} else {
							// 为了下次选主做准备，如果不同步则下次投票只能做follower
							rf.syncMaxTerm(maxTerm)
						}
					} else {
						// 为了下次选主做准备，如果不同步则下次投票只能做follower
						rf.syncMaxTerm(maxTerm)
					}
					// 如果节点状态为candidate则转为follower
					rf.setStateWithConditionUnLock(CANDIDATE, FOLLOWER)
					return true
				}()
			}
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setStateWithCondition(condition RaftState, state RaftState, pos int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == state {
		return true
	}
	if rf.state == condition {
		DPrintf("{%d}setStateWithCondition，cond=%d,target=%d-%d", rf.me, condition, state, pos)
		rf.state = state
		return true
	}
	return false
}

func (rf *Raft) setStateWithConditionUnLock(condition RaftState, state RaftState) bool {
	if rf.state == state {
		return true
	}
	if rf.state == condition {
		DPrintf("{%d}setStateWithConditionUnLock，cond=%d,target=%d, rf.state=%d", rf.me, condition, state, rf.state)
		rf.state = state
		return true
	}
	return false
}

func (rf *Raft) requireVotes() (bool, int) {
	// 先投自己一票
	voteGrantedCount := 1
	maxTerm := rf.term
	lock := sync.Mutex{}
	done := make(chan struct{}, 10)
	remain := len(rf.peers) - 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			defer func() {
				done <- struct{}{}
			}()
			args := &RequestVoteArgs{
				Term:        rf.term,
				CandidateId: fmt.Sprintf("%d", rf.me),
			}
			if len(rf.logEntrys) > 0 {
				lastLogEntry := rf.logEntrys[len(rf.logEntrys)-1]
				args.LastLogIndex = lastLogEntry.LogIndex
				args.LastLogTerm = lastLogEntry.Term
			}
			reply := &RequestVoteReply{}
			startTs := time.Now().UnixMilli()
			ok := rf.sendRequestVote(peer, args, reply)
			DPrintf("{%d}-{%d}请求投票 cost %d ms，args=%v, reply=%v,", rf.me, peer, time.Now().UnixMilli()-startTs, getJson(args), getJson(reply))
			if ok && reply.VoteGranted {
				func() {
					lock.Lock()
					defer lock.Unlock()
					if reply.Term > maxTerm {
						maxTerm = reply.Term
					}
					voteGrantedCount++
				}()
			} else if ok {
				func() {
					lock.Lock()
					defer lock.Unlock()
					if reply.Term > maxTerm {
						maxTerm = reply.Term
					}
				}()
			}
		}(i)
	}
	for remain > 0 {
		select {
		case <-done:
			remain--
		case <-time.NewTicker(100 * time.Millisecond).C:
			remain = 0
		}
	}
	DPrintf("选举{%d}获得选票数：%d", rf.me, voteGrantedCount)
	if rf.term >= maxTerm && voteGrantedCount > len(rf.peers)/2 {
		return true, rf.term
	}
	return false, maxTerm
}

func (rf *Raft) addTerm() bool {
	if time.Now().UnixNano()/1e6 <= rf.lastLeaderAct+2*BEAT_HEART_INTERVAL || rf.state != CANDIDATE {
		//DPrintf("{%d}状态：%d", rf.me, rf.state)
		return false
	}
	rf.term = rf.term + 1
	return true
}

func (rf *Raft) noticeLeaderElectionSuccess() (bool, int) {
	maxTerm := rf.term
	lock := sync.Mutex{}
	remain := len(rf.peers) - 1
	done := make(chan struct{}, 10)
	replayCount := 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			defer func() {
				done <- struct{}{}
			}()
			args := &AppendEntriesArgs{
				RequestType: NoticeLeader,
				Term:        rf.term,
				LeaderId:    fmt.Sprintf("%d", rf.me),
				Peer:        rf.me,
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, args, reply)
			DPrintf("noticeLeaderElectionSuccess{%d}-%d-ok=%t,args=%s,reply=%s", rf.me, peer, ok, getJson(args), getJson(reply))
			if ok && reply.Term > rf.term {
				func() {
					lock.Lock()
					defer lock.Unlock()
					if reply.Term > maxTerm {
						maxTerm = reply.Term
					}
				}()
			} else {
				if ok && reply.ErrorCode == SUCCESS {
					func() {
						lock.Lock()
						defer lock.Unlock()
						replayCount++
						rf.lastRespTimePeers[peer] = time.Now().UnixMilli()
						rf.syncMaxTerm(reply.Term)
					}()
				} else if ok {

				}
			}
		}(i)
	}
	for remain > 0 {
		select {
		case <-done:
			remain--
		}
	}
	if replayCount > len(rf.peers)/2 && rf.term >= maxTerm {
		return true, rf.term
	}
	return false, maxTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	tryCount := 0
	success := false
	for tryCount < TRY_COUNT {
		success = func() bool {
			done := make(chan struct{}, 1)
			var ok bool
			go func() {
				ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
				done <- struct{}{}
			}()
			select {
			case <-done:
				return ok
			case <-time.After(time.Millisecond * 20):
				return ok
			}
		}()
		if success {
			break
		}
		tryCount++
	}
	return success
}

func (rf *Raft) syncMaxTermWithLock(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term < term {
		rf.term = term
		return true
	}
	return false
}

func (rf *Raft) syncMaxTerm(term int) bool {
	if rf.term < term {
		rf.term = term
		return true
	}
	return false
}

func (rf *Raft) commitLogEntries(logIndex int, commitPeer int) (success bool) {
	defer func() {
		DPrintf("{%d}commitLogEntries logIndex=%d,Success=%t", rf.me, logIndex, success)
	}()
	lock := sync.Mutex{}
	commitedCount := 1
	if logIndex > rf.logIndex {
		return false
	}
	wg := sync.WaitGroup{}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer func() {
				wg.Done()
			}()
			if rf.isLeader() && rf.nextAppendIndexOfPeer[peer] > logIndex {
				lock.Lock()
				defer lock.Unlock()
				now := time.Now()
				if rf.commitIndexOfPeer[peer] >= logIndex {
					rf.lastRespTimePeers[peer] = now.UnixMilli()
					commitedCount++
					return
				}
				args := &AppendEntriesArgs{
					CommitIndex: logIndex,
					RequestType: CommitEntries,
					Term:        rf.term,
					LeaderId:    fmt.Sprintf("%d", rf.me),
					Peer:        rf.me,
				}
				reply := &AppendEntriesReply{}
				DPrintf("{%d}-{%d}commitAppendEntries START args=%s", rf.me, peer, getJson(args))
				ok := rf.sendAppendEntries(peer, args, reply)
				DPrintf("{%d}-{%d}commitAppendEntries ok = %t, args=%s,reply=%s", rf.me, peer, ok, getJson(args), getJson(reply))
				if ok && reply.ErrorCode == EXPIRE {
					rf.setStateWithCondition(LEADER, FOLLOWER, 2)
				} else if ok && reply.ErrorCode == SUCCESS {
					rf.lastRespTimePeers[peer] = now.UnixMilli()
					rf.syncMaxTermWithLock(reply.Term)
					rf.commitIndexOfPeer[peer] = logIndex
					commitedCount++
				}
			}
		}(i)
	}
	overCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(overCh)
	}()
	startTs := time.Now().UnixMilli()
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*BEAT_HEART_INTERVAL)
	select {
	case <-ctx.Done():
	case <-overCh:
	}
	DPrintf("cost %d ms", (time.Now().UnixMilli() - startTs))
	if commitedCount > len(rf.peers)/2 {
		success = true
		return
	}
	success = false
	return
}

func getJson(o interface{}) string {
	bs, err := json.Marshal(o)
	if err != nil {
		return ""
	}
	return string(bs)
}

func (rf *Raft) maintainLeaderAuthority() {
	// 发送心跳
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			for rf.isLeader() {
				now := time.Now()
				args := &AppendEntriesArgs{
					CommitIndex: rf.commitIndex,
					RequestType: NoticeLeader,
					Term:        rf.term,
					LeaderId:    fmt.Sprintf("%d", rf.me),
					AppendIndex: rf.nextAppendIndexOfPeer[peer] - 1,
					Peer:        rf.me,
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, args, reply)
				if ok && reply.ErrorCode == EXPIRE {
					rf.setStateWithCondition(LEADER, FOLLOWER, 2)
					break
				} else if ok && reply.ErrorCode == SUCCESS {
					rf.lastRespTimePeers[peer] = now.UnixMilli()
					rf.syncMaxTermWithLock(reply.Term)

					if len(rf.logEntrys) > 0 {
						// append log
						commitLogIndex, foundLog := rf.findLogEntryByIndex(rf.commitIndex)
						DPrintf("{%d}是否需要重推{%d}，ok=%t,cidx1=%d,cidx2=%d,rf.nextAppendIndexOfPeer[peer]=%d", rf.me, peer, ok, rf.nextAppendIndexOfPeer[peer], rf.commitIndex, rf.nextAppendIndexOfPeer[peer])
						if foundLog && rf.logEntrys[commitLogIndex].Term == rf.term && rf.nextAppendIndexOfPeer[peer] <= rf.commitIndex {
							// 需要重推
							DPrintf("{%d}需要重推{%d}，ok=%t,cidx1=%d,cidx2=%d", rf.me, peer, ok, rf.nextAppendIndexOfPeer[peer], rf.commitIndex)
							appendEvent := AppendEvent{
								appendResultChan: make(chan *AppendResult, 1),
							}
							rf.appendEventChan <- appendEvent
						}
						// commit log
						commitLogindex := rf.commitIndexOfPeer[peer]
						maxAppendOverLogIndex := rf.getMaxAppendOverLogIndex()
						offset, found := rf.findLogEntryByIndex(maxAppendOverLogIndex)
						var maxAppendOverLogEntry *LogEntry
						if found {
							maxAppendOverLogEntry = rf.logEntrys[offset]
						}
						DPrintf("{%d}recommit log,nextAppendLogindex=%d,maxAppendOverLogEntry=%s", peer, commitLogindex, getJson(maxAppendOverLogEntry))
						if maxAppendOverLogEntry != nil && maxAppendOverLogEntry.Term == rf.term {
							DPrintf("{%d}recommit, commitLogIndex=%d", peer, commitLogIndex)
							if maxAppendOverLogEntry.LogIndex > rf.commitIndex {
								commitEvent := CommitEvent{
									logIndex:         maxAppendOverLogEntry.LogIndex,
									commitResultChan: make(chan *CommitResult, 1),
									peer:             rf.me,
								}
								rf.commitEventChan <- commitEvent
							} else {
								if commitLogindex < rf.commitIndex {
									commitEvent := CommitEvent{
										logIndex:         rf.commitIndex,
										commitResultChan: make(chan *CommitResult, 1),
										peer:             peer,
									}
									rf.commitEventChan <- commitEvent
								}
							}
						}
					}
				}
				time.Sleep(BEAT_HEART_INTERVAL * time.Millisecond)
				rf.checkLease()
			}
		}(i)
	}
}

func (rf *Raft) findLogEntryByIndex(index int) (int, bool) {
	for i := len(rf.logEntrys) - 1; i >= 0; i-- {
		if rf.logEntrys[i] == nil {
			DPrintf("HJH")
		}
		if rf.logEntrys[i].LogIndex == index {
			return i, true
		}
	}
	return 0, false
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) appendLogEntry() (bool, int, string) {
	// DPrintf("{%d}appendLogEntry, logIndex=%d", rf.me, rf.logIndex)
	lock := sync.Mutex{}
	maxTerm := rf.term
	remain := len(rf.peers) - 1
	done := make(chan struct{}, 10)
	replayCount := 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			defer func() {
				done <- struct{}{}
			}()
			commitSize := 200
			for {
				nextAppendIndex := rf.nextAppendIndexOfPeer[peer]
				if nextAppendIndex == 0 {
					nextAppendIndex = 1
				}
				if nextAppendIndex > rf.logIndex {
					func() {
						lock.Lock()
						defer lock.Unlock()
						replayCount++
					}()
					break
				}
				prevLogIndex := 0
				prevLogTerm := 0
				if nextAppendIndex > 1 {
					prevLogEntry, ok := rf.getLogEntryByCommitIndex(nextAppendIndex - 1)
					if ok {
						prevLogIndex = prevLogEntry.LogIndex
						prevLogTerm = prevLogEntry.Term
					}
				}
				prepareLogEntries := rf.getLogEntriesByCommitIndex(nextAppendIndex, commitSize)
				args := &AppendEntriesArgs{
					RequestType:  AppendEntries,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Term:         rf.term,
					LeaderId:     fmt.Sprintf("%d", rf.me),
					Entries:      prepareLogEntries,
					Peer:         rf.me,
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, args, reply)
				DPrintf("{%d}sendAppendEntries{%d},term=%d,PrevLogIndex=%d,reply=%v,ok=%t", rf.me, peer, rf.term, args.PrevLogIndex, getJson(reply), ok)
				if ok {
					if reply.ErrorCode == EXPIRE {
						// 变为follower重新选举
						func() {
							lock.Lock()
							defer lock.Unlock()
							maxTerm = reply.Term
						}()
						break
					} else {
						if reply.ErrorCode == SUCCESS {
							rf.nextAppendIndexOfPeer[peer] = reply.LogIndex + 1
							rf.syncMaxTermWithLock(reply.Term)
						} else {
							break
						}
					}
				} else {
					break
				}
				if rf.nextAppendIndexOfPeer[peer] > rf.logIndex {
					func() {
						lock.Lock()
						defer lock.Unlock()
						replayCount++
					}()
					break
				}
			}
		}(i)
	}
	for remain > 0 {
		select {
		case <-done:
			remain--
		}
	}
	if replayCount > len(rf.peers)/2 {
		// DPrintf("commit Success!!!replayCount=%d,totalCount=%d，cmd=%v,index=%d", replayCount, len(rf.peers), rf.commitIndex, rf.logIndex)
		return true, rf.term, ""
	}
	return false, maxTerm, fmt.Sprintf("replayCount=%d,len(rf.peers)=%d", replayCount, len(rf.peers))
}

func (rf *Raft) getLogEntryByCommitIndex(index int) (*LogEntry, bool) {
	for i := len(rf.logEntrys) - 1; i >= 0; i-- {
		if rf.logEntrys[i] == nil {
			fmt.Println("HELLo")
		}
		if rf.logEntrys[i].LogIndex == index {
			return rf.logEntrys[i], true
		}
	}
	return nil, false
}

func (rf *Raft) getLogEntriesByCommitIndex(index int, size int) []*LogEntry {
	logEntries := make([]*LogEntry, 0, size)
	for i, _ := range rf.logEntrys {
		if rf.logEntrys[i].LogIndex == index || (len(logEntries) > 0 && len(logEntries) < size) {
			logEntries = append(logEntries, rf.logEntrys[i])
		} else if len(logEntries) > size {
			break
		}
	}
	// DPrintf("{%d}==,totalSize=%d,size=%d,logEntries.len=%d", rf.me, len(rf.logEntrys), size, len(logEntries))
	return logEntries
}

func (rf *Raft) getLogEntriesByCommitIndexRange(startIdx int, endIdx int, size int) []*LogEntry {
	logEntries := make([]*LogEntry, 0, size)
	for i, _ := range rf.logEntrys {
		if rf.logEntrys[i].LogIndex <= endIdx && (rf.logEntrys[i].LogIndex == startIdx || (len(logEntries) > 0 && len(logEntries) < size)) {
			logEntries = append(logEntries, rf.logEntrys[i])
		} else if len(logEntries) > size {
			break
		}
	}
	// DPrintf("==,totalSize=%d,size=%d,logEntries.len=%d", len(rf.logEntrys), size, len(logEntries))
	return logEntries
}

func (rf *Raft) incrementLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logIndex++
	DPrintf("incrementLogIndex index=%d", rf.logIndex)
	return rf.logIndex
}

func (rf *Raft) findLastLogEntryOfLastTerm() int {
	if len(rf.logEntrys) == 0 {
		return -1
	}
	curTerm := rf.logEntrys[len(rf.logEntrys)-1].Term
	for i := len(rf.logEntrys) - 1; i >= 0; i-- {
		if rf.logEntrys[i].Term != curTerm {
			return i
		}
	}
	return 0
}

func (rf *Raft) appendLocalLogEntry(logEntry *LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logIndex++
	logEntry.LogIndex = rf.logIndex
	rf.logEntrys = append(rf.logEntrys, logEntry)
}

func (rf *Raft) appendLocalLogEntryUnLock(logEntry *LogEntry) {
	rf.logIndex++
	logEntry.LogIndex = rf.logIndex
	rf.logEntrys = append(rf.logEntrys, logEntry)
}

func (rf *Raft) getMaxAppendOverLogIndex() int {
	tmps := make([]int, len(rf.nextAppendIndexOfPeer))
	for peer, _ := range rf.nextAppendIndexOfPeer {
		tmps[peer] = rf.nextAppendIndexOfPeer[peer]
	}
	if len(rf.logEntrys) > 0 {
		tmps[rf.me] = rf.logEntrys[len(rf.logEntrys)-1].LogIndex
	}
	sort.Ints(tmps)
	DPrintf("getMaxAppendOverLogIndex tmps=%v", tmps)
	return tmps[len(tmps)/2] - 1
}

func (rf *Raft) buildBlankEntry() *LogEntry {
	return &LogEntry{
		Command: nil,
		Term:    rf.term,
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.logEntrys = make([]*LogEntry, 0)
	rf.nextAppendIndexOfPeer = make([]int, len(rf.peers))
	rf.lastRespTimePeers = make([]int64, len(rf.peers))
	// Your initialization code here (2A, 2B, 2C).
	rf.appendEventChan = make(chan AppendEvent, 10000)
	rf.commitEventChan = make(chan CommitEvent, 10000)
	rf.commitIndexOfPeer = make([]int, len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 初始化logIndex
	if len(rf.logEntrys) > 0 {
		rf.logIndex = rf.logEntrys[len(rf.logEntrys)-1].LogIndex
	}
	go rf.checkLeaderAlive()
	go rf.consumeAppendEvent()
	go rf.consumeCommitEvent()
	DPrintf("Init raft--{%d}", rf.me)
	return rf
}

type AppendEvent struct {
	appendResultChan chan *AppendResult
}

type CommitEvent struct {
	commitResultChan chan *CommitResult
	logIndex         int
	peer             int
}

type AppendResult struct {
	Success bool   `json:"success"`
	MaxTerm int    `json:"max_term"`
	Desc    string `json:"Desc"`
}

type CommitResult struct {
	success bool
	maxTerm int
	desc    string
}
