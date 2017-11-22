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
	"encoding/gob"
	"fmt"
	"labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// log entry
type logEntry struct {
	Command interface{}
	Term    int
}

type raftLogs []logEntry

func indexInLogs(index, snapshotIndex int) int {
	return index - (snapshotIndex + 1)
}

func (rf *Raft) logsLastIndexAndTerm() (int, int) {
	logLen := len(rf.logs)

	if logLen == 0 {
		return rf.snapshotIndex, rf.snapshotTerm
	}

	lastLogIndex := logLen + rf.snapshotIndex
	lastTerm := rf.logsGetTerm(lastLogIndex)
	return lastLogIndex, lastTerm
}

func (rf *Raft) logsGetTerm(index int) int {
	if index < rf.snapshotIndex {
		str := fmt.Sprintf("logsGetTerm: index(%v) should greater than or equal snapshot index(%v)", index, rf.snapshotIndex)
		panic(str)
	}

	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	}

	return rf.logsGet(index).Term
}

func (rf *Raft) logsGet(index int) logEntry {
	if index <= rf.snapshotIndex {
		str := fmt.Sprintf("logsGet: index(%v) should greater than snapshot index(%v)", index, rf.snapshotIndex)
		panic(str)
	}

	i := indexInLogs(index, rf.snapshotIndex)
	return rf.logs[i]
}

func (rf *Raft) logsSlice(start, end int) []logEntry {
	// TODO: logsSlice has a bug if caller set snapshotIndex before call it
	if end < start {
		panic(fmt.Sprintf("logsSclie: end(%d) should not less than start(%d)", end, start))
	}

	if end <= rf.snapshotIndex {
		str := fmt.Sprintf("logsSlice: end(%v) should not less than snapshot index(%v)", end, rf.snapshotIndex)
		panic(str)
	}

	start = max(rf.snapshotIndex+1, start)
	start = indexInLogs(start, rf.snapshotIndex)
	end = indexInLogs(end, rf.snapshotIndex)
	return append([]logEntry{}, rf.logs[start:end]...)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int
	votedFor    int
	logs        raftLogs

	// snapshot state
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte

	// Volatile State
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// chann
	applyCh     chan ApplyMsg
	becomeFlw   chan struct{}
	canBeLeader chan struct{}
	grantVote   chan struct{}

	// state
	stateHandler func()
	state        int

	// timer
	timer *time.Timer
}

// GetState return currentTerm and whether this server
// believes it is a leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.Persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntries Invoked by leader to replicate log entries;
// also used as commandMessage.
type AppendEntries struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

// AppendEntriesReply is reply by call RPC in other server's AppendEntries method
type AppendEntriesReply struct {
	Term      int
	NextIndex int
	Success   bool
}

type SnapshotEntries struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type SnapshotEntriesReply struct {
	Term          int
	SnapshotNewer bool
}

// AppendEntries RPC handler called by leader to append logs to follower's log
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	reply.Success = false
	reply.NextIndex = 0
	reply.Term = rf.currentTerm
	lastLogIndex, _ := rf.logsLastIndexAndTerm()

	if args.Term < rf.currentTerm {
		reply.NextIndex = lastLogIndex + 1
		return
	}

	if args.Term > rf.currentTerm {
		// set follower state
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		needPersist = true
	}
	go rf.becomeFollower()

	// leader prevIndex larger than me, not match
	if args.PrevLogIndex > lastLogIndex {
		reply.NextIndex = lastLogIndex + 1
		return
	}

	// leader prevIndex less than snapshot, not match
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.NextIndex = rf.snapshotIndex + 1
		return
	}

	if args.PrevLogIndex >= 0 {
		term := rf.logsGetTerm(args.PrevLogIndex)
		// leader prevTerm not identity in the same index log term, not match
		// TODO: follower will not receive snapshot
		if term != args.PrevLogTerm {
			var i int
			for i = args.PrevLogIndex - 1; i >= rf.commitIndex; i-- {
				if rf.logsGetTerm(i) != term {
					break
				}
			}
			reply.NextIndex = i + 1
			return
		}
	}

	needPersist = true
	reply.Success = true
	newIndex := args.PrevLogIndex + 1
	rf.logs = rf.logsSlice(0, newIndex)
	rf.logs = append(rf.logs, args.Entries...)
	lastLogIndex, _ = rf.logsLastIndexAndTerm()
	reply.NextIndex = lastLogIndex + 1

	// set commitIndex and apply if there is some new logs
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
	}
	rf.applyLogs()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
// RequestVote RPC handler called by leader to requst vote for it.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	needPersist := false
	needBecomeFollower := false
	defer func() {
		if needPersist {
			rf.persist()
		}

		if needBecomeFollower {
			go rf.becomeFollower()
		}
	}()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needBecomeFollower = true
		needPersist = true
	}

	reply.Term = rf.currentTerm
	lastLogIndex, lastTerm := rf.logsLastIndexAndTerm()
	// leader's logs is beyond or equal to my logs
	candidateUpToDate := (lastTerm < args.LastLogTerm) ||
		(lastTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)
	// not vote for other candidate
	canVoteForCandidate := rf.votedFor == -1 || rf.votedFor == args.CandidateID

	if canVoteForCandidate && candidateUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		needBecomeFollower = true
		needPersist = true
	}

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	index, _ := rf.logsLastIndexAndTerm()
	index += 2
	isLeader := rf.state == leader

	// Your code here (2B).
	if isLeader {
		rf.logs = append(rf.logs, logEntry{command, term})
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// broadcastAppendEntries send the RequestVoteArgs struct to other peers expect itself,
// the peer vote for itself.
func (rf *Raft) broadcastRequestVote() {
	// vote for self
	rf.votedFor = rf.me
	lastLogIndex, lastTerm := rf.logsLastIndexAndTerm()
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastTerm}
	count := 1

	replyHandler := func(server int, reply *RequestVoteReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == candidate {
			if reply.Term > args.Term {
				rf.currentTerm = reply.Term
				rf.persist()
				go rf.becomeFollower()
				return
			}

			if reply.VoteGranted {
				if count == len(rf.peers)/2 {
					go func() {
						rf.stopTimer()
						rf.canBeLeader <- struct{}{}
					}()
				}
				count++
			}
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{-1, false}
				if rf.sendRequestVote(server, &args, &reply) {
					replyHandler(server, &reply)
				}
			}(i)
		}
	}
}

// broadcastAppendEntries send the AppendEntries struct to every server expect itself
func (rf *Raft) broadcastAppendEntries() {
	// count majority of reply
	count := 1
	lastIndex, lastTerm := rf.logsLastIndexAndTerm()
	logLen := lastIndex + 1
	currentTermHasLog := lastTerm == rf.currentTerm

	becomeFollower := func(term int) {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
		go rf.becomeFollower()
	}

	replyHandler := func(server int, args *AppendEntries, reply *AppendEntriesReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == leader && rf.currentTerm == args.Term {
			// follower think me is not a leader
			if reply.Term > rf.currentTerm {
				becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				if currentTermHasLog {
					if count == (len(rf.peers) / 2) {
						rf.commitIndex = lastIndex
						rf.applyLogs()
					}
					count++
				}
				rf.matchIndex[server] = lastIndex
			}
			rf.nextIndex[server] = max(reply.NextIndex, 0)
		}
	}

	snapshotHandler := func(server int, args *SnapshotEntries, reply *SnapshotEntriesReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == leader && rf.currentTerm == args.Term {
			// follower think me is not a leader
			if reply.Term > rf.currentTerm {
				becomeFollower(reply.Term)
				return
			}

			rf.nextIndex[server] = max(args.LastIncludedIndex + 1, 0)
			rf.matchIndex[server] = args.LastIncludedIndex
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			nextIndex := rf.nextIndex[i]

			if nextIndex > rf.snapshotIndex {
				// send the needed log to follower
				args := AppendEntries{LeaderID: rf.me, LeaderCommit: rf.commitIndex, Term: rf.currentTerm, PrevLogTerm: -1}
				args.PrevLogIndex = nextIndex - 1
				if args.PrevLogIndex > -1 {
					args.PrevLogTerm = rf.logsGetTerm(args.PrevLogIndex)
				}
				if nextIndex < logLen {
					args.Entries = rf.logsSlice(nextIndex, logLen)
				}

				go func(server int) {
					reply := AppendEntriesReply{-1, 0, false}
					if rf.sendAppendEntries(server, &args, &reply) {
						replyHandler(server, &args, &reply)
					}
				}(i)
			} else {
				args := SnapshotEntries{
					rf.currentTerm, rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot}
				reply := SnapshotEntriesReply{-1, false}
				go func(server int) {
					if rf.sendInstallSnapshot(server, &args, &reply) {
						snapshotHandler(server, &args, &reply)
					}
				}(i)
			}
		}
	}
}

// leader send a sendInstallSnapshot RPC call to follower to install a snapshot.
func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotEntries, reply *SnapshotEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) applyLogs() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		logEntry := rf.logsGet(i)
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: logEntry.Command}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) applySnapshot() {
	rf.applyCh <- ApplyMsg{UseSnapshot: true, Snapshot: rf.snapshot}
}

//
// InstallSnapshot RPC called by leader to
// send snapshots to followers that are too far behind.
// When a follower receives a snapshot with this
// RPC, it must decide what to do with its existing log entries.
// Usually the snapshot will contain new information
// not already in the recipient’s log. In this case, the follower
// discards its entire log; it is all superseded by the snapshot
// and may possibly have uncommitted entries that conflict
// with the snapshot. If instead the follower receives a snapshot
// that describes a prefix of its log (due to retransmission
// or by mistake), then log entries covered by the snapshot
// are deleted but entries following the snapshot are still
// valid and must be retained.
//
func (rf *Raft) InstallSnapshot(args *SnapshotEntries, reply *SnapshotEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// leader behind me
	if args.Term < rf.currentTerm {
		return
	}

	// leader beyond me
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	go rf.becomeFollower()

	// leader may lost snapshot
	if len(args.Snapshot) == 0 {
		return
	}

	// old/delayed RPC
	leaderSendSameSnapshot := args.LastIncludedIndex == rf.snapshotIndex &&
		args.LastIncludedTerm == rf.snapshotTerm
	if leaderSendSameSnapshot {
		return
	}

	// use snapshot
	rf.logs = make(raftLogs, 0)
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Snapshot
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = rf.commitIndex

	// save snapshot
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.snapshot)
	rf.Persister.SaveSnapshot(w.Bytes())
	rf.persist()
	rf.applySnapshot()
}

// SaveSnapshot called by server if it think the log entrys size is too large,
// raft discard old entry before and include index log, save snapshot
// from data.
func (rf *Raft) SaveSnapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied < index {
		str := fmt.Sprintf("Raft SaveSnapshot should not save snapshot up to %v larger than last Applyied log(%v)", index, rf.lastApplied)
		panic(str)
	}

	if index < rf.snapshotIndex {
		str := fmt.Sprintf("Raft SaveSnapshot parameter index(%v) should not less or equal than snapshot index(%v)", index, rf.snapshotIndex)
		panic(str)
	}

	lastIndex, _ := rf.logsLastIndexAndTerm()
	rf.snapshotTerm = rf.logsGetTerm(index)
	rf.logs = rf.logsSlice(index+1, lastIndex+1)
	rf.snapshotIndex = index
	rf.snapshot = data
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.snapshot)
	rf.Persister.SaveSnapshot(w.Bytes())
	rf.persist()
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := bytes.NewBuffer(data)
	e := gob.NewDecoder(w)
	e.Decode(&rf.snapshotIndex)
	e.Decode(&rf.snapshotTerm)
	e.Decode(&rf.snapshot)
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.commitIndex
	rf.applySnapshot()
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
	rf.peers = peers
	rf.Persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.logs = make(raftLogs, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.snapshotIndex = -1
	rf.snapshotTerm = -1
	rf.state = follower
	rf.becomeFlw = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.timer = time.NewTimer(-1)
	rf.stateHandler = makeStateHandler(rf, follower)
	// restore state from persister after a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// state machine handler
	go func() {
		rf.stopTimer()
		for {
			rf.stateHandler()
		}
	}()

	return rf
}
