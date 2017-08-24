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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int
	votedFor    int
	log         []logEntry

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
	stateHandle func()
	state       int

	// timer
	timer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
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
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	Term    int
	Success bool
}

// AppendEntries RPC handler receive commandMessage or leader's AppendEntries struct
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		go rf.becomeFollower()
		return
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	lastLogIndex, _ := rf.getLastIndexAndTerm()
	matchLeaderLog := args.PrevLogIndex == -1 || (lastLogIndex >= args.PrevLogIndex &&
		rf.log[args.PrevLogIndex].Term == args.PrevLogTerm)
	// same term but not match leader log
	if !matchLeaderLog {
		go rf.becomeFollower()
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	reply.Success = true
	newIndex := args.PrevLogIndex + 1
	conflictWithNew := lastLogIndex >= newIndex &&
		rf.log[newIndex].Term != args.Term
	if conflictWithNew {
		rf.log = rf.log[:newIndex]
	}

	// add any new log entries
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
	}

	// set commitIndex and commit if there is someting
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, newIndex)
		// println("change commit id", rf.commitIndex)
	}

	go rf.becomeFollower()
	go rf.applyLogs()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	lastLogIndex, lastTerm := rf.getLastIndexAndTerm()
	candidateUpDate := (lastTerm != args.LastLogTerm && lastTerm < args.LastLogTerm) ||
		(lastTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// grant if candidate has all committed logs
		reply.VoteGranted = candidateUpDate
		go rf.becomeFollower()
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		candidateUpDate {
		reply.VoteGranted = true
		go rf.becomeFollower()
	}

	// if request vote server is at least as up-to-date to me,
	// vote for it.
	if reply.VoteGranted {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		// println("me vote for:", rf.me, rf.grantVote, args.CandidateID, rf.currentTerm, args.Term, rf.votedFor)
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
	index, _ := rf.getLastIndexAndTerm()
	index += 2
	isLeader := rf.state == leader

	// Your code here (2B).
	if isLeader {
		rf.log = append(rf.log, logEntry{command, term})
		// println("append log", rf.me, command.(int))
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

// getLastIndexAndTerm caller should lock the state before call this method
func (rf *Raft) getLastIndexAndTerm() (int, int) {
	l := len(rf.log)
	if l == 0 {
		return -1, -1
	}
	lastLogIndex := l - 1
	lastTerm := rf.log[lastLogIndex].Term
	return lastLogIndex, lastTerm
}

// broadcoastAppendEntries send the RequestVoteArgs struct to other peers expect itself,
// the peer vote for itself.
func (rf *Raft) broadcoastRequestVote() {
	// vote for self
	rf.votedFor = rf.me
	lastLogIndex, lastTerm := rf.getLastIndexAndTerm()
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastTerm}
	countVote := 1
	changeState := false

	requestVoteHandler := func(server int, reply *RequestVoteReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == candidate {
			if reply.VoteGranted {
				if countVote == len(rf.peers)/2 {
					go func() {
						rf.stopTimer()
						rf.canBeLeader <- struct{}{}
					}()
				}
				countVote++
			} else if reply.Term > args.Term {
				if !changeState {
					rf.currentTerm = reply.Term
					changeState = true
					go rf.becomeFollower()
				}
			}
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{-1, false}
				if rf.sendRequestVote(server, &args, &reply) {
					requestVoteHandler(server, &reply)
				}
			}(i)
		}
	}
}

// broadcoastAppendEntries send the AppendEntries struct to every server expect itself
func (rf *Raft) broadcoastAppendEntries() {
	// count majority of reply
	count := 1
	logLen := len(rf.log)
	lastIndex := logLen - 1
	changeState := false

	repleyHandler := func(server int, args *AppendEntries, reply *AppendEntriesReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == leader {
			if reply.Success {
				if count == (len(rf.peers) / 2) {
					rf.commitIndex = lastIndex
					go rf.applyLogs()
				}
				count++
				rf.nextIndex[server] = logLen
				rf.matchIndex[server] = lastIndex
			} else if reply.Term > args.Term {
				if !changeState {
					rf.currentTerm = reply.Term
					changeState = true
					go rf.becomeFollower()
				}
			} else {
				// println("send to server failed", server, rf.nextIndex[server], lastIndex)
				rf.nextIndex[server] = max(rf.nextIndex[server]-1, 0)
			}
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			args := AppendEntries{PrevLogTerm: -1}
			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex > -1 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			if rf.nextIndex[i] < logLen {
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			args.LeaderCommit = rf.commitIndex
			reply := AppendEntriesReply{-1, false}

			go func(server int) {
				if rf.sendAppendEntries(server, &args, &reply) {
					repleyHandler(server, &args, &reply)
				}
			}(i)
		}
	}
}

func (rf *Raft) stopTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if it is expire, drain the channel
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
			return
		}
	}
}

func (rf *Raft) resetTimer(duration time.Duration) {
	rf.stopTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.timer.Reset(duration) {
		panic("timer is running or not stop")
	}
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		// println(rf.me, "apply-index:", i, "currentTerm:", rf.currentTerm, "command", rf.log[i].Command.(int))
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: rf.log[i].Command}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) becomeFollower() {
	rf.stopTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeFlw <- struct{}{}
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
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.log = make([]logEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = follower
	rf.becomeFlw = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.timer = time.NewTimer(-1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	_, rf.currentTerm = rf.getLastIndexAndTerm()
	rf.stateHandle = makeStateHandler(rf, follower)
	go func() {
		rf.stopTimer()
		for {
			rf.stateHandle()
		}
	}()
	return rf
}
