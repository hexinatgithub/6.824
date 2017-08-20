package raft

import (
	"math/rand"
	"time"
)

const (
	follower = iota
	candidate
	leader
)

// broadcoastAppendEntries send the RequestVoteArgs struct to other peers expect itself,
// the peer vote for itself.
func broadcoastRequestVote(rf *Raft) {
	// vote for self
	rf.votedFor = rf.me
	lastLogIndex, lastTerm := rf.getLastIndexAndTerm()
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastTerm}
	count := 1

	for i := range rf.peers {
		if i != rf.me {
			i := i
			go func() {
				reply := RequestVoteReply{-1, false}
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok && reply.VoteGranted {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					count++
					if rf.state == candidate && (count > len(rf.peers)/2) {
						rf.isLeader <- struct{}{}
					}
				}
			}()
		}
	}
}

// broadcoastAppendEntries send the AppendEntries struct to every server expect itself
func broadcoastAppendEntries(rf *Raft, logs []interface{}) {
	lastLogIndex, lastTerm := rf.getLastIndexAndTerm()
	args := AppendEntries{rf.currentTerm, rf.me, lastLogIndex,
		lastTerm, logs, rf.commitIndex}
	for i := range rf.peers {
		if i != rf.me {
			i := i
			go func() {
				reply := AppendEntriesReply{-1, false}
				rf.sendAppendEntries(i, &args, &reply)
			}()
		}
	}
}

// electionTime generate random election timeout
func electionTime() time.Duration {
	f := time.Duration(rand.Int31n(150) + 300)
	return time.Duration(f * time.Millisecond)
}

// setRaftState use safe way to change the Raft's state
// f is caller defined function to change the Raft's state, f should not call goroutine,
// otherwise the setRaftState will lose safety meaning.
func setRaftState(rf *Raft, state int, f func()) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if state != rf.state {
		rf.state = state
		rf.stateHandle = makeStateHandler(rf, state)
	}
	if f != nil {
		f()
	}
}

// makeFollowerHandler create a followerHandler handle,
// followerHandler handle the Raft action when it is in follower state
func makeFollowerHandler(rf *Raft) func() {
	return func() {
		select {
		case <-rf.heartbeat:
		case <-rf.grantVote:
		case <-time.After(electionTime()):
			setRaftState(rf, candidate, func() {
				rf.currentTerm++
				rf.isLeader = make(chan struct{}, 1)
				broadcoastRequestVote(rf)
			})
		}
	}
}

// makeCandidateHandler create a candidateHandler handle,
// candidateHandler handle the Raft action when it is in candidate state
func makeCandidateHandler(rf *Raft) func() {
	return func() {
		closeChan := func() {
			close(rf.isLeader)
		}
		select {
		case <-rf.grantVote:
			setRaftState(rf, follower, closeChan)
		case <-rf.heartbeat:
			setRaftState(rf, follower, closeChan)
		case <-rf.isLeader:
			setRaftState(rf, leader, closeChan)
		case <-time.After(electionTime()):
			setRaftState(rf, candidate, func() {
				closeChan()
				rf.isLeader = make(chan struct{}, 1)
				rf.currentTerm++
				broadcoastRequestVote(rf)
			})
		}
	}
}

// makeLeaderHandler create a leaderHandler handle,
// leaderHandler handle the Raft action when it is in leader state
func makeLeaderHandler(rf *Raft) func() {
	return func() {
		select {
		case <-rf.grantVote:
			setRaftState(rf, follower, nil)
		case <-rf.heartbeat:
			setRaftState(rf, follower, nil)
		case <-time.After(time.Second / 10):
			setRaftState(rf, leader, func() {
				broadcoastAppendEntries(rf, nil)
			})
		}
	}
}

func makeStateHandler(rf *Raft, state int) func() {
	var handler func()
	switch state {
	case follower:
		handler = makeFollowerHandler(rf)
	case candidate:
		handler = makeCandidateHandler(rf)
	case leader:
		handler = makeLeaderHandler(rf)
	}
	return handler
}
