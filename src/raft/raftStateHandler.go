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

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
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

func (rf *Raft) becomeFollower() {
	rf.stopTimer()
	rf.becomeFlw <- struct{}{}
}

// changeState use safe way to change the Raft's state
// f is caller defined function to change the Raft's state, f should not call goroutine,
// otherwise the changeState will lose safety meaning.
func (rf *Raft) changeState(state int, f func()) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if state != rf.state {
		rf.state = state
		rf.stateHandler = makeStateHandler(rf, state)
	}
	f()
}

func electionTime() time.Duration {
	f := time.Duration(rand.Int31n(300) + 300)
	return time.Duration(f * time.Millisecond)
}

// makeFollowerHandler create a followerHandler handle,
// followerHandler handle the Raft action when it is in follower state
func makeFollowerHandler(rf *Raft) func() {
	return func() {
		rf.resetTimer(electionTime())
		select {
		case <-rf.becomeFlw:
		case <-rf.timer.C:
			rf.changeState(candidate, func() {
				rf.currentTerm++
				rf.canBeLeader = make(chan struct{}, 1)
				rf.broadcastRequestVote()
			})
		}
	}
}

// makeCandidateHandler create a candidateHandler handle,
// candidateHandler handle the Raft action when it is in candidate state
func makeCandidateHandler(rf *Raft) func() {
	return func() {
		rf.resetTimer(electionTime())
		closeChan := func() {
			close(rf.canBeLeader)
		}
		select {
		case <-rf.becomeFlw:
			rf.changeState(follower, closeChan)
		case <-rf.canBeLeader:
			rf.changeState(leader, func() {
				rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))
				lastIndex, _ := rf.logsLastIndexAndTerm()
				for i := range rf.nextIndex {
					rf.nextIndex[i] = lastIndex + 1
					rf.matchIndex[i] = lastIndex
				}
				closeChan()
				rf.broadcastAppendEntries()
			})
		case <-rf.timer.C:
			rf.changeState(candidate, func() {
				rf.currentTerm++
				rf.broadcastRequestVote()
			})
		}
	}
}

// makeLeaderHandler create a leaderHandler handle,
// leaderHandler handle the Raft action when it is in leader state
func makeLeaderHandler(rf *Raft) func() {
	return func() {
		rf.resetTimer(time.Second / 11)
		releaseFun := func() {
			rf.nextIndex = nil
			rf.matchIndex = nil
		}
		select {
		case <-rf.becomeFlw:
			rf.changeState(follower, releaseFun)
		case <-rf.timer.C:
			rf.changeState(leader, rf.broadcastAppendEntries)
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
