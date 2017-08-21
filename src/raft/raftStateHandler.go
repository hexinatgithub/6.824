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

func resetTimer(timer *time.Timer, duration time.Duration) {
	if timer.Reset(duration) {
		panic("timer is running or not stop")
	}
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

func electionTime() time.Duration {
	f := time.Duration(rand.Int31n(300) + 300)
	return time.Duration(f * time.Millisecond)
}

// makeFollowerHandler create a followerHandler handle,
// followerHandler handle the Raft action when it is in follower state
func makeFollowerHandler(rf *Raft) func() {
	return func() {
		resetTimer(rf.timer, electionTime())
		select {
		case <-rf.commandMessage:
			// println("follower receive command message", rf.me)
		case <-rf.timer.C:
			// println(rf.me, "follower to candidate")
			setRaftState(rf, candidate, func() {
				rf.currentTerm++
				rf.canBeLeader = make(chan bool, 1)
				rf.broadcoastRequestVote()
			})
		}
	}
}

// makeCandidateHandler create a candidateHandler handle,
// candidateHandler handle the Raft action when it is in candidate state
func makeCandidateHandler(rf *Raft) func() {
	return func() {
		resetTimer(rf.timer, electionTime())
		closeChan := func() {
			close(rf.canBeLeader)
		}
		select {
		case <-rf.commandMessage:
			setRaftState(rf, follower, closeChan)
		case ok := <-rf.canBeLeader:
			if ok {
				// println(rf.me, "candidate become Leader")
				setRaftState(rf, leader, func() {
					rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))
					logLength := len(rf.log)
					for i := range rf.nextIndex {
						rf.nextIndex[i] = logLength
					}
					closeChan()
				})
			} else {
				setRaftState(rf, follower, closeChan)
			}
		case <-rf.timer.C:
			setRaftState(rf, candidate, func() {
				rf.currentTerm++
				rf.broadcoastRequestVote()
				// println(rf.me, "reelection")
			})
		}
	}
}

// makeLeaderHandler create a leaderHandler handle,
// leaderHandler handle the Raft action when it is in leader state
func makeLeaderHandler(rf *Raft) func() {
	return func() {
		resetTimer(rf.timer, time.Second/11)
		// println("leader", ok)
		releaseFun := func() {
			rf.nextIndex = nil
			rf.matchIndex = nil
		}
		select {
		case <-rf.commandMessage:
			setRaftState(rf, follower, releaseFun)
			// println(rf.me, "leader become follower")
		case <-rf.timer.C:
			setRaftState(rf, leader, rf.broadcoastAppendEntries)
			// println(rf.me, "heartbeat")
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
