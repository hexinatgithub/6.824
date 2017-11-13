package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_PUT    = "Put"
	OP_GET    = "Get"
	OP_APPEND = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Kind  string
	Key   string
	Value string
	RequstArgs
}

func (op *Op) isEqual(other Op) bool {
	if other.Kind != op.Kind {
		return false
	}

	switch op.Kind {
	case OP_GET, OP_APPEND:
		return other.Key == op.Key && other.Kind == op.Kind &&
			other.RequstArgs == op.RequstArgs
	case OP_PUT:
		return other == *op
	default:
		return false
	}
}

type CommitReply struct {
	// CommitReply object send back from watch goroutine inlucde
	// some information the RPC call need to reply to client.
	op  Op
	err Err
}

// ApplyReply include some information the RaftKV whether or not
// apply some log success.
type ApplyReply CommitReply

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	commitReply map[int]chan CommitReply
	history     map[int64]int // client session map to committed requestID
}

func (kv *RaftKV) commitLog(op Op) CommitReply {
	kv.mu.Lock()
	var reply CommitReply
	reply.op.Kind = op.Kind
	reply.op.Key = op.Key
	_, isLeader := kv.rf.GetState()

	// not leader return
	if !isLeader {
		kv.mu.Unlock()
		reply.err = ErrNotLeader
		return reply
	}

	// duplicate request, just return data
	if kv.history[op.Session] >= op.RequstID {
		reply.err = OK
		reply.op.Value = kv.data[op.Key]
		kv.mu.Unlock()
		return reply
	}

	// commit log
	index, term, isLeader := kv.rf.Start(op)
	ch := make(chan CommitReply, 1)
	kv.commitReply[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		close(kv.commitReply[index])
		delete(kv.commitReply, index)
	}()

	select {
	case reply = <-ch:
		currentTerm, _ := kv.rf.GetState()
		if !op.isEqual(reply.op) || term != currentTerm {
			reply.err = ErrNotLeader
		}
	case <-time.After(time.Second):
		reply.err = ErrCommit
	}
	return reply
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	logEntry := Op{Kind: OP_GET, Key: args.Key, RequstArgs: RequstArgs{Session: args.Session, RequstID: args.RequstID}}
	commitRpl := kv.commitLog(logEntry)
	reply.Err = commitRpl.err
	reply.WrongLeader = commitRpl.err == ErrNotLeader
	reply.Value = commitRpl.op.Value
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	logEntry := Op{Kind: args.Op, Key: args.Key, Value: args.Value, RequstArgs: RequstArgs{Session: args.Session, RequstID: args.RequstID}}
	commitRpl := kv.commitLog(logEntry)
	reply.Err = commitRpl.err
	reply.WrongLeader = commitRpl.err == ErrNotLeader
}

func (kv *RaftKV) applyLog(op Op) ApplyReply {
	var reply ApplyReply
	reply.err = OK
	reply.op = op

	// duplicate request, just return data
	if kv.history[op.Session] >= op.RequstID {
		reply.op.Value = kv.data[op.Key]
		return reply
	}

	// apply log
	switch op.Kind {
	case OP_GET:
		_, ok := kv.data[op.Key]
		if !ok {
			reply.err = ErrNoKey
		}
	case OP_PUT:
		kv.data[op.Key] = op.Value
	case OP_APPEND:
		kv.data[op.Key] += op.Value
	}
	reply.op.Value = kv.data[op.Key]
	// save session
	kv.history[op.Session] = op.RequstID

	return reply
}

func (kv *RaftKV) watch() {
	for msg := range kv.applyCh {
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			// use snapshot to restore state.
			if msg.UseSnapshot {
				kv.restoreSnapshot(msg.Snapshot)
				return
			}

			op := msg.Command.(Op)
			applyReply := kv.applyLog(op)

			// only leader have reply channel
			rplChan, ok := kv.commitReply[msg.Index]
			if ok {
				rplChan <- CommitReply(applyReply)
			}

			// If the max raft state size greater than raft state size,
			// then save the snapshot.
			exceedSize := kv.maxraftstate > -1 &&
				kv.maxraftstate <= kv.rf.Persister.RaftStateSize()
			if exceedSize {
				kv.saveSnapshot(msg.Index - 1)
			}
		}()
	}
}

func (kv *RaftKV) saveSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.history)
	data := w.Bytes()
	kv.rf.SaveSnapshot(index, data)
}

func (kv *RaftKV) restoreSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	kv.data = make(map[string]string)
	kv.history = make(map[int64]int)
	d.Decode(&kv.data)
	d.Decode(&kv.history)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.history = make(map[int64]int)
	kv.commitReply = make(map[int]chan CommitReply)

	// You may need initialization code here.
	go kv.watch()

	return kv
}
