package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	requestID int
	seesionID int64
	leader    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seesionID = nrand()
	ck.requestID = 1
	ck.leader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	ck.mu.Lock()
	args.Session = ck.seesionID
	args.RequstID = ck.requestID
	ck.requestID++
	ck.mu.Unlock()

	for {
		for i := ck.leader; ; {
			var reply GetReply
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
			if ok && !reply.WrongLeader {
				ck.SaveLeader(i)
				switch reply.Err {
				case OK:
					return reply.Value
				case ErrNoKey:
					return ""
				default:
					continue
				}
			}
			i++
			i %= len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	ck.mu.Lock()
	args.Session = ck.seesionID
	args.RequstID = ck.requestID
	ck.requestID++
	ck.mu.Unlock()

	for {
		for i := ck.leader; ; {
			var reply PutAppendReply
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
			if ok && !reply.WrongLeader {
				ck.SaveLeader(i)
				switch reply.Err {
				case OK:
					return
				default:
					continue
				}
			}
			i++
			i %= len(ck.servers)
		}
	}
}

func (ck *Clerk) SaveLeader(serve int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader = serve
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OP_PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OP_APPEND)
}
