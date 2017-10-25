package raftkv

const (
	OK           = "OK"
	ErrNoKey     = "ErrNoKey"
	ErrNotLeader = "ErrNotLeader"
	ErrCommit    = "ErrCommit"
)

type Err string

type RequstArgs struct {
	Session  int64
	RequstID int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequstArgs
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequstArgs
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
