package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	/* 客户端唯一标识符 */
	ClientId int64
	Seq      int32
}

type PutAppendReply struct {
	Error string
}

type GetArgs struct {
	Key string

	/* 客户端唯一标识符 */
	ClientId int64
	Seq      int32
}

type GetReply struct {
	Error   string
	Value string
}
