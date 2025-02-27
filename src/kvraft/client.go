package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"fmt"
	_ "log"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	cmdSeq    int32
	clientId  int64
	curServer int
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
	ck.curServer = (int)(nrand() % (int64)(len(ck.servers)))
	ck.clientId = nrand()
	ck.cmdSeq = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	begin := time.Now()
	defer func() {
		end := time.Now()
		DPrintf("C[%d] Get %dms", ck.clientId, (end.Sub(begin)).Milliseconds())
	}()
	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		Seq:      atomic.AddInt32(&ck.cmdSeq, 1),
	}
	reply := &GetReply{}

	if ok := ck.servers[ck.curServer].Call("KVServer.Get", args, reply); ok {
		if val, err := ck.handleGetReply(args, reply); err == nil {
			DPrintf("C[%d] Get ok!", ck.clientId)
			return val
		} else {
			// DPrintf("C[%d] Get error:%s", ck.clientId, err)
		}
	}

	for {
		args := &GetArgs{
			Key:      key,
			ClientId: ck.clientId,
			Seq:      args.Seq,
		}
		reply = &GetReply{}
		i := (int)(nrand() % (int64)(len(ck.servers)))
		if ck.curServer == i {
			continue
		}
		ck.curServer = i

		if ok := ck.servers[i].Call("KVServer.Get", args, reply); !ok {
			// log.Printf("C[%d] Get Call failed", ck.clientId)
			continue
		}

		if val, err := ck.handleGetReply(args, reply); err != nil {
			// log.Printf("C[%d] Get error:%s", ck.clientId, err)
			continue
		} else {
			// DPrintf("C[%d] Get ok!", ck.clientId)
			// DPrintf("C[%d] Get v:%v", ck.clientId, val)
			return val
		}
	}
}

func (ck *Clerk) handleGetReply(args *GetArgs, reply *GetReply) (string, error) {
	switch reply.Error {
	case ErrWrongLeader, ErrTimeOut:
		return "", fmt.Errorf("%v", reply.Error)
		/* retry */
	case ErrNoKey:
		return "", nil
	case OK:
		return reply.Value, nil
	default:
		return "", fmt.Errorf("unknown error")
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	begin := time.Now()
	defer func() {
		end := time.Now()
		DPrintf("C[%d] %s %dms", ck.clientId, op, (end.Sub(begin)).Milliseconds())
	}()
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		Seq:      atomic.AddInt32(&ck.cmdSeq, 1),
	}
	reply := &PutAppendReply{}
	DPrintf("C[%d] first send args=%+v to %d", ck.clientId, args, ck.curServer)
	if ok := ck.servers[ck.curServer].Call("KVServer.PutAppend", args, reply); ok {
		if err := ck.handlePutAppendReply(args, reply); err == nil {
			DPrintf("C[%d] %s {k:%v v:%v} OK", ck.clientId, op, key, value)
			return
		} else {
			// DPrintf("C[%d] %s error:%s", ck.clientId, op, err)
		}
	}

	for {
		args := &PutAppendArgs{
			Key:      key,
			Value:    value,
			ClientId: ck.clientId,
			Seq:      args.Seq,
			Op:       op,
		}
		reply = &PutAppendReply{}
		i := (int)(nrand() % (int64)(len(ck.servers)))
		if ck.curServer == i {
			continue
		}
		/* 选择新的服务器进行发送 */
		ck.curServer = i
		DPrintf("C[%d] reSend args=%+v to %d", ck.clientId, args, i)

		if ok := ck.servers[ck.curServer].Call("KVServer.PutAppend", args, reply); !ok {
			continue
		}
		if err := ck.handlePutAppendReply(args, reply); err != nil {
			// DPrintf("C[%d] %s error:%s", ck.clientId, op, err)
			continue
		} else {
			DPrintf("C[%d] %s {k:%v v:%v} OK", ck.clientId, op, key, value)
			return
		}
	}
}

func (ck *Clerk) handlePutAppendReply(args *PutAppendArgs, reply *PutAppendReply) error {
	switch reply.Error {
	case ErrWrongLeader, ErrTimeOut:
		/* retry */
		return fmt.Errorf("%v", reply.Error)
	case OK:
		return nil
	default:
		return fmt.Errorf("unknown error")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
