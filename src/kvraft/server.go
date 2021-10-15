package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key   string
	Value string
	Op    string // "Put" or "Append" or "Get"

	ClientId int64
	Seq      int32
}

/* 需要持久化 */
type ClientsOpRecord struct {
	/* 操作 */
	Op Op
	/* 结果 */
	ResultValue string
	Error       Err
}

type NoticeCh struct {
	ApplyMsgCh chan *ClientsOpRecord
	// NotLeaderEventCh chan interface{}
}

type ActiveClient struct {
	ClientId  int64
	NoticeChs *NoticeCh
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	KVTable         map[string]string          /* 服务器状态机 */
	ClientsOpRecord map[int64]*ClientsOpRecord /* clientId -> lastClientOpRecord 客户端的最后请求的历史记录（后期可以修改为所有的历史记录） */
	ActiveClients   map[int64]*ActiveClient    /* clientId -> activeClient */
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	/* 如果保留的客户最后一条记录 seq  和当前rpc相同，则直接返回记录值 */
	if record, ok := kv.ClientsOpRecord[args.ClientId]; ok && record.Op.Seq == args.Seq {
		reply.Error = record.Error
		reply.Value = record.ResultValue
		kv.mu.Unlock()
		return
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Key:      args.Key,
		Op:       "Get",
	}

	applyMsgCh := make(chan *ClientsOpRecord, 1)
	// notLeaderEventCh := make(chan interface{}, 1)
	noticeChs := &NoticeCh{applyMsgCh /* , notLeaderEventCh */}
	/* 活跃客户端 */
	if c, ok := kv.ActiveClients[args.ClientId]; !ok {
		/* 如果不存在该客户端 */
		client := &ActiveClient{
			ClientId:  args.ClientId,
			NoticeChs: noticeChs,
		}
		kv.ActiveClients[args.ClientId] = client
	} else {
		/* 由于这里不允许单个客户端发送多个请求rpc，
		此时这个rpc超时会直接返回，那咱就不多余处理了 */
		// if c.NoticeChs != nil {
		// }
		c.NoticeChs = noticeChs
	}

	/* 开启定时器（租约） */
	leaseTimeOut := time.NewTimer(raft.VoteTimeOutBase * time.Millisecond)
	defer leaseTimeOut.Stop()
	/* 添加命令到 raft 日志中 */
	_, _, isLeader := kv.rf.Start(op)

	kv.mu.Unlock()
	if !isLeader {
		reply.Error = ErrWrongLeader
	} else {
		/* 等待 raft 处理 */
		select {
		// case <-notLeaderEventCh:
		// reply.Error = ErrWrongLeader
		case msg := <-applyMsgCh:
			reply.Error = msg.Error
			reply.Value = msg.ResultValue
		case <-leaseTimeOut.C:
			reply.Error = ErrTimeOut
		}
	}
	DPrintf("KV[%d] will send Get %s Event to C[%d] Seq[%d]", kv.me, reply.Error, args.ClientId, args.Seq)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	/* 如果保留的客户最后一条记录 seq  和当前rpc相同，则直接返回记录值 */
	if record, ok := kv.ClientsOpRecord[args.ClientId]; ok && record.Op.Seq == args.Seq {
		reply.Error = record.Error
		kv.mu.Unlock()
		return
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
	}

	applyMsgCh := make(chan *ClientsOpRecord, 1)
	// notLeaderEventCh := make(chan interface{}, 1)
	noticeChs := &NoticeCh{applyMsgCh /* ,notLeaderEventCh */}
	/* 活跃客户端 */
	if c, ok := kv.ActiveClients[args.ClientId]; !ok {
		/* 如果不存在该客户端 */
		client := &ActiveClient{
			ClientId:  args.ClientId,
			NoticeChs: noticeChs,
		}
		kv.ActiveClients[args.ClientId] = client
	} else {
		c.NoticeChs = noticeChs
	}

	/* 开启定时器（租约） */
	leaseTimeOut := time.NewTimer(raft.VoteTimeOutBase)
	defer leaseTimeOut.Stop()
	/* 添加命令到 raft 日志中 */
	_, _, isLeader := kv.rf.Start(op)

	kv.mu.Unlock()
	if !isLeader {
		reply.Error = ErrWrongLeader
	} else {
		/* 等待 raft 处理 */
		select {
		// case <-notLeaderEventCh:
		// reply.Error = ErrWrongLeader
		case msg := <-applyMsgCh:
			reply.Error = msg.Error
		case <-leaseTimeOut.C:
			reply.Error = ErrTimeOut
		}
	}
	DPrintf("KV[%d] will send %s %s Event to C[%d] Seq[%d]", kv.me, args.Op, reply.Error, args.ClientId, args.Seq)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(raft.RaftLogs{})
	labgob.Register([]raft.RaftLog{})

	kv := &KVServer{}
	kv.me = me
	kv.dead = 0
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.KVTable = make(map[string]string)
	/* 暂且不考虑 kv.ClientsOpRecord 的崩溃恢复，因为日志反正都是会重放的， */
	kv.ClientsOpRecord = make(map[int64]*ClientsOpRecord)
	kv.ActiveClients = make(map[int64]*ActiveClient)

	// go func() {
	// 	wait_ch := make(chan interface{})
	// 	kv.rf.RegisterNotLeaderNowCh(wait_ch)
	// 	for !kv.killed() {
	// 		DPrintf("KV[%d] wait for NotLeaderEvent", kv.me)
	// 		<-wait_ch
	// 		kv.mu.Lock()
	// 		for _, client := range kv.ActiveClients {
	// 			go func(client *ActiveClient) {
	// 				// if client.NoticeChs != nil {
	// 				// client.NoticeChs.NotLeaderEventCh <- interface{}(nil)
	// 				// }
	// 			}(client)
	// 		}
	// 		kv.mu.Unlock()
	// 	}
	// }()
	/* 监听客户端从 applyCh 的提交 */
	go func() {
		for msg := range kv.applyCh {
			DPrintf("KV[%d] applyCh: %+v\n", kv.me, msg)
			if msg.CommandValid {
				switch msg.Command.(type) {
				case Op:
					cmdOp := msg.Command.(Op)
					switch cmdOp.Op {
					case "Get":
						var record *ClientsOpRecord
						ok := false
						kv.mu.Lock()

						/* 寻找已经发送的历史记录中是否存在该操作，有则直接返回记录 */
						record, ok = kv.ClientsOpRecord[cmdOp.ClientId]
						if (ok && record.Op.Seq < cmdOp.Seq) || (!ok) {
							/* 记录中只有更小的序列号,则我们可以构造新记录并替换 */
							record = &ClientsOpRecord{Op: cmdOp}
							/* resultValue 和 err */
							if value, ok := kv.KVTable[cmdOp.Key]; ok {
								record.ResultValue = value
								record.Error = OK
							} else {
								record.Error = ErrNoKey
							}
							kv.ClientsOpRecord[cmdOp.ClientId] = record
						} else if ok && record.Op.Seq > cmdOp.Seq {
							/* 我们需要返回错误：ErrNeedRetry (更新的请求) */
							record = &ClientsOpRecord{Op: cmdOp, Error: ErrNeedBiggerSeq}
						} else {
							// 返回该历史结果
						}

						if activeClient, ok := kv.ActiveClients[cmdOp.ClientId]; ok {
							if activeClient.NoticeChs != nil {
								activeClient.NoticeChs.ApplyMsgCh <- record
								activeClient.NoticeChs = nil
							}
						}
						/* 使用有缓冲区的 channel，
						即便是对面没有人接受也是可以接受的 */
						kv.mu.Unlock()
					case "Put", "Append":
						var record *ClientsOpRecord
						ok := false
						kv.mu.Lock()

						/* 寻找已经发送的历史记录中是否存在该操作，有则直接返回记录 */
						record, ok = kv.ClientsOpRecord[cmdOp.ClientId]
						if (ok && record.Op.Seq < cmdOp.Seq) || (!ok) {
							/* 记录中只有更小的序列号,则我们可以构造新记录并替换 */
							/* resultValue 和 err */
							if cmdOp.Op == "Append" {
								kv.KVTable[cmdOp.Key] += cmdOp.Value
							} else if cmdOp.Op == "Put" {
								kv.KVTable[cmdOp.Key] = cmdOp.Value
							}
							record = &ClientsOpRecord{Op: cmdOp, Error: OK}
							kv.ClientsOpRecord[cmdOp.ClientId] = record
						} else if ok && record.Op.Seq > cmdOp.Seq {
							/* 我们需要返回错误：ErrNeedRetry (更新的请求) */
							record = &ClientsOpRecord{Op: cmdOp, Error: ErrNeedBiggerSeq}
						} else {
							// 返回该历史结果
						}
						if activeClient, ok := kv.ActiveClients[cmdOp.ClientId]; ok {
							if activeClient.NoticeChs != nil {
								activeClient.NoticeChs.ApplyMsgCh <- record
								activeClient.NoticeChs = nil
							}
						}
						kv.mu.Unlock()
					default:
						log.Fatalf("KV[%d] applyCh: unknown command\n", kv.me)
					}
				case string:
					cmdOp := msg.Command.(string)
					if cmdOp != "INVALID" {
						log.Fatalf("KV[%d] applyCh: unknown command %v\n", kv.me, msg.Command)
					}
				default:
					log.Fatalf("KV[%d] applyCh: unknown command %v\n", kv.me, msg.Command)
				}
			} else {
				log.Fatalf("KV[%d] applyCh: unknown command %v\n", kv.me, msg.Command)
			}
		}
	}()
	return kv
}
