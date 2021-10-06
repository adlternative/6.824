package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/jwangsadinata/go-multimap/slicemultimap"
)

const Debug = false

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
	Error       string
}

type NoticeCh struct {
	ApplyMsgChs       chan *ClientsOpRecord
	NotLeaderEventChs chan *ClientsOpRecord
}

type ActiveClient struct {
	ClientId    int64
	NoticeChMap *slicemultimap.MultiMap
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	memTable        map[string]string         /* 服务器状态机 */
	ClientsOpRecord map[int64]ClientsOpRecord /* 客户端的最后请求的历史记录（后期可以修改为所有的历史记录） */
	activeClients   map[int64]*ActiveClient   /* clientId -> activeClient */
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	/* 幂等操作处理 */
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
	notLeaderEventCh := make(chan *ClientsOpRecord, 1)
	noticeChs := &NoticeCh{applyMsgCh, notLeaderEventCh}

	if client, ok := kv.activeClients[args.ClientId]; ok {
		if client.NoticeChMap == nil {
			client.NoticeChMap = slicemultimap.New()
		}
		client.NoticeChMap.Put(args.Seq, noticeChs)
	} else {
		kv.activeClients[args.ClientId] = &ActiveClient{
			ClientId:    args.ClientId,
			NoticeChMap: slicemultimap.New(),
		}
		kv.activeClients[args.ClientId].NoticeChMap.Put(args.Seq, noticeChs)
	}
	leaseTimeOut := time.NewTimer(kv.rf.VoteTimeOut)
	defer leaseTimeOut.Stop()

	_, oldTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Error = ErrWrongLeader
		kv.activeClients[args.ClientId].NoticeChMap.Remove(args.Seq, noticeChs)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	/* 等待 raft 处理 */

	select {
	case <-notLeaderEventCh:
		kv.mu.Lock()
		DPrintf("KV[%d] will send NotLeaderEvent to C[%d] Seq[%d]", kv.me, args.ClientId, args.Seq)
		reply.Error = ErrWrongLeader
	case msg := <-applyMsgCh:
		kv.mu.Lock()
		DPrintf("KV[%d] will send Get reply %+v to C[%d] Seq[%d]", kv.me, reply, args.ClientId, args.Seq)
		if term, isLeader := kv.rf.GetState(); !isLeader || term != oldTerm {
			reply.Error = ErrWrongLeader
		} else {
			reply.Error = msg.Error
			reply.Value = msg.ResultValue
		}
	case <-leaseTimeOut.C:
		kv.mu.Lock()
		DPrintf("KV[%d] will send TimeOutEvent to C[%d] Seq[%d]", kv.me, args.ClientId, args.Seq)
		reply.Error = ErrTimeOut
	}
	kv.activeClients[args.ClientId].NoticeChMap.Remove(args.Seq, noticeChs)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	/* 幂等操作处理 */
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
	notLeaderEventCh := make(chan *ClientsOpRecord, 1)
	noticeChs := &NoticeCh{applyMsgCh, notLeaderEventCh}

	if client, ok := kv.activeClients[args.ClientId]; ok {
		if client.NoticeChMap == nil {
			client.NoticeChMap = slicemultimap.New()
		}
		client.NoticeChMap.Put(args.Seq, noticeChs)
	} else {
		kv.activeClients[args.ClientId] = &ActiveClient{
			ClientId:    args.ClientId,
			NoticeChMap: slicemultimap.New(),
		}
		kv.activeClients[args.ClientId].NoticeChMap.Put(args.Seq, noticeChs)
	}

	leaseTimeOut := time.NewTimer(kv.rf.VoteTimeOut)
	defer leaseTimeOut.Stop()
	_, oldTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("KV[%d] want start %+v ,but it's not a Leader\n", kv.me, op)
		kv.activeClients[args.ClientId].NoticeChMap.Remove(args.Seq, noticeChs)
		reply.Error = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	/* 等待 raft 处理 */
	select {
	case <-notLeaderEventCh:
		kv.mu.Lock()
		DPrintf("KV[%d] will send NotLeaderEvent to C[%d] Seq[%d]", kv.me, args.ClientId, args.Seq)
		reply.Error = ErrWrongLeader
	case msg := <-applyMsgCh:
		kv.mu.Lock()
		defer DPrintf("KV[%d] will send %s reply %+v to C[%d] Seq[%d]", kv.me, args.Op, reply, args.ClientId, args.Seq)
		if term, isLeader := kv.rf.GetState(); !isLeader || term != oldTerm {
			reply.Error = ErrWrongLeader
		} else {
			reply.Error = (string)(msg.Error)
		}
	case <-leaseTimeOut.C:
		kv.mu.Lock()
		DPrintf("KV[%d] will send TimeOutEvent to C[%d] Seq[%d]", kv.me, args.ClientId, args.Seq)
		reply.Error = ErrTimeOut
	}
	kv.activeClients[args.ClientId].NoticeChMap.Remove(args.Seq, noticeChs)
	kv.mu.Unlock()
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.memTable = make(map[string]string)
	/* 暂且不考虑 kv.ClientsOpRecord 的崩溃恢复，因为日志反正都是会重放的， */
	kv.ClientsOpRecord = make(map[int64]ClientsOpRecord)
	kv.activeClients = make(map[int64]*ActiveClient)
	go func() {
		wait_ch := make(chan interface{})
		kv.rf.RegisterNotLeaderNowCh(wait_ch)
		for !kv.killed() {
			DPrintf("KV[%d] wait for NotLeaderEvent", kv.me)
			<-wait_ch
			kv.mu.Lock()
			for _, client := range kv.activeClients {
				for _, entry := range client.NoticeChMap.Entries() {
					DPrintf("KV[%d] will send NotLeaderEvent to C[%d] Seq[%d]", kv.me, client.ClientId, entry.Key.(int32))
					entry.Value.(*NoticeCh).NotLeaderEventChs <- &ClientsOpRecord{Error: ErrWrongLeader}
					DPrintf("KV[%d] will send NotLeaderEvent to C[%d] Seq[%d] step[0]", kv.me, client.ClientId, entry.Key.(int32))
				}
			}
			kv.mu.Unlock()
		}
	}()
	/* 监听客户端从 applyCh 的提交 */
	go func() {
		for msg := range kv.applyCh {
			DPrintf("KV[%d] applyCh: %+v\n", kv.me, msg)
			if msg.CommandValid {

				cmdOp := msg.Command.(Op)

				switch cmdOp.Op {
				case "Get":
					kv.mu.Lock()
					var record ClientsOpRecord
					ok := false

					/* 寻找已经发送的历史记录中是否存在该操作，有则直接返回记录 */
					if record, ok = kv.ClientsOpRecord[cmdOp.ClientId]; !ok || record.Op.Seq != cmdOp.Seq {
						/* 否则构造记录 */
						record = ClientsOpRecord{
							Op: cmdOp,
						}
						/* resultValue 和 err */
						if value, ok := kv.memTable[cmdOp.Key]; ok {
							record.ResultValue = value
							record.Error = OK
						} else {
							record.Error = ErrNoKey
						}

						kv.ClientsOpRecord[cmdOp.ClientId] = record
					}
					if activeClient, ok := kv.activeClients[cmdOp.ClientId]; ok {
						if entries, found := activeClient.NoticeChMap.Get(cmdOp.Seq); found {
							for _, entry := range entries {
								entry.(*NoticeCh).ApplyMsgChs <- &record
							}
						}
					}
					/* 使用有缓冲区的 channel，
					即便是对面没有人接受也是可以接受的 */
					kv.mu.Unlock()
				case "Put", "Append":
					kv.mu.Lock()
					var record ClientsOpRecord
					ok := false

					/* 寻找已经发送的历史记录中是否存在该操作，有则直接返回记录 */
					if record, ok = kv.ClientsOpRecord[cmdOp.ClientId]; !ok || record.Op.Seq != cmdOp.Seq {
						/* 否则构造记录 */
						if cmdOp.Op == "Append" {
							kv.memTable[cmdOp.Key] += cmdOp.Value
						} else if cmdOp.Op == "Put" {
							kv.memTable[cmdOp.Key] = cmdOp.Value
						}
						record = ClientsOpRecord{
							Op:    cmdOp,
							Error: OK,
						}
						kv.ClientsOpRecord[cmdOp.ClientId] = record
					}
					if activeClient, ok := kv.activeClients[cmdOp.ClientId]; ok {
						if entries, found := activeClient.NoticeChMap.Get(cmdOp.Seq); found {
							for _, entry := range entries {
								entry.(*NoticeCh).ApplyMsgChs <- &record
							}
						}
					}
					kv.mu.Unlock()
				default:
					log.Fatalf("KV[%d] applyCh: unknown command\n", kv.me)
				}
			} else {
				log.Fatalf("KV[%d] applyCh: unknown command\n", kv.me)
			}
		}
	}()
	return kv
}
