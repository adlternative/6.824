package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"context"
	// "log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	"github.com/google/logger"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftLog struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state State // 不同的服务器状态

	/* 持久性状态 */
	currentTerm int        // 服务器已知最新的任期
	votedFor    int        /* 当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空 */
	log         []*RaftLog /* 日志条目 <command,term> + index */

	// /* 易失性状态 */
	// commitIndex int //	已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	// lastApplied int //	已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	// /* 领导者（服务器）上的易失性状态 (选举后已经重新初始化) */
	// nextIndex  []int /* 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1） */
	// matchIndex []int /* 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增） */

	/* 用于在服务器发送 appendEntriesRpc 之后重置选举超时 */
	appendEntriesRpcCh chan bool

	sendHeartBeatTimeOut time.Duration // 发送心跳时间
	recvHeartBeatTimeOut time.Duration // 接受心跳时间
	logger               *logger.Logger
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int /* 选举人的任期 */
	CandidateId  int /* 选举人的 ID */
	LastLogIndex int /* 选举人最后一条日志的 索引 */
	LastLogTerm  int /* 选举人最后一条日志的 任期 */
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  /* 投票人的任期 */
	VoteGranted bool /* 投票人是否投票 */
}

//
// example RequestVote RPC handler.
//
/* 接收的服务器上处理 RequestVoteRPC  */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/* 设置返回任期 */
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		/* 如果选举者的任期比自己的低 */
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		/* 如果选举者的任期比自己的高，更新自己任期 */
		rf.resetToFollowerWithLock() /* 变回 跟随者 */
		rf.currentTerm = args.Term

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		/* 如果选举者的任期和自己相同 我已经投过票*/
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			rf.logger.Infof("[%d] have vote for [%d] so cannot vote for [%d]", rf.me, rf.votedFor, args.CandidateId)
			reply.VoteGranted = false
			return
		}
		/* 否则将票投给他 */
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int       /* 领导者人的任期 */
	LeaderId     int       /* 领导者人的 ID */
	PrevLogIndex int       /* 领导者人发送的新日志的前一条日志索引 */
	PrevLogTerm  int       /* 领导者人发送的新日志的前一条日志任期 */
	Entries      []RaftLog /* 领导者人发送的新日志 */
	LeaderCommit int       /* 领导者人的最后一条日志的索引 */
}

type AppendEntriesReply struct {
	Term    int  /* 投票人的任期 */
	Success bool /*  如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了 结果为真*/
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/* 设置返回任期 */
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		/* 如果领导者的任期比自己的低 ，返回错误*/
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		/* 如果领导者的任期比自己的高，更新自己任期 */
		rf.resetToFollowerWithLock() /* 变回 跟随者 */
		rf.currentTerm = args.Term
	} else {
		if rf.state == Candidater {
			/* 选举人收到了新 leader 的 appendEntriesRpc */
			rf.resetToFollowerWithLock() /* 变回 跟随者 */
		}
	}
	// if args.PrevLogIndex > rf.log.LastIndex() {
	// 	reply.Success = false
	// } else if args.PrevLogIndex == rf.log.LastIndex() {
	// 	if args.PrevLogTerm != rf.log.LastTerm() {
	// 		reply.Success = false
	// 	}
	// } else {
	// 	if args.PrevLogTerm != rf.log.Get(args.PrevLogIndex).Term {
	// 		reply.Success = false
	// 	}
	// }
	// if reply.Success {
	// 	rf.log.Append(args.Entries)
	// 	rf.commitIndex = Min(args.LeaderCommit, rf.log.LastIndex())
	// 	rf.lastApplied = rf.commitIndex
	// }
	rf.appendEntriesRpcCh <- true /* 重置等待选举的超时定时器 */
	reply.Success = true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) ticker() {
	rf.logger.Infof("[%d] ticker: start", rf.me)
	defer rf.logger.Infof("[%d] ticker: stop", rf.me)

	timeout := time.NewTimer(rf.recvHeartBeatTimeOut)
	defer timeout.Stop()

	for !rf.killed() {
		select {
		/* 如果在超时之前收到了当前领导人的请求 */
		case <-rf.appendEntriesRpcCh:
			rf.logger.Infof("[%d] ticker: get appendEntriesRpc, so reset the timer", rf.me)
			// /* 如果 当前领导者发 参选者 ---> 跟随者 */
			timeout.Reset(time.Duration(rand.Int63n(300)+300) *
				time.Millisecond)
			/* 关闭计时器 */
		case <-timeout.C:
			/* 如果计时器超时 ，执行超时回调 */
			rf.logger.Infof("[%d] ticker: timeout  current goroutine num :%d", rf.me, runtime.NumGoroutine())
			timeout.Reset(time.Duration(rand.Int63n(300)+300) *
				time.Millisecond)
			rf.VoteTimeOutCallBack()
		}
	}
}

func (rf *Raft) VoteTimeOutCallBack( /* voteCh <-chan bool */ ) {
	rf.logger.Infof("[%d] VoteTimeOutCallBack: start", rf.me)
	defer rf.logger.Infof("[%d] VoteTimeOutCallBack: stop", rf.me)

	var voteCnt int
	rf.mu.Lock()
	if rf.state == Leader {
		rf.logger.Infof("[%d] VoteTimeOutCallBack: It seems like me's not a follower or candidate but a Leader", rf.me)
		rf.mu.Unlock()
		return
	}

	rf.state = Candidater
	rf.currentTerm++          /* 自增当前的任期号 */
	oldTerm := rf.currentTerm /* 记录当前任期号 */
	oldState := rf.state

	rf.votedFor = rf.me /* 投票给自己 */
	voteCnt++
	rf.mu.Unlock()

	/* 发送 vote request rpc */
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			rf.logger.Infof("[%d] VoteTimeOutSendVote: start", rf.me)
			defer rf.logger.Infof("[%d] VoteTimeOutSendVote: stop", rf.me)

			/* 选举状态已经发生改变 */
			if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
				rf.logger.Infof("[%d] VoteTimeOutSendVote: state changed!", rf.me)
				rf.mu.Unlock()
				return
			}
			logLen := len(rf.log)
			reply := &RequestVoteReply{}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: logLen - 1,
				LastLogTerm:  rf.log[logLen-1].Term,
			}
			rf.mu.Unlock()
			/* 也许这时候已经选举发生了改变... 但由于没有加锁(也不该加锁)，
			我们毅然决然的发送了 */
			rf.logger.Infof("[%d] sendRequestVote %v to [%d]", rf.me, args, i)
			if ok := rf.sendRequestVote(i, args, reply); !ok {
				rf.logger.Infof("[%d] sendRequestVote to [%d]: not ok", rf.me, i)
				return
			}
			rf.logger.Infof("[%d] recv VoteRespond from [%d] %v", rf.me, i, reply)

			rf.mu.Lock()
			/* 选举状态已经发生改变 */
			if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
				rf.logger.Infof("[%d] VoteTimeOutSendVote: state changed!", rf.me)
				rf.mu.Unlock()
				return
			}
			/* 拿到这张票 */
			if reply.VoteGranted {
				voteCnt++
				rf.logger.Infof("[%d] get [%d] vote, now it have %d votes", rf.me, i, voteCnt)
				if voteCnt >= len(rf.peers)/2+1 {
					rf.state = Leader
					// update nextIndex[]
					// update matchIndex[]
					/* 变成 leader 后定期发送心跳包 */
					rf.logger.Infof("[%d] to be a leader!", rf.me)
					go rf.HeartBeatTicker()
				}
			} else if reply.Term > rf.currentTerm {
				/* 任期小 不配当领导者 */
				rf.resetToFollowerWithLock() /* 变回 跟随者 */
				rf.currentTerm = reply.Term  /* 更新任期 */
			}
			rf.mu.Unlock()
		}(i)
	}
}

/* leader 才可以定期发送心跳包 */
func (rf *Raft) HeartBeatTicker() {
	rf.logger.Infof("[%d] HeartBeatTicker: begin", rf.me)
	defer rf.logger.Infof("[%d] HeartBeatTicker: end", rf.me)

	HeartBeatTimeOut := time.NewTimer(rf.sendHeartBeatTimeOut) /* 100 */
	defer HeartBeatTimeOut.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for !rf.killed() {
		/* 如果不是领导者则应该退出 */
		select {
		/* 超时 */
		case <-HeartBeatTimeOut.C:
			HeartBeatTimeOut.Reset(rf.sendHeartBeatTimeOut)
			/* 发送心跳 */
			rf.HeartBeatTimeOutCallBack(ctx, cancel)
		case <-ctx.Done():
			/* 领导者状态发生了改变 */
			return
		}
	}
}

/* 领导者服务器心跳超时的回调函数 */
func (rf *Raft) HeartBeatTimeOutCallBack(ctx context.Context, cancel context.CancelFunc) {
	var heartBeatAckCnt int
	rf.logger.Infof("[%d] HeartBeatTimeOutCallBack: begin", rf.me)
	defer rf.logger.Infof("[%d] HeartBeatTimeOutCallBack: end", rf.me)

	changed, oldTerm, oldState := rf.AreStateOrTermChange(-1, Leader)
	if changed {
		rf.logger.Infof("[%d] HeartBeatTimeOutCallBack: state or term change", rf.me)
		cancel()
		return
	}

	/* 发送 AppendEntries request rpc */

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.logger.Infof("[%d] HeartBeatTimeOutSendAppendEntriesRPC: begin", rf.me)
			defer rf.logger.Infof("[%d] HeartBeatTimeOutSendAppendEntriesRPC: end", rf.me)

			rf.mu.Lock()

			/* 领导者状态已经发生改变 */
			if changed, _, _ = rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
				rf.logger.Infof("[%d] HeartBeatTimeOutSendAppendEntriesRPC: state or term change", rf.me)
				cancel()
				rf.mu.Unlock()
				return
			}

			reply := &AppendEntriesReply{}
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// PrevLogIndex: ,
				// PrevLogTerm: ,
				// Entries: ,
				// LeaderCommit: ,
			}
			rf.mu.Unlock()

			if ok := rf.sendAppendEntries(i, args, reply); !ok {
				rf.logger.Infof("[%d] sendAppendEntries: not ok", rf.me)
				return
			}

			rf.mu.Lock()
			/* 领导者状态已经发生改变 */
			if changed, _, _ = rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
				rf.logger.Infof("[%d] HeartBeatTimeOutSendAppendEntriesRPC: state or term change", rf.me)
				cancel()
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				heartBeatAckCnt++
				rf.logger.Infof("[%d] get heartBeatAck from [%d], now it get %d Ack", rf.me, i, heartBeatAckCnt)
			} else if reply.Term > rf.currentTerm {
				/* 任期小 不配当领导者 */
				rf.resetToFollowerWithLock() /* 变回 跟随者 */
				rf.currentTerm = reply.Term  /* 更新任期 */
				rf.logger.Infof("[%d] LEADER --> FOLLOWER term = %d", rf.me, rf.currentTerm)
			}
			rf.mu.Unlock()
		}(i)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.resetToFollower() /* 变跟随者 (设置 voteFor = -1) */
	rf.log = append(rf.log, &RaftLog{})
	rf.appendEntriesRpcCh = make(chan bool)
	rf.sendHeartBeatTimeOut = 100 * time.Millisecond
	rf.recvHeartBeatTimeOut = time.Duration(rand.Int63n(300)+300) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	lf, err := os.OpenFile(strconv.Itoa(rf.me)+".log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		logger.Fatalf("Failed to open log file: %v", err)
	}
	rf.logger = logger.Init("raftlog", false, true, lf)

	go rf.ticker()

	return rf
}
