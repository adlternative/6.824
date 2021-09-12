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
	// "context"
	// "log"
	"fmt"
	// "log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
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

func (log *RaftLog) String() string {
	val, ok := log.Command.(int)
	if ok {
		return fmt.Sprintf("{%d, %d}", val, log.Term)
	}
	return ""
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
	currentTerm int       // 服务器已知最新的任期
	votedFor    int       /* 当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空 */
	log         []RaftLog /* 日志条目 <command,term> + index */

	/* 易失性状态 */
	commitIndex int //	已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //	已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	/* 领导者（服务器）上的易失性状态 (选举后已经重新初始化) */
	nextIndex  []int /* 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1） */
	matchIndex []int /* 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增） */

	/* 用于在服务器发送 appendEntriesRpc 之后重置选举超时 */
	appendEntriesRpcCh chan bool
	applyCh            chan ApplyMsg // 用于提交日志条目

	sendHeartBeatTimeOut time.Duration // 发送心跳时间
	recvHeartBeatTimeOut time.Duration // 接受心跳时间
	logger               *logger.Logger

	routineCnt int32 // 主动开的 go协程数量统计
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
	/* raft state init */
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.votedFor = -1
	rf.log = append(rf.log, RaftLog{})
	rf.appendEntriesRpcCh = make(chan bool)
	rf.sendHeartBeatTimeOut = 100 * time.Millisecond
	rf.recvHeartBeatTimeOut = time.Duration(rand.Int63n(300)+300) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	/* log for debug */
	lf, err := os.OpenFile(strconv.Itoa(rf.me)+".log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		logger.Fatalf("Failed to open log file: %v", err)
	}
	rf.logger = logger.Init("raftlog", false, true, lf)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.RoutineCntDebug()
	return rf
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
	isLeader := false
	if !rf.killed() {
		rf.mu.Lock()
		/* 向当前服务器添加日志项 */
		term = rf.currentTerm
		isLeader = rf.state == Leader
		if isLeader {
			rf.log = append(rf.log, RaftLog{command, term})
			rf.logger.Info(rf.log)
			index = len(rf.log) - 1
			// log.Printf("[%d] start command %v in index(%d)", rf.me, command, index)
			rf.matchIndex[rf.me] = index
		}
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

func (rf *Raft) RoutineCntDebug() {
	for {
		time.Sleep(time.Second)
		rf.logger.Infof("[%d] go rountine count: %d, total: %d", rf.me, atomic.LoadInt32(&rf.routineCnt), runtime.NumGoroutine())
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
