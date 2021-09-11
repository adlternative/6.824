package raft

import (
	_ "log"
	"math/rand"
	"sync/atomic"
	"time"
)

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) ticker() {
	atomic.AddInt32(&rf.routineCnt, 1)
	defer atomic.AddInt32(&rf.routineCnt, -1)

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
			rf.logger.Infof("[%d] ticker: timeout ", rf.me)
			timeout.Reset(time.Duration(rand.Int63n(300)+300) *
				time.Millisecond)
			go rf.VoteTimeOutCallBack()
		}
	}
}

func (rf *Raft) VoteTimeOutCallBack( /* voteCh <-chan bool */ ) {
	rf.logger.Infof("[%d] VoteTimeOutCallBack: start", rf.me)
	defer rf.logger.Infof("[%d] VoteTimeOutCallBack: stop", rf.me)

	var voteCnt int
	rf.mu.Lock()
	if rf.state == Leader {
		rf.logger.Infof("[%d] VoteTimeOutCallBack: It seems like [%d] is not a follower or candidater but a leader", rf.me, rf.me)
		rf.mu.Unlock()
		return
	}

	rf.state = Candidater
	rf.currentTerm++ /* 自增当前的任期号 */
	// rf.logger.Infof("[%d] currentTerm: %d", rf.me, rf.currentTerm)
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
			atomic.AddInt32(&rf.routineCnt, 1)
			defer atomic.AddInt32(&rf.routineCnt, -1)

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
			for ok := rf.sendRequestVote(i, args, reply); !ok; ok = rf.sendRequestVote(i, args, reply) {
				if rf.killed() {
					return
				}
				rf.logger.Infof("[%d] sendRequestVote to [%d]: not ok", rf.me, i)
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
				if voteCnt == len(rf.peers)/2+1 {
					rf.TurnToLeaderWithLock()
				}
			} else if reply.Term > rf.currentTerm {
				/* 任期小 不配当领导者 */
				rf.ResetToFollowerWithLock() /* 变回 跟随者 */
				rf.currentTerm = reply.Term  /* 更新任期 */
			}
			rf.mu.Unlock()
		}(i)
	}
}

//
// example RequestVote RPC handler.
//
/* 接收的服务器上处理 RequestVoteRPC  */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	atomic.AddInt32(&rf.routineCnt, 1)
	defer atomic.AddInt32(&rf.routineCnt, -1)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	/* 设置返回任期 */
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		/* 如果选举者的任期比自己的低 */
		rf.logger.Infof("[%d] term is less than [%d] so cannot vote for [%d]", args.CandidateId, rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm {
		/* 如果选举者的任期和自己相同 我已经投过票*/
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			rf.logger.Infof("[%d] have vote for [%d] so cannot vote for [%d]", rf.me, rf.votedFor, args.CandidateId)
			reply.VoteGranted = false
			return
		}
	} else /*  if args.Term > rf.currentTerm */ {
		/* 如果选举者的任期比自己的高，更新自己任期 */
		rf.ResetToFollowerWithLock() /* 变回 跟随者 */
		rf.currentTerm = args.Term
		/* 先别投票 （得对方的 日志是否够新）*/
	}

	/* 候选人的日志没有和自己一样新	 */
	if args.LastLogIndex < len(rf.log)-1 ||
		args.LastLogTerm < rf.log[len(rf.log)-1].Term {
		rf.logger.Infof("[%d] log is old than [%d] so cannot vote for [%d]", args.CandidateId, rf.me, args.CandidateId)
		// rf.logger.Infof("[%d] args.LastLogIndex =%d, args.LastLogTerm =%d, len(rf.log) =%d  rf.log[len(rf.log)-1].Term = %d",
		// 	rf.me, args.LastLogIndex, args.LastLogTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
		reply.VoteGranted = false
		return
	}

	/* 否则将票投给他 */
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
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
