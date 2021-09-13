package raft

import (
	"fmt"
	"log"
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
	Term        int    /* 投票人的任期 */
	VoteGranted bool   /* 投票人是否投票 */
	Reason      string /* 拒绝的原因 */
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
		/* 重置超时的方法 */
		case <-rf.resetTimerCh:
			rf.logger.Infof("[%d] ticker: reset the timer", rf.me)
			// /* 如果 当前领导者发 参选者 ---> 跟随者 */
			timeout.Reset(time.Duration(rand.Int63n(300)+300) * time.Millisecond)
			/* 关闭计时器 */
		case <-timeout.C:
			/* 如果计时器超时 ，执行超时回调 */
			rf.logger.Infof("[%d] ticker: timeout ", rf.me)
			timeout.Reset(time.Duration(rand.Int63n(300)+300) * time.Millisecond)
			/* 如果当前节点是领导者 */
			rf.mu.Lock()
			if rf.state != Leader {
				go rf.VoteTimeOutCallBack()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) VoteTimeOutCallBack( /* voteCh <-chan bool */ ) {
	rf.logger.Infof("[%d] VoteTimeOutCallBack: start", rf.me)
	defer rf.logger.Infof("[%d] VoteTimeOutCallBack: stop", rf.me)

	var voteCnt int
	rf.mu.Lock()

	rf.state = Candidater
	rf.currentTerm++          /* 自增当前的任期号 */
	oldState := rf.state      /* Candidater */
	oldTerm := rf.currentTerm /* 记录当前任期号 */

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
			log.Printf("T[%d] S[%d] sendRequestVote to [%d]", rf.currentTerm, rf.me, i)
			rf.mu.Unlock()

			/* =======UNLOCK SPACE========== */
			/* 也许这时候已经选举发生了改变... 但由于没有加锁(也不该加锁)，
			我们毅然决然的发送了 */
			rf.logger.Infof("S[%d] sendRequestVote %v to [%d]", rf.me, args, i)
			if ok := rf.sendRequestVote(i, args, reply); !ok {
				rf.logger.Infof("S[%d] sendRequestVote to [%d]: not ok", rf.me, i)
				return
				// if rf.killed() {
				// 	return
				// }
				// rf.logger.Infof("[%d] sendRequestVote to [%d]: not ok", rf.me, i)
				// continue
			}
			rf.logger.Infof("S[%d] recv VoteRespond from [%d] %v", rf.me, i, reply)
			/* =======UNLOCK SPACE========== */

			rf.mu.Lock()
			/* 选举状态已经发生改变 */
			if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
				rf.logger.Infof("S[%d] VoteTimeOutSendVote: state changed!", rf.me)
				rf.mu.Unlock()
				return
			}
			/* 拿到这张票 */
			if reply.VoteGranted {
				voteCnt++
				rf.logger.Infof("S[%d] get S[%d] vote, now it have %d votes", rf.me, i, voteCnt)
				if voteCnt == len(rf.peers)/2+1 {
					rf.TurnToLeaderWithLock()
				}
			} else if reply.Term > rf.currentTerm {
				/* 没有拿到这张票 */
				/* 任期小 不配当领导者 变回 跟随者 */
				log.Printf("T[%d] S[%d] vote is rejected by S[%d] because: %s", rf.currentTerm, rf.me, i, reply.Reason)

				rf.ResetToFollowerWithLock(fmt.Sprintf("[%d]任期 %d 小于[%d]任期 %d 不配当领导者", rf.me, rf.currentTerm, i, reply.Term))
				rf.currentTerm = reply.Term /* 更新任期 */
			} else {
				/* 其他拒绝原因  */
				log.Printf("T[%d] S[%d] vote is rejected by S[%d] because: %s", rf.currentTerm, rf.me, i, reply.Reason)
			}
			rf.mu.Unlock()
			// return
			// }
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
		// rf.logger.Infof("[%d] term is less than [%d] so cannot vote for [%d]", args.CandidateId, rf.me, args.CandidateId)
		reply.Reason = fmt.Sprintf("S[%d] term T[%d] is less than S[%d] term T[%d]", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		/* 任期新 */
		if ok, err_reason := rf.ArelogNewerWithLock(args); ok {
			log.Printf("T[%d] S[%d] vote for S[%d]", rf.currentTerm, rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.Reason = err_reason
			reply.VoteGranted = false
		}
	} else {
		reply.Reason = fmt.Sprintf("S[%d] vote for S[%d] before, so can not vote for S[%d]", rf.currentTerm, rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
	}

	if args.Term > rf.currentTerm {
		/* 如果选举者的任期比自己的高，更新自己任期 */
		rf.ResetToFollowerWithLock(fmt.Sprintf("[%d]任期 %d 小于[%d]任期%d", rf.me, rf.currentTerm, args.CandidateId, args.Term))
		rf.currentTerm = args.Term
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

/* 参选者的日志是否更加新？ */
func (rf *Raft) ArelogNewerWithLock(args *RequestVoteArgs) (bool, string) {
	/* 先比任期 */
	lastLogTerm := rf.log[len(rf.log)-1].Term
	if args.LastLogTerm < lastLogTerm {
		return false, fmt.Sprintf("S[%d] LastLogTerm T[%d] < S[%d] LastLogTerm T[%d]", args.CandidateId, args.LastLogTerm, rf.me, lastLogTerm)
	}

	/* 任期相同 比最后日志的坐标 */
	lastLogIndex := len(rf.log) - 1
	if args.LastLogTerm == lastLogTerm &&
		args.LastLogIndex < lastLogIndex {
		return false, fmt.Sprintf("S[%d] LastLogIndex T[%d] < S[%d] LastLogIndex T[%d]", args.CandidateId, args.LastLogIndex, rf.me, lastLogIndex)
	}
	return true, ""
}
