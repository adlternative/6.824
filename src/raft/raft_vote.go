package raft

import (
	"fmt"
	"math/rand"

	// "runtime"
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

	// if pc, _, _, ok := runtime.Caller(0); ok {
	// 	rf.logger.Infof("[%d] %s: begin", rf.me, Trace(pc))
	// 	defer rf.logger.Infof("[%d] %s: end", rf.me, Trace(pc))
	// }

	timeout := time.NewTimer(rf.VoteTimeOut)
	defer timeout.Stop()

	for !rf.killed() {
		select {
		/* 重置超时的方法 */
		case <-rf.resetTimerCh:
			// rf.Debug("ticker: reset the timer")
			// /* 如果 当前领导者发 参选者 ---> 跟随者 */
			timeout.Reset(time.Duration(rand.Int63n(500)+500) * time.Millisecond)
			/* 关闭计时器 */
		case <-timeout.C:
			/* 如果计时器超时 ，执行超时回调 */
			// rf.Debug("ticker: timeout ")
			timeout.Reset(time.Duration(rand.Int63n(500)+500) * time.Millisecond)
			/* 如果当前节点是领导者 */
			go rf.VoteTimeOutCallBack()
		}
	}
}

func (rf *Raft) VoteTimeOutCallBack( /* voteCh <-chan bool */ ) {
	// if pc, _, _, ok := runtime.Caller(0); ok {
	// 	rf.logger.Infof("[%d] %s: begin", rf.me, Trace(pc))
	// 	defer rf.logger.Infof("[%d] %s: end", rf.me, Trace(pc))
	// }

	var voteCnt int
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate
	rf.CurrentTerm++    /* 自增当前的任期号 */
	rf.VotedFor = rf.me /* 投票给自己 */
	rf.persist()

	oldState := rf.state      /* Candidate */
	oldTerm := rf.CurrentTerm /* 记录当前任期号 */
	voteCnt++

	rf.DebugWithLock("now sending vote (term update to %d)", rf.CurrentTerm)
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
			// if pc, _, _, ok := runtime.Caller(0); ok {
			// 	rf.logger.Infof("[%d] %s: begin", rf.me, Trace(pc))
			// 	defer rf.logger.Infof("[%d] %s: end", rf.me, Trace(pc))
			// }
			/* 选举状态已经发生改变 */
			if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
				rf.DebugWithLock("VoteTimeOutSendVote: state changed!")
				rf.mu.Unlock()
				return
			}
			logLen := rf.Log.Len()
			reply := &RequestVoteReply{}
			args := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: logLen - 1,
				LastLogTerm:  rf.Log.TermOf((logLen - 1)),
			}
			rf.DebugWithLock("sendRequestVote to S[%d]", i)
			rf.mu.Unlock()

			/* =======UNLOCK SPACE========== */
			/* 也许这时候已经选举发生了改变... 但由于没有加锁(也不该加锁)，
			我们毅然决然的发送了 */
			// rf.Debug("sendRequestVote %v to [%d]", args, i)
			if ok := rf.sendRequestVote(i, args, reply); !ok {
				// rf.Debug("sendRequestVote %v to [%d]", args, i)
				// rf.Debug("sendRequestVote to [%d]: not ok", i)
				return
			}
			// rf.Debug("recv VoteRespond from [%d] %v", i, reply)
			/* =======UNLOCK SPACE========== */

			rf.mu.Lock()
			/* 选举状态已经发生改变 */
			if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
				rf.DebugWithLock("S[%d] VoteTimeOutSendVote: state changed!", rf.me)
				rf.mu.Unlock()
				return
			}
			/* 拿到这张票 */
			if reply.VoteGranted {
				voteCnt++
				rf.DebugWithLock("get S[%d] vote, now it have %d votes", i, voteCnt)
				if voteCnt == len(rf.peers)/2+1 {
					rf.TurnToLeaderWithLock()
				}
			} else if reply.Term > rf.CurrentTerm {
				/* 没有拿到这张票 */
				/* 任期小 不配当领导者 变回 跟随者 */
				rf.DebugWithLock("vote is rejected by S[%d] because: %s", i, reply.Reason)
				rf.ResetToFollowerWithLock(fmt.Sprintf("[%d]任期 %d 小于[%d]任期 %d 不配当领导者", rf.me, rf.CurrentTerm, i, reply.Term))
				rf.VotedFor = -1
				rf.CurrentTerm = reply.Term /* 更新任期 */
				rf.persist()
			} else {
				/* 其他拒绝原因  */
				rf.DebugWithLock("vote is rejected by S[%d] because: %s", i, reply.Reason)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/* 设置返回任期 */
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		/* 如果选举者的任期比自己的低 */
		// rf.logger.Infof("[%d] term is less than [%d] so cannot vote to [%d]", args.CandidateId, rf.me, args.CandidateId)
		reply.Reason = fmt.Sprintf("S[%d] term T[%d] is less than S[%d] term T[%d]", args.CandidateId, args.Term, rf.me, rf.CurrentTerm)
		reply.VoteGranted = false
	} else {
		/* 如果任期更大且更新就投 */
		if args.Term > rf.CurrentTerm {
			/* 如果选举者的任期比自己的高，更新自己任期 */
			rf.ResetToFollowerWithLock(fmt.Sprintf("S[%d]任期 T[%d] 小于 S[%d] 任期 T[%d]", rf.me, rf.CurrentTerm, args.CandidateId, args.Term))
			rf.CurrentTerm = args.Term

			/* 日志更新 */
			if ok, err_reason := rf.ArelogNewerWithLock(args); ok {
				/* 日志新 */
				rf.DebugWithLock("vote to S[%d]", args.CandidateId)
				rf.VotedFor = args.CandidateId
				rf.resetTimer() /* 重置等待选举的超时定时器 */
				reply.VoteGranted = true
				rf.persist()
			} else {
				rf.DebugWithLock("don't vote to S[%d]", args.CandidateId)
				reply.Reason = err_reason
				rf.VotedFor = -1
				reply.VoteGranted = false
				rf.persist()
			}
		} else {
			/*
				assert args.Term == rf.CurrentTerm
			*/
			/* 如果任期相同且没有投给别人且更新就投 */
			if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
				/* 没有投票给其他人 */
				if ok, err_reason := rf.ArelogNewerWithLock(args); ok {
					/* 日志新 */
					rf.DebugWithLock("vote to S[%d]", args.CandidateId)
					rf.ResetToFollowerWithLock("任期相同且没有投给别人且更新就投")
					rf.VotedFor = args.CandidateId
					rf.resetTimer() /* 重置等待选举的超时定时器 */
					reply.VoteGranted = true
					rf.persist()
				} else {
					rf.DebugWithLock("don't vote to S[%d]", args.CandidateId)
					reply.Reason = err_reason
					rf.VotedFor = -1
					reply.VoteGranted = false
					rf.persist()
				}
			} else {
				rf.DebugWithLock("don't vote to S[%d]", args.CandidateId)
				reply.Reason = fmt.Sprintf("S[%d] vote to S[%d] before, so can not vote to S[%d]", rf.me, rf.VotedFor, args.CandidateId)
				reply.VoteGranted = false
			}
		}
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
	/* assert(rf.log.Len()>0) */
	lastLogTerm := rf.Log.TermOf((rf.Log.Len() - 1))
	if args.LastLogTerm < lastLogTerm {
		return false, fmt.Sprintf("S[%d] LastLogTerm T[%d] < S[%d] LastLogTerm T[%d]",
			args.CandidateId, args.LastLogTerm, rf.me, lastLogTerm)
	}

	/* 任期相同 比最后日志的坐标 */
	lastLogIndex := rf.Log.Len() - 1
	if args.LastLogTerm == lastLogTerm &&
		args.LastLogIndex < lastLogIndex {
		return false, fmt.Sprintf("S[%d] LastLogIndex T[%d] < S[%d] LastLogIndex T[%d]",
			args.CandidateId, args.LastLogIndex, rf.me, lastLogIndex)
	}
	return true, ""
}
