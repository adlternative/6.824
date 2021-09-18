package raft

import (
	"context"
	"fmt"
	"log"

	// "runtime"
	"sync/atomic"
	"time"
)

type AppendEntriesArgs struct {
	Term         int       /* 领导者人的任期 */
	LeaderId     int       /* 领导者人的 ID */
	PrevLogIndex int       /* 领导者人发送的新日志的前一条日志索引 */
	PrevLogTerm  int       /* 领导者人发送的新日志的前一条日志任期 */
	Entries      []RaftLog /* 领导者人发送的新日志 */
	LeaderCommit int       /* 领导者人的最后一条日志的索引 */
}

type AppendEntriesReply struct {
	Term          int  /* 投票人的任期 */
	Success       bool /*  如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了 结果为真*/
	MayMatchIndex int  /* 跟随者的最后日志索引 */

	// UnMatchPrevLogTerm int  /* 不匹配的 prevlog 记录的任期 */
}

/* leader 才可以定期发送心跳包 */
func (rf *Raft) HeartBeatTicker() {
	atomic.AddInt32(&rf.routineCnt, 1)
	defer atomic.AddInt32(&rf.routineCnt, -1)
	// if pc, _, _, ok := runtime.Caller(0); ok {
	// 	rf.Debug("[%d] %s: begin", rf.me, Trace(pc))
	// 	defer rf.Debug("[%d] %s: end", rf.me, Trace(pc))
	// }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	/* 立即发送一波 */
	rf.HeartBeatTimeOutCallBack(ctx, cancel)

	HeartBeatTimeOut := time.NewTimer(rf.sendHeartBeatTimeOut) /* 100 */
	defer HeartBeatTimeOut.Stop()

	for !rf.killed() {
		/* 如果不是领导者则应该退出 */
		select {
		/* 超时 */
		case <-HeartBeatTimeOut.C:
			/* 发送心跳 */
			HeartBeatTimeOut.Reset(rf.sendHeartBeatTimeOut)
			go rf.HeartBeatTimeOutCallBack(ctx, cancel)
		case <-ctx.Done():
			/* 领导者状态发生了改变 */
			return
		}
	}
}

/* 领导者服务器心跳超时的回调函数 */
func (rf *Raft) HeartBeatTimeOutCallBack(ctx context.Context, cancel context.CancelFunc) {
	var heartBeatAckCnt int
	// if pc, _, _, ok := runtime.Caller(0); ok {
	// 	rf.logger.Infof("[%d] %s: begin", rf.me, Trace(pc))
	// 	defer rf.logger.Infof("[%d] %s: end", rf.me, Trace(pc))
	// }

	changed, oldTerm, oldState := rf.AreStateOrTermChange(-1, Leader)
	if changed {
		// rf.Debug("HeartBeatTimeOutCallBack: state or term change")
		cancel()
		return
	}

	/* 发送 AppendEntries request rpc */

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			atomic.AddInt32(&rf.routineCnt, 1)
			defer atomic.AddInt32(&rf.routineCnt, -1)

			// if pc, _, _, ok := runtime.Caller(0); ok {
			// 	rf.logger.Infof("[%d] %s: begin", rf.me, Trace(pc))
			// 	defer rf.logger.Infof("[%d] %s: end", rf.me, Trace(pc))
			// }

			for !rf.killed() {
				rf.mu.Lock()

				/* 领导者状态已经发生改变 */
				if changed, _, _ = rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
					// rf.Debug("HeartBeatTimeOutSendAppendEntriesRPC: state or term change")
					cancel()
					rf.mu.Unlock()
					return
				}

				reply := &AppendEntriesReply{}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				}

				args.Entries = make([]RaftLog, len(rf.log[rf.nextIndex[i]:]))
				copy(args.Entries, rf.log[rf.nextIndex[i]:])
				rf.DebugWithLock("want to send logs:%v to S[%d]", args.Entries, i)

				rf.mu.Unlock()

				/* ==========UNLOCK SPACE========= */
				/* [TODO] 出错多少次退出？ */
				if ok := rf.sendAppendEntries(i, args, reply); !ok {
					// rf.Debug("sendAppendEntries to [%d]: not ok", rf.me, i)
					return
				}
				/* =================== */

				rf.mu.Lock()
				/* 领导者状态已经发生改变 */
				if changed, _, _ = rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
					rf.DebugWithLock("HeartBeatTimeOutSendAppendEntriesRPC: state or term change")
					cancel()
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					heartBeatAckCnt++
					rf.DebugWithLock(" get heartBeatAck from [%d], now it get %d Ack", i, heartBeatAckCnt)

					/* 更新 matchIndex AND nextIndex */
					rf.matchIndex[i] = reply.MayMatchIndex
					rf.nextIndex[i] = rf.matchIndex[i] + 1

					if rf.nextIndex[i] > len(rf.log) { //debug
						rf.mu.Unlock()
						log.Fatalf("[BUG] rf.nextIndex[%d]=%d >len(rf.log)=%d",
							i, rf.nextIndex[i], len(rf.log))
					}

					if rf.log[rf.matchIndex[i]].Term == rf.currentTerm {
						matchCnt := 0
						for j := 0; j < len(rf.matchIndex); j++ {
							if rf.matchIndex[j] == rf.matchIndex[i] {
								matchCnt++
							}
							/* 表示我们的日志已经保存在多个服务器上了 则可以提交了*/
							if matchCnt == len(rf.matchIndex)/2+1 &&
								rf.matchIndex[i] > rf.commitIndex {
								// updateCommitIndex()
								rf.commitIndex = rf.matchIndex[i]
								if rf.commitIndex > rf.lastApplied {
									go rf.ApplyCommittedMsgs()
								}
								break
							}
						}
					}

					rf.mu.Unlock()
					return
				} else if reply.Term > rf.currentTerm {
					/* 任期小 不配当领导者 */
					/* 变回 跟随者 */
					rf.ResetToFollowerWithLock(fmt.Sprintf("小于[%d]任期 %d 不配当领导者", i, reply.Term))
					rf.votedFor = -1
					rf.currentTerm = reply.Term /* 更新任期 */
					rf.persist()
					// rf.logger.Infof("[%d] term = %d", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				} else {
					/* 降低 nextIndex 并重试 */
					rf.nextIndex[i] = reply.MayMatchIndex + 1
					// reply.UnMatchPrevLogTerm
				}
				rf.mu.Unlock()
				/* continue */
			}
		}(i)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	atomic.AddInt32(&rf.routineCnt, 1)
	defer atomic.AddInt32(&rf.routineCnt, -1)

	// if pc, _, _, ok := runtime.Caller(0); ok {
	// 	rf.logger.Infof("[%d] %s: begin", rf.me, Trace(pc))
	// 	defer rf.logger.Infof("[%d] %s: end", rf.me, Trace(pc))
	// }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	/* 设置返回值为 follower 的任期 */
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		/* 1.  如果领导者的任期 小于 接收者的当前任期 返回假 */
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		/* 如果领导者的任期比自己的高，更新自己任期 */
		rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] AE", args.Term, args.LeaderId))
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	} else {
		/* ASSERT(rf.currentTerm == args.Term) */
		if rf.state == Candidater {
			/* 选举人收到了新 leader 的 appendEntriesRpc */
			rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] AE", args.Term, args.LeaderId))
			rf.votedFor = -1
			rf.persist()
			/* 变回 跟随者 */
		}
	}

	/* ASSERT( rf.state == FOLLOWER) */

	/*  即该条目的任期在prevLogIndex上能和prevLogTerm匹配上*/
	/* PrevLogIndex > 最后一条日志的坐标 */
	if args.PrevLogIndex > len(rf.log)-1 ||
		args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		rf.DebugWithLock("reject logs from S[%d] because S[%d] PrevLog doesn't match", args.LeaderId, args.LeaderId)
		reply.Success = false
		if args.PrevLogIndex > len(rf.log)-1 {
			reply.MayMatchIndex = len(rf.log) - 1
		} else {
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				/* 找到恰好不是该 term 的位置 */
				if rf.log[args.PrevLogIndex].Term != rf.log[i].Term {
					reply.MayMatchIndex = i
				}
			}
		}

	} else {
		/* 发过来的坐标是 [PrevLogIndex + 1, PrevLogIndex + len(arg.Entries) ] */
		/* rf.log[PrevLogIndex + 1:] 都是冲突项 */
		/* 去除冲突项 */
		rf.log = rf.log[:args.PrevLogIndex+1]

		if args.Entries == nil {
			rf.DebugWithLock("get a heartbeat from S[%d]", args.LeaderId)
		} else {
			rf.DebugWithLock("get log entries from S[%d]", args.LeaderId)
			/* 后添新项 */
			rf.log = append(rf.log, args.Entries...)
			rf.DebugWithLock("append logs:%v from S[%d]", args.Entries, args.LeaderId)
		}

		/* 更新 commitIndex  */
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		/* 还应该 log apply to state machine */
		if rf.commitIndex > rf.lastApplied {
			go rf.ApplyCommittedMsgs()
		}
		rf.resetTimerCh <- true /* 重置等待选举的超时定时器 */
		reply.Success = true
		reply.MayMatchIndex = len(rf.log) - 1
		rf.persist()
	}
}

/* 更新 commitIndex */
func (rf *Raft) ApplyCommittedMsgs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DebugWithLock("apply log entries log[%d:%d]:%v to app",
		rf.lastApplied+1, rf.commitIndex+1, rf.log[rf.lastApplied+1:rf.commitIndex+1])

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}
	rf.lastApplied = rf.commitIndex
}
