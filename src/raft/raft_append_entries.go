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
	Term          int  /* 跟随者的任期 */
	Success       bool /* 跟随者成功添加日志项*/
	MayMatchIndex int  /* 跟随者的最后日志索引 */
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

				logicPrevLogIndex := rf.nextIndex[i] - 1

				/* 如果我们需要 appendEntries 却发现 nextIndex[i]-1
				 * (用来作为 prevlogIndex 竟然比日志中保存的最后的index 还要小
				 * 那么这个 prevlogIndex 我们是无法再拿到了，我们应当直接发送快照) */
				if logicPrevLogIndex < rf.log.LastIncludedIndex {
					reply := &InstallSnapshotReply{}
					args := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.log.LastIncludedIndex,
						LastIncludedTerm:  rf.log.LastIncludedTerm,
						Data:              rf.persister.ReadSnapshot(),
					}

					rf.mu.Unlock()
					/* ==========UNLOCK SPACE========= */
					if ok := rf.sendInstallSnapshot(i, args, reply); !ok {
						// rf.Debug("sendInstallSnapshot to [%d]: not ok", i)
						return
					}
					/* =================== */
					if rf.HandleInstallSnapshot(i, oldTerm, oldState, &heartBeatAckCnt, ctx, cancel, reply) {
						return
					}
				} else {
					reply := &AppendEntriesReply{}
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
					}
					// if logicPrevLogIndex == rf.log.LastIncludedIndex {
					// 	args.PrevLogIndex = rf.log.LastIncludedIndex
					// 	args.PrevLogTerm = rf.log.LastIncludedTerm
					// } else {
					args.PrevLogIndex = logicPrevLogIndex
					args.PrevLogTerm = rf.log.TermOf(logicPrevLogIndex)
					// }

					args.Entries = make([]RaftLog, len(rf.log.Entries[rf.log.getEntryIndex(rf.nextIndex[i]):]))
					copy(args.Entries, rf.log.Entries[rf.log.getEntryIndex(rf.nextIndex[i]):])
					rf.DebugWithLock("want to send logs(%d,%d):%v to S[%d]",
						rf.log.getEntryIndex(rf.nextIndex[i]),
						rf.log.getEntryIndex(rf.nextIndex[i])+len(args.Entries),
						args.Entries, i)

					rf.mu.Unlock()
					/* ==========UNLOCK SPACE========= */
					/* [TODO] 出错多少次退出？ */
					if ok := rf.sendAppendEntries(i, args, reply); !ok {
						// rf.Debug("sendAppendEntries to [%d]: not ok", rf.me, i)
						return
					}
					/* =================== */
					if rf.HandleApplyEntries(i, oldTerm, oldState, &heartBeatAckCnt, ctx, cancel, reply) {
						// rf.DebugWithLock("HandleApplyEntries over")
						return
					}
				}
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
		if rf.state == Candidate {
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
	if args.PrevLogIndex > rf.log.Len()-1 {
		/*  1> prevlogindex 竟然超过了 日志总长度 -1
		那么说明我们的leader 太超前， matchindex 设置为  len -1 */
		reply.Success = false
		reply.MayMatchIndex = rf.log.Len() - 1
		rf.DebugWithLock("[reject AE]: args.PrevLogIndex=%d > rf.log.Len()-1=%d, so leader to need to turn down prelogindex", args.PrevLogIndex, rf.log.Len()-1)
	} else if args.PrevLogIndex < rf.log.LastIncludedIndex {
		/* 2> prevlogindex 竟然小于快照最后的坐标
		 * 那么说明我们的 leader 太落后？直接原坐标返回
		 * (leader 说不定需要重新发送快照)
		 * (unlikely)
		 */
		reply.Success = false
		reply.MayMatchIndex = args.PrevLogIndex
		rf.DebugWithLock("[reject AE]: args.PrevLogIndex=%d < rf.log.LastIncludedIndex=%d, so leader may need to send snapshot again", args.PrevLogIndex, rf.log.LastIncludedIndex)
	} else if args.PrevLogIndex == rf.log.LastIncludedIndex &&
		args.PrevLogTerm != rf.log.LastIncludedTerm {
		/* 3> 快照不匹配
		 * (leader 说不定需要重新发送快照)
		 * (unlikely)
		 */
		reply.Success = false
		if args.PrevLogIndex == 0 {
			rf.FatalWithLock("[BUG] args.PrevLogIndex <= 0 "+
				"and args.PrevLogTerm (%d) not match with"+
				" rf.log.LastIncludedTerm (%d)", args.PrevLogIndex,
				rf.log.LastIncludedTerm)
		} else {
			reply.MayMatchIndex = args.PrevLogIndex - 1
			rf.DebugWithLock("[reject AE]: args.PrevLogTerm=%d != rf.log.LastIncludedTerm=%d, so leader may should send snapshot again", args.PrevLogIndex, rf.log.LastIncludedTerm)
		}
	} else if args.PrevLogIndex > rf.log.LastIncludedIndex &&
		args.PrevLogTerm != rf.log.TermOf(args.PrevLogIndex) {
		/* 4> 日志不匹配 */
		rf.DebugWithLock("[reject AE]: because args.PrevLogTerm=%d != rf.log.TermOf(args.PrevLogIndex)=%d", args.PrevLogTerm, rf.log.TermOf(args.PrevLogIndex))
		reply.Success = false
		i := args.PrevLogIndex - 1
		/* 1> 过了日志总长度 return 也许匹配到 总长度-1 */
		for ; i >= rf.log.LastIncludedIndex+1 && i >= 0; i-- {
			/* 找到恰好不是该 term 的位置 */
			if rf.log.TermOf(args.PrevLogIndex) != rf.log.TermOf((i)) {
				reply.MayMatchIndex = i
			}
		}
		if i == rf.log.LastIncludedIndex {
			if i >= 0 {
				if rf.log.TermOf(args.PrevLogIndex) != rf.log.TermOf((rf.log.LastIncludedIndex)) {
					reply.MayMatchIndex = i
				} else {
					/* 说明匹配点在日志中 */
					if i > 0 {
						reply.MayMatchIndex = i - 1
					} else {
						reply.MayMatchIndex = 0
					}
				}
			} else {
				/* i == -1 == rf.log.LastIncludedIndex */
				reply.MayMatchIndex = 0
			}
		}
	} else {
		/* 发过来的坐标是 [PrevLogIndex + 1, PrevLogIndex + len(arg.Entries) ] */
		/* rf.log[PrevLogIndex + 1:] 都是冲突项 */
		/* 去除冲突项 */
		rf.log.Entries = rf.log.Entries[:rf.log.getEntryIndex(args.PrevLogIndex+1)]

		if args.Entries == nil || len(args.Entries) == 0 {
			rf.DebugWithLock("get a heartbeat from S[%d]", args.LeaderId)
		} else {
			rf.DebugWithLock("get log entries from S[%d]", args.LeaderId)
			/* 后添新项 */
			rf.log.Entries = append(rf.log.Entries, args.Entries...)
			rf.DebugWithLock("append logs:%v from S[%d]", args.Entries, args.LeaderId)
		}
		rf.persist()

		rf.DebugWithLock("args.LeaderCommit=%v rf.log.Len()-1=%v rf.lastApplied=%v",
			args.LeaderCommit, rf.log.Len()-1, rf.lastApplied)

		/* 更新 commitIndex  */
		rf.commitIndex = Min(args.LeaderCommit, rf.log.Len()-1)
		/* 还应该 log apply to state machine */
		if rf.commitIndex > rf.lastApplied {
			rf.DebugWithLock("can apply logs")
			go func() { rf.signalApplyCh <- interface{}(nil) }()
		}
		rf.resetTimerCh <- true /* 重置等待选举的超时定时器 */
		reply.Success = true
		reply.MayMatchIndex = rf.log.Len() - 1
	}
}

/* 更新 commitIndex */
func (rf *Raft) ApplyCommittedMsgs() {
	for !rf.killed() {
		<-rf.signalApplyCh
		rf.mu.Lock()
		rf.DebugWithLock("can apply logs wit lock")
		/* bug lastApplied=0 */
		var beginIndex int
		if rf.lastApplied < rf.log.LastIncludedIndex {
			/* 安装快照到service */
			beginIndex = rf.log.LastIncludedIndex + 1
			msg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.log.LastIncludedTerm,
				SnapshotIndex: rf.log.LastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			beginIndex = rf.lastApplied + 1
		}

		for i := beginIndex; i <= rf.commitIndex; i++ {
			if rf.log.isIndexInSnapShot(i) {
				rf.DebugWithLock("i=%v rf.log=%+v", i, rf.log)
				break
			}
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log.at(i).Command,
				CommandIndex:  i,
				SnapshotValid: false,
			}
			rf.lastApplied = i
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleApplyEntriesImpl(i int, oldTerm int, oldState State,
	heartBeatAckCnt *int, ctx context.Context, cancel context.CancelFunc,
	reply *AppendEntriesReply, needRetry bool /* need_apply bool */) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/* 领导者状态已经发生改变 */
	if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
		rf.DebugWithLock("handleApplyEntriesImpl: state or term change")
		cancel()
		return true
	}
	rf.DebugWithLock("reply:%#v", reply)
	if reply.Success {
		*heartBeatAckCnt++
		rf.DebugWithLock("get heartBeatAck from S[%d], now it get %d Ack", i, *heartBeatAckCnt)

		rf.matchIndex[i] = reply.MayMatchIndex
		rf.nextIndex[i] = rf.matchIndex[i] + 1

		/* 更新 matchIndex AND nextIndex */
		if reply.MayMatchIndex < rf.log.LastIncludedIndex {
			return false
		}

		if rf.nextIndex[i] > rf.log.Len() { //debug
			cancel()
			log.Fatalf("[BUG] rf.nextIndex[%d]=%d >rf.log.Len()=%d",
				i, rf.nextIndex[i], rf.log.Len())
		}

		rf.DebugWithLock("rf.log=%v rf.matchIndex[i]=%d", rf.log, rf.matchIndex[i])
		if rf.log.TermOf(rf.matchIndex[i]) == rf.currentTerm {
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
					/* leader 可以应用日志了 */
					if rf.commitIndex > rf.lastApplied {
						rf.DebugWithLock("can apply logs")
						go func() { rf.signalApplyCh <- interface{}(nil) }()
					}
					break
				}
			}
		}
		return true
	} else if reply.Term > rf.currentTerm {
		/* 任期小 不配当领导者 */
		/* 变回 跟随者 */
		rf.ResetToFollowerWithLock(fmt.Sprintf("小于 S[%d]任期 T[%d] 不配当领导者", i, reply.Term))
		rf.votedFor = -1
		rf.currentTerm = reply.Term /* 更新任期 */
		rf.persist()
		cancel()
		return true
	} else {
		if needRetry {
			/* 降低 nextIndex 并重试 */
			rf.nextIndex[i] = reply.MayMatchIndex + 1
			/* continue */
			return false
		} else {
			return true
		}
	}
}

func (rf *Raft) HandleApplyEntries(i int, oldTerm int, oldState State,
	heartBeatAckCnt *int, ctx context.Context, cancel context.CancelFunc,
	reply *AppendEntriesReply) bool {
	return rf.handleApplyEntriesImpl(i, oldTerm, oldState, heartBeatAckCnt, ctx, cancel, reply, true)
}
