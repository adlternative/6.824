package raft

import (
	"context"
	"fmt"
	// "log"
	"time"
)

const (
	OK        = iota
	ErrOldRPC = iota
	ErrSnapshotMismatch
	ErrLogMismatch
	ErrTermTooSmall
	ErrPrevLogIndexTooBig
	ErrPrevLogIndexTooSmall
)

type AppendEntriesArgs struct {
	Term         int       /* 领导者人的任期 */
	LeaderId     int       /* 领导者人的 ID */
	PrevLogIndex int       /* 领导者人发送的新日志的前一条日志索引 */
	PrevLogTerm  int       /* 领导者人发送的新日志的前一条日志任期 */
	LeaderCommit int       /* 领导者人的最后一条日志的索引 */
	Entries      []RaftLog /* 领导者人发送的新日志 */
}

type AppendEntriesReply struct {
	Term          int  /* 跟随者的任期 */
	Success       bool /* 跟随者成功添加日志项*/
	MayMatchIndex int  /* 跟随者的最后日志索引 */
	Error         int  /* 错误信息 */
}

/* leader 才可以定期发送心跳包 */
func (rf *Raft) HeartBeatTicker() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	/* 立即发送一波 */
	rf.HeartBeatTimeOutCallBack(ctx, cancel)

	HeartBeatTimeOut := time.NewTimer(rf.SendHeartBeatTimeOut) /* 100 */
	defer HeartBeatTimeOut.Stop()

	for !rf.killed() {
		/* 如果不是领导者则应该退出 */
		select {
		/* 超时 */
		case <-HeartBeatTimeOut.C:
			/* 发送心跳 */
			HeartBeatTimeOut.Reset(rf.SendHeartBeatTimeOut)
			go rf.HeartBeatTimeOutCallBack(ctx, cancel)
		case <-rf.signalHeartBeatTickerCh:
			/* 发送心跳 */
			HeartBeatTimeOut.Reset(rf.SendHeartBeatTimeOut)
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

	changed, oldTerm, oldState := rf.AreStateOrTermChange(-1, Leader)
	if changed {
		cancel()
		return
	}

	/* 发送 AppendEntries request rpc */

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for !rf.killed() {
				rf.mu.Lock()

				/* 领导者状态已经发生改变 */
				if changed, _, _ = rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
					cancel()
					rf.mu.Unlock()
					return
				}

				logicPrevLogIndex := rf.nextIndex[i] - 1

				/* 如果我们需要 appendEntries 却发现 nextIndex[i]-1
				 * (用来作为 prevlogIndex 竟然比日志中保存的最后的index 还要小
				 * 那么这个 prevlogIndex 我们是无法再拿到了，我们应当直接发送快照) */
				if logicPrevLogIndex < rf.Log.LastIncludedIndex {
					reply := &InstallSnapshotReply{}
					args := &InstallSnapshotArgs{
						Term:              rf.CurrentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.Log.LastIncludedIndex,
						LastIncludedTerm:  rf.Log.LastIncludedTerm,
						Data:              rf.persister.ReadSnapshot(),
					}

					rf.DebugWithLock("want to send snapshot to S[%d], args=%+v", i, args)

					rf.mu.Unlock()
					/* ==========UNLOCK SPACE========= */
					if ok := rf.sendInstallSnapshot(i, args, reply); !ok {
						DPrintf("but sendInstallSnapshot not ok!")
						// rf.Debug("sendInstallSnapshot to [%d]: not ok", i)
						return
					}
					/* =================== */
					if rf.HandleInstallSnapshot(i, oldTerm, oldState, &heartBeatAckCnt, ctx, cancel, args, reply) {
						return
					}
				} else {
					reply := &AppendEntriesReply{}
					args := &AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: logicPrevLogIndex,
						PrevLogTerm:  rf.Log.TermOf(logicPrevLogIndex),
						Entries:      append([]RaftLog{}, rf.Log.Entries[rf.Log.getEntryIndex(rf.nextIndex[i]):]...),
					}
					// log.Printf("S[%d] [SEND]:args=%+v?", rf.me, args)

					rf.DebugWithLock("want to send logs(%d,%d) to S[%d]",
						rf.Log.getEntryIndex(rf.nextIndex[i]),
						rf.Log.getEntryIndex(rf.nextIndex[i])+len(args.Entries), i)

					rf.mu.Unlock()
					/* ==========UNLOCK SPACE========= */
					/* [TODO] 出错多少次退出？ */
					if ok := rf.sendAppendEntries(i, args, reply); !ok {
						DPrintf("but sendAppendEntries not ok!")
						return
					}
					/* =================== */
					if rf.HandleApplyEntries(i, oldTerm, oldState, &heartBeatAckCnt, ctx, cancel, reply) {
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
	/* 等待初始化? */
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("S[%d] [RECV]:%v", rf.me, args)
	// defer func() { log.Printf("S[%d] reply with %+v", rf.me, reply) }()
	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	// oldTerm := rf.CurrentTerm

	if args.Entries == nil || len(args.Entries) == 0 {
		rf.DebugWithLock("get a heartbeat from S[%d]", args.LeaderId)
	} else {
		rf.DebugWithLock("get log entries from S[%d]", args.LeaderId)
	}

	/* 设置返回值为 follower 的任期 */
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		/* 1. 如果领导者的任期 小于 接收者的当前任期 返回假 */
		rf.DebugWithLock("[reject AE]: 领导者的任期 %v 小于 接收者的当前任期 %v", args.Term, rf.CurrentTerm)
		reply.Success = false
		reply.Error = ErrTermTooSmall
		return
	} else if args.Term > rf.CurrentTerm {
		/* 2. 如果领导者的任期比自己的高，更新自己任期 */
		rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] AE", args.Term, args.LeaderId))
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
		/*如果 leader 的 term 比我大，但发送的日志比较旧， 截断日志 */
		needPersist = true
	} else {
		/* ASSERT(rf.CurrentTerm == args.Term) */
		if rf.state == Candidate {
			/* 选举人收到了新 leader 的 appendEntriesRpc */
			rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] AE", args.Term, args.LeaderId))
			rf.VotedFor = -1
			needPersist = true
			/* 变回 跟随者 */
		}
	}

	/* ASSERT( rf.state == FOLLOWER) */
	rf.resetTimer() /* 重置等待选举的超时定时器 */

	/*  即该条目的任期在prevLogIndex上能和prevLogTerm匹配上*/
	/* PrevLogIndex > 最后一条日志的坐标 */
	if args.PrevLogIndex > rf.Log.Len()-1 {
		/*  1> prevlogindex 竟然超过了 日志总长度 -1
		那么说明我们的leader 太超前， matchindex 设置为  len -1 */
		reply.Success = false
		reply.Error = ErrPrevLogIndexTooBig
		reply.MayMatchIndex = rf.Log.Len() - 1
		rf.DebugWithLock("[reject AE]: args.PrevLogIndex=%d > rf.log.Len()-1=%d, so leader to need to turn down prelogindex", args.PrevLogIndex, rf.Log.Len()-1)
	} else if args.PrevLogIndex < rf.Log.LastIncludedIndex {
		/* 2> prevlogindex 竟然小于快照最后的坐标
		 * 那么说明我们的 leader 太落后？直接原坐标返回
		 * (leader 说不定需要重新发送快照)
		 * (unlikely)
		 */
		reply.Success = false
		reply.Error = ErrPrevLogIndexTooSmall
		reply.MayMatchIndex = args.PrevLogIndex
		rf.DebugWithLock("[reject AE]: args.PrevLogIndex=%d < rf.log.LastIncludedIndex=%d, so leader may need to send snapshot again", args.PrevLogIndex, rf.Log.LastIncludedIndex)
	} else if args.PrevLogIndex == rf.Log.LastIncludedIndex &&
		args.PrevLogTerm != rf.Log.LastIncludedTerm {
		/* 3> 快照不匹配
		 * (leader 说不定需要重新发送快照)
		 * (unlikely)
		 */
		if args.PrevLogIndex <= 0 {
			rf.FatalWithLock("[BUG] args.PrevLogIndex <= 0 "+
				"and args.PrevLogTerm (%d) not match with"+
				" rf.log.LastIncludedTerm (%d)", args.PrevLogIndex,
				rf.Log.LastIncludedTerm)
		}
		reply.Success = false
		reply.Error = ErrSnapshotMismatch
		reply.MayMatchIndex = args.PrevLogIndex - 1
		rf.DebugWithLock("[reject AE]: args.PrevLogTerm=%d != rf.log.LastIncludedTerm=%d, so leader may should send snapshot again", args.PrevLogIndex, rf.Log.LastIncludedTerm)
	} else if args.PrevLogIndex > rf.Log.LastIncludedIndex &&
		args.PrevLogTerm != rf.Log.TermOf(args.PrevLogIndex) {
		/* 4> 日志不匹配 */
		rf.DebugWithLock("[reject AE]: because args.PrevLogTerm=%d != rf.log.TermOf(args.PrevLogIndex)=%d", args.PrevLogTerm, rf.Log.TermOf(args.PrevLogIndex))
		reply.Success = false
		reply.Error = ErrLogMismatch
		i := args.PrevLogIndex - 1
		/* 1> 过了日志总长度 return 也许匹配到 总长度-1 */
		for ; i >= rf.Log.LastIncludedIndex+1; i-- {
			/* 找到恰好不是该 term 的位置 */
			if rf.Log.TermOf(args.PrevLogIndex) != rf.Log.TermOf((i)) {
				reply.MayMatchIndex = i
			}
		}
		if i == rf.Log.LastIncludedIndex {
			if i >= 0 {
				if rf.Log.TermOf(args.PrevLogIndex) != rf.Log.TermOf((rf.Log.LastIncludedIndex)) {
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
		/* 发过来的坐标是 [PrevLogIndex + 1, PrevLogIndex + len(arg.Entries) )*/
		/* 如果和日志有不相同的点则去除冲突项 */
		i := 0
		for ; i < len(args.Entries); i++ {
			pos := i + args.PrevLogIndex + 1
			if (pos < rf.Log.Len()) && (rf.Log.TermOf(pos) == args.Entries[i].Term) {
				/* 寻找第一个不同点 */
				continue
			} else if pos >= rf.Log.Len() {
				/* 到达日志末尾，说明 args 更多 */
				rf.Log.Entries = append(rf.Log.Entries, args.Entries[i:]...)
				needPersist = true
				break
			} else if rf.Log.TermOf(pos) != args.Entries[i].Term {
				/* 说明出现了不同的地方，去重并追加*/
				rf.Log.Entries = append(rf.Log.Entries[:rf.Log.getEntryIndex(pos)], args.Entries[i:]...)
				needPersist = true
				break
			}
		}

		rf.DebugWithLock("args.LeaderCommit=%v rf.log.Len()-1=%v rf.lastApplied=%v",
			args.LeaderCommit, rf.Log.Len()-1, rf.lastApplied)

		/* 更新 commitIndex  */
		rf.commitIndex = Min(args.LeaderCommit, rf.Log.Len()-1)
		/* 还应该 log apply to state machine */
		if rf.commitIndex > rf.lastApplied {
			rf.DebugWithLock("can apply logs")
			go func() { rf.signalApplyCh <- interface{}(nil) }()
		}
		reply.Success = true
		reply.MayMatchIndex = rf.Log.Len() - 1
	}
}

/* 更新 commitIndex */
func (rf *Raft) ApplyCommittedMsgs() {
	for !rf.killed() {
		<-rf.signalApplyCh
		rf.mu.Lock()
		rf.DebugWithLock("can apply logs wit lock")
		for rf.lastApplied+1 <= rf.commitIndex {
			if rf.lastApplied+1 <= rf.Log.LastIncludedIndex {
				rf.FatalWithLock("[BUG] i=%v rf.log=%+v", rf.lastApplied, rf.Log)
				break
			}
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.Log.at(rf.lastApplied + 1).Command,
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
			}
			rf.DebugWithLock("apply msg:%+v", msg)
			rf.lastApplied++
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) HandleApplyEntries(i int, oldTerm int, oldState State,
	heartBeatAckCnt *int, ctx context.Context, cancel context.CancelFunc,
	reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/* 领导者状态已经发生改变 */
	if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
		rf.DebugWithLock("handleApplyEntriesOrInstallSnapShot: state or term change")
		cancel()
		return true
	}
	rf.DebugWithLock("get S[%d] ApplyEntries reply:%+v", i, reply)
	if reply.Success {
		*heartBeatAckCnt++
		rf.DebugWithLock("get heartBeatAck from S[%d], now it get %d Ack", i, *heartBeatAckCnt)

		if reply.MayMatchIndex < rf.matchIndex[i] {
			/* 说明是旧的 RPC 回复 */
			rf.DebugWithLock("get old reply from S[%d]", i)
			return true
		}

		if reply.MayMatchIndex >= rf.Log.Len() {
			/* JUST RETURN */
			rf.DebugWithLock("[SPECIAL] get reply from S[%d] mayMatchIndex=%v >= rf.Log.Len()=%v",
				i, reply.MayMatchIndex, rf.Log.Len())
			return true
		}

		/* 更新 matchIndex AND nextIndex */
		rf.matchIndex[i] = reply.MayMatchIndex
		rf.nextIndex[i] = rf.matchIndex[i] + 1

		// rf.DebugWithLock("rf.log=%+v", rf.Log)
		rf.DebugWithLock("matchIndex=%+v commitIndex=%d", rf.matchIndex, rf.commitIndex)
		/* 如果返回的 MATCHINDEX 是在快照中 直接返回，并决定是否需要重试（而不用做下面的提交了） */
		if rf.Log.isIndexInSnapShot(rf.matchIndex[i]) {
			return false
		}

		/* if rf.matchIndex[i] <= lastIncludeIndex */
		if rf.Log.TermOf(rf.matchIndex[i]) == rf.CurrentTerm {
			matchCnt := 0
			for j := 0; j < len(rf.matchIndex); j++ {
				if rf.matchIndex[j] == rf.matchIndex[i] {
					matchCnt++
				}
			}
			rf.DebugWithLock("matchCnt:%d", matchCnt)

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
			}
		}
		return true
	} else {
		if reply.Term > rf.CurrentTerm {
			/* 任期小 不配当领导者 */
			/* 变回 跟随者 */
			rf.ResetToFollowerWithLock(fmt.Sprintf("小于 S[%d]任期 T[%d] 不配当领导者", i, reply.Term))
			rf.VotedFor = -1
			rf.CurrentTerm = reply.Term /* 更新任期 */
			rf.persist()
			cancel()
			return true
		}

		if reply.Error == ErrOldRPC {
			// log.Printf("match:%+v next:%+v len:%+v", rf.matchIndex, rf.nextIndex, rf.Log.Len())
			return true
		} else {
			/* 降低 nextIndex*/
			rf.nextIndex[i] = reply.MayMatchIndex + 1
			return false
		}
	}
}
