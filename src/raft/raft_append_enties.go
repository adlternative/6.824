package raft

import (
	"context"
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
	Term    int  /* 投票人的任期 */
	Success bool /*  如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了 结果为真*/
}

/* leader 才可以定期发送心跳包 */
func (rf *Raft) HeartBeatTicker() {
	atomic.AddInt32(&rf.routineCnt, 1)
	defer atomic.AddInt32(&rf.routineCnt, -1)

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
			atomic.AddInt32(&rf.routineCnt, 1)
			defer atomic.AddInt32(&rf.routineCnt, -1)

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	atomic.AddInt32(&rf.routineCnt, 1)
	defer atomic.AddInt32(&rf.routineCnt, -1)

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
