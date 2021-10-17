package raft

import (
	"context"
	"fmt"
)

type InstallSnapshotArgs struct {
	Term              int //领导人的任期号
	LeaderId          int //领导人的 Id，以便于跟随者重定向请求
	LastIncludedIndex int //快照中包含的最后日志条目的索引值
	LastIncludedTerm  int //快照中包含的最后日志条目的任期号
	// Offset            int    //分块在快照中的字节偏移量
	Data []byte //从偏移量开始的快照分块的原始字节
	// Done bool   //	如果这是最后一个分块则为 true
}

type InstallSnapshotReply struct {
	Term    int  /* 跟随者的任期 */
	Success bool /* 跟随者成功安装快照*/
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) HandleInstallSnapshot(i int, oldTerm int, oldState State,
	heartBeatAckCnt *int, ctx context.Context,
	cancel context.CancelFunc, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	/* 领导者状态已经发生改变 */
	if changed, _, _ := rf.AreStateOrTermChangeWithLock(oldTerm, oldState); changed {
		rf.DebugWithLock("HandleInstallSnapshot: state or term change")
		cancel()
		return true
	}
	rf.DebugWithLock("i:%d reply:%+v", i, reply)
	if reply.Success {
		*heartBeatAckCnt++
		rf.DebugWithLock("get heartBeatAck from S[%d], now it get %d Ack", i, *heartBeatAckCnt)

		/* 更新 matchIndex AND nextIndex */
		rf.matchIndex[i] = args.LastIncludedIndex
		rf.nextIndex[i] = rf.matchIndex[i] + 1

		// rf.DebugWithLock("rf.log=%+v", rf.Log)
		rf.DebugWithLock("matchIndex=%+v commitIndex=%d", rf.matchIndex, rf.commitIndex)
		return false /* 继续发日志 */
	} else if reply.Term > rf.CurrentTerm {
		/* 任期小 不配当领导者 */
		/* 变回 跟随者 */
		rf.ResetToFollowerWithLock(fmt.Sprintf("小于 S[%d]任期 T[%d] 不配当领导者", i, reply.Term))
		rf.VotedFor = -1
		rf.CurrentTerm = reply.Term /* 更新任期 */
		rf.persist()
		cancel()
		return true
	} else {
		/* 假设对面有个更新的快照？？？或者有一些额外的日志 ??? */

		/* 考虑到对端可能收到多个安装快照RPC 恢复给 leader ,
		*	可能会错误的将 matchindex 改小，然后导致多次的发送快照，
		*	目前没有想到很好的方式 不修改 matchIndex[i],
		*	即使 matchIndex[i] < lastIncludeIndex,
		*	我们仍然不能确定对方匹配到哪里。
		* 如果添加一个 MayMatchIndex 或许是可行的，
		* 只是我们需要对端能够将它的 lastINcludeIndex 通知给 rpc
		* （之前使用 信号量做，非常不优雅，但也不太容易用 channel）
		 */
		return true
	}
}

/* InstallSnapshot  */
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DebugWithLock("GET T[%d] S[%d] IS args:%#v", args.Term, args.LeaderId, args)

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.CurrentTerm {
		/* 如果领导者的任期比自己的高，更新自己任期 */
		rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] IS", args.Term, args.LeaderId))
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
		rf.persist()
	} else {
		/* ASSERT(rf.CurrentTerm == args.Term) */
		if rf.state == Candidate {
			/* 选举人收到了新 leader 的 InstallSnapshotRpc */
			rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] IS", args.Term, args.LeaderId))
			rf.VotedFor = -1
			rf.persist()
			/* 变回 跟随者 */
		}
	}

	/* ASSERT( rf.state == FOLLOWER) */
	rf.resetTimer() /* 重置等待选举的超时定时器 */

	/* 发来的快照旧 */
	if args.LastIncludedIndex < rf.Log.LastIncludedIndex ||
		args.LastIncludedIndex < rf.commitIndex {
		rf.DebugWithLock("[reject IS] args.LastIncludedIndex=%d, rf.Log.LastIncludedIndex=%d, rf.commitIndex=%d",
			args.LastIncludedIndex, rf.Log.LastIncludedIndex, rf.commitIndex)
		// reply.MayMatchIndex = rf.commitIndex
		reply.Success = true
		return
	}

	/* APPLY TO SERVE*/
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() { rf.applyCh <- msg }()
	reply.Success = true
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// 一个服务想要切换到快照。 仅当 Raft 在 applyCh 上传达了快照之后
// "没有更新的信息" 时才这样做。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DebugWithLock("CondInstallSnapshot with lastIncludedTerm=%d, lastIncludedIndex=%d", lastIncludedTerm, lastIncludedIndex)
	if !rf.initted() {
		rf.Log.LastIncludedIndex = lastIncludedIndex
		rf.Log.LastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.Init()
		rf.InitCond.Broadcast()
		return true
	}

	/* 如果有新的提交日志则不安装本次的快照 */
	if lastIncludedIndex < 0 {
		rf.FatalWithLock("[BUG] CondInstallSnapshot with lastIncludedTerm=%d, lastIncludedIndex=%d", lastIncludedTerm, lastIncludedIndex)
		return false
	} else if lastIncludedIndex <= rf.Log.LastIncludedIndex {
		rf.DebugWithLock("[reject IS] lastIncludedIndex=%d <= rf.log.LastIncludedIndex=%d 本次的快照不够新", lastIncludedIndex, rf.Log.LastIncludedIndex)
		// rf.snapShotMayMatchIndex = rf.Log.LastIncludedIndex
		return false
	} else if lastIncludedIndex <= rf.commitIndex {
		rf.DebugWithLock("[reject IS] lastIncludedIndex=%d < rf.commitIndex=%d 如果有新的提交日志则不安装本次的快照", lastIncludedIndex, rf.commitIndex)
		// rf.snapShotMayMatchIndex = rf.commitIndex
		return false
	} else if lastIncludedIndex < rf.Log.Len() &&
		lastIncludedTerm == rf.Log.TermOf(lastIncludedIndex) {
		/* 如果现存的日志条目与快照中最后包含的日志条目具有相同的索引值和任期号，
		则保留其后的日志条目并进行回复*/
		rf.DebugWithLock("[reject IS] follower seems have more log than this snapshot. lastIncludedIndex=%d rf.log.Len()=%d", lastIncludedTerm, rf.Log.Len())
		// rf.snapShotMayMatchIndex = lastIncludedIndex
		return false
	}
	/* 持久化快照 */
	rf.Log.LastIncludedIndex = lastIncludedIndex
	rf.Log.LastIncludedTerm = lastIncludedTerm
	rf.Log.Entries = rf.Log.Entries[:0]

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.persistStateAndSnapShot(snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 服务说它已经创建了一个快照，其中包含包括索引在内的所有信息。
// 这意味着服务不再需要通过（并包括）该索引的日志。
// Raft 现在应该尽可能地修剪它的日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/* 如果在我们执行 Snapshot 的时候 我们当前快照的最大index
	 * 比我们 applySnapShot 提交的 msg 中的 index 还要大，
	 * 不用更改，直接返回
	 */

	if index < rf.Log.LastIncludedIndex {
		rf.DebugWithLock("index=%d < rf.log.LastIncludedIndex=%d we already have a bigger snapshot", index, rf.Log.LastIncludedIndex)
		return
	}

	// /* 丢弃 index 之前旧的日志 */
	entryIndex := rf.Log.getEntryIndex(index)
	rf.Log.LastIncludedIndex = index
	rf.Log.LastIncludedTerm = rf.Log.Entries[entryIndex].Term
	rf.Log.Entries = rf.Log.Entries[entryIndex+1:]

	rf.persistStateAndSnapShot(snapshot)
}
