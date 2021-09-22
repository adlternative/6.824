package raft

import (
	"context"
	"fmt"
	"sync/atomic"
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
	Term          int  /* 跟随者的任期 */
	Success       bool /* 跟随者成功安装快照*/
	MayMatchIndex int  /* 跟随者的最后日志索引 */
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) HandleInstallSnapshot(i int, oldTerm int, oldState State,
	heartBeatAckCnt *int, ctx context.Context,
	cancel context.CancelFunc, reply *InstallSnapshotReply) bool {
	return rf.handleApplyEntriesImpl(i, oldTerm, oldState, heartBeatAckCnt, ctx, cancel, (*AppendEntriesReply)(reply), false)
}

/* InstallSnapshot  */
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	atomic.AddInt32(&rf.routineCnt, 1)
	defer atomic.AddInt32(&rf.routineCnt, -1)
	rf.DebugWithLock("GET T[%d] S[%d] IS args:%#v", args.Term, args.LeaderId, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		/* 如果领导者的任期比自己的高，更新自己任期 */
		rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] IS", args.Term, args.LeaderId))
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	} else {
		/* ASSERT(rf.currentTerm == args.Term) */
		if rf.state == Candidate {
			/* 选举人收到了新 leader 的 appendEntriesRpc */
			rf.ResetToFollowerWithLock(fmt.Sprintf("GET T[%d] S[%d] IS", args.Term, args.LeaderId))
			rf.votedFor = -1
			rf.persist()
			/* 变回 跟随者 */
		}
	}

	/* ASSERT( rf.state == FOLLOWER) */

	/* 如果现存的日志条目与快照中最后包含的日志条目具有相同的索引值和任期号，
	则保留其后的日志条目并进行回复*/
	if args.LastIncludedIndex < rf.log.Len() &&
		args.LastIncludedTerm == rf.log.TermOf((args.LastIncludedIndex)) {
		rf.DebugWithLock("the follower seem have something more than leader's snapshot, just return!")
		rf.resetTimerCh <- true /* 重置等待选举的超时定时器 */
		reply.Success = true
		reply.MayMatchIndex = args.LastIncludedIndex /* 移动到快照的最后坐标即可 */
		return
	}

	/* 发来的快照旧 */
	if args.LastIncludedIndex < rf.log.LastIncludedIndex {
		reply.Success = false
		return
	}

	/* APPLY TO SERVE*/
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
	rf.snapShotPersistCond.Wait()
	/* 条件变量 等待持久化完成的信号*/
	rf.resetTimerCh <- true /* 重置等待选举的超时定时器 */
	reply.Success = true
	reply.MayMatchIndex = args.LastIncludedIndex /* 移动到快照的最后坐标即可 */
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
	/* 如果有新的提交日志则不安装本次的快照 */
	if lastIncludedIndex < rf.commitIndex {
		rf.DebugWithLock("lastIncludedIndex=%d < rf.commitIndex=%d 如果有新的提交日志则不安装本次的快照", lastIncludedIndex, rf.commitIndex)
		return false
	}
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.log.LastIncludedIndex = lastIncludedIndex
	rf.log.LastIncludedTerm = lastIncludedTerm
	rf.log.Entries = rf.log.Entries[:0]
	rf.persistStateAndSnapShot(snapshot)
	rf.snapShotPersistCond.Signal()
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

	if index < rf.log.LastIncludedIndex {
		rf.DebugWithLock("index=%d < rf.log.LastIncludedIndex=%d we already have a bigger snapshot", index, rf.log.LastIncludedIndex)
		return
	}

	rf.DebugWithLock("index=%d rf.log.getEntryIndex(index)=%d rf.log=%v", index, rf.log.getEntryIndex(index), rf.log)

	// /* 丢弃 index 之前旧的日志 */
	entryIndex := rf.log.getEntryIndex(index)
	rf.log.LastIncludedIndex = index
	rf.log.LastIncludedTerm = rf.log.Entries[entryIndex].Term
	rf.log.Entries = rf.log.Entries[entryIndex+1:]

	rf.persistStateAndSnapShot(snapshot)
}
