package raft

import "sync/atomic"

type State int

const (
	Leader State = iota
	Candidater
	Follower
)

/* （需要外界加锁）服务器状态或者任期发生变化 填入的 oldTerm == -1 || oldState == -1 表示我们不关心相关的属性 */
func (rf *Raft) AreStateOrTermChangeWithLock(oldTerm int, oldState State) (bool, int, State) {
	/* 状态改变  */
	/* -1 表示 不关心 */
	if (oldTerm != rf.currentTerm && oldTerm != -1) ||
		(oldState != rf.state && oldState != -1) {
		return true, rf.currentTerm, rf.state
	}

	return false, rf.currentTerm, rf.state
}

/* 服务器状态或者任期发生变化 填入的 oldTerm == -1 || oldState == -1 表示我们不关心相关的属性 */
func (rf *Raft) AreStateOrTermChange(oldTerm int, oldState State) (bool, int, State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.AreStateOrTermChangeWithLock(oldTerm, oldState)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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

func (rf *Raft) resetToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetToFollowerWithLock()
}

func (rf *Raft) resetToFollowerWithLock() {
	rf.state = Follower
	rf.votedFor = -1
}
