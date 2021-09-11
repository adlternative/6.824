package raft

import "log"

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

func (rf *Raft) ResetToFollower(reason string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ResetToFollowerWithLock(reason)
}

func (rf *Raft) ResetToFollowerWithLock(reason string) {
	log.Printf("[%d] ResetToFollower with log: %v, term: %v reason: %s", rf.me, rf.log, rf.currentTerm, reason)
	switch rf.state {
	case Leader:
		rf.logger.Infof("[%d]  LEADER --> FOLLOWER!", rf.me)
	case Candidater:
		rf.logger.Infof("[%d]  CANDIDATER --> FOLLOWER!", rf.me)
	case Follower:
		rf.logger.Infof("[%d]  FOLLLOWER --> FOLLOWER!", rf.me)
	default:
		rf.logger.Fatalf("[%d]  UNKNOWN --> FOLLOWER!", rf.me)
	}
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) TurnToLeaderWithLock() {
	/* assert rf.state = Candidater */
	rf.logger.Infof("[%d] CANDIDATER --> LEADER!", rf.me)
	log.Printf("[%d] TurnToLeaderWithLock with log: %v term: %v", rf.me, rf.log, rf.currentTerm)

	rf.state = Leader
	// update nextIndex[]
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	// update matchIndex[]
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
	}
	/* 变成 leader 后定期发送心跳包 */
	go rf.HeartBeatTicker()
}
