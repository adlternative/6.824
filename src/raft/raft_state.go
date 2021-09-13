package raft

import "log"

type State int

const (
	Leader State = iota
	Candidater
	Follower
)

func (s State) ToString() string {
	switch s {
	case Leader:
		return "Leader"
	case Candidater:
		return "Candidater"
	case Follower:
		return "Follower"
	default:
		log.Fatalf("bug?")
		return "(nil)"
	}
}

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
	log.Printf("T[%d] S%d ResetToFollower with log: %v, term: %v reason: %s",
		rf.currentTerm, rf.me, rf.log, rf.currentTerm, reason)
	switch rf.state {
	case Leader:
		log.Printf("T[%d] S[%d] LEADER --> FOLLOWER!", rf.currentTerm, rf.me)
	case Candidater:
		log.Printf("T[%d] S[%d] CANDIDATER --> FOLLOWER!", rf.currentTerm, rf.me)
	case Follower:
		log.Printf("T[%d] S[%d] FOLLLOWER --> FOLLOWER!", rf.currentTerm, rf.me)
	default:
		log.Fatalf("T[%d] S[%d] ERROR?!", rf.currentTerm, rf.me)
	}
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) TurnToLeaderWithLock() {
	/* assert rf.state = Candidater */
	rf.logger.Infof("S[%d] CANDIDATER --> LEADER!", rf.me)
	log.Printf("T[%d] S[%d] TurnToLeaderWithLock with log: %v term: %v", rf.currentTerm, rf.me, rf.log, rf.currentTerm)

	rf.state = Leader
	rf.votedFor = -1
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
