package raft

import "log"

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

func (s State) String() string {
	switch s {
	case Leader:
		return "LEADER"
	case Candidate:
		return "CANDIDATE"
	case Follower:
		return "FOLLOWER"
	default:
		log.Fatalf("[BUG] unknown state?")
		return "(nil)"
	}
}

/* （需要外界加锁）服务器状态或者任期发生变化 填入的 oldTerm == -1 || oldState == -1 表示我们不关心相关的属性 */
func (rf *Raft) AreStateOrTermChangeWithLock(oldTerm int, oldState State) (bool, int, State) {
	/* 状态改变  */
	/* -1 表示 不关心 */
	if (oldTerm != rf.CurrentTerm && oldTerm != -1) ||
		(oldState != rf.state && oldState != -1) {
		rf.DebugWithLock("AreStateOrTermChangeWithLock: oldTerm %v oldState %v rf.CurrentTerm %v rf.state %v", oldTerm, oldState, rf.CurrentTerm, rf.state)
		return true, rf.CurrentTerm, rf.state
	}

	return false, rf.CurrentTerm, rf.state
}

/* 服务器状态或者任期发生变化 填入的 oldTerm == -1 || oldState == -1 表示我们不关心相关的属性 */
func (rf *Raft) AreStateOrTermChange(oldTerm int, oldState State) (bool, int, State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.AreStateOrTermChangeWithLock(oldTerm, oldState)
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.GetStateWithLock()
}

func (rf *Raft) GetStateWithLock() (int, bool) {
	return rf.CurrentTerm, rf.state == Leader
}

func (rf *Raft) ResetToFollower(reason string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ResetToFollowerWithLock(reason)
}

func (rf *Raft) ResetToFollowerWithLock(reason string) {
	rf.DebugWithLock("ResetToFollower with the reason: %s", reason)
	switch rf.state {
	case Leader:
		rf.DebugWithLock("LEADER --> FOLLOWER!")
		for _, ch := range rf.registerNotLeaderNowCh {
			ch <- interface{}(nil)
		}
	case Candidate:
		rf.DebugWithLock("CANDIDATE --> FOLLOWER!")
	case Follower:
		rf.DebugWithLock("FOLLOWER --> FOLLOWER!")
	default:
		log.Fatalf("ERROR?!")
	}
	rf.state = Follower
}

func (rf *Raft) TurnToLeaderWithLock() {
	/* assert rf.state = Candidate */
	rf.DebugWithLock("CANDIDATE --> LEADER!")
	// rf.DebugWithLock("TurnToLeaderWithLock with log: %v", rf.log)

	rf.state = Leader
	rf.VotedFor = -1
	// update nextIndex[]
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.Log.Len()
	}
	// update matchIndex[]
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
	}
	/* 变成 leader 后定期发送心跳包 */
	go rf.HeartBeatTicker()
}

func (rf *Raft) RegisterNotLeaderNowCh(ch chan<- interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.registerNotLeaderNowCh = append(rf.registerNotLeaderNowCh, ch)
}
