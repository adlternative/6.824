package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) DebugWithLock(format string, a ...interface{}) {
	alist := []interface{}{rf.currentTerm, rf.me, rf.state}
	alist = append(alist, a...)

	log.Printf("T[%03d] S[%03d] I[%s] "+format, alist...)
}

func (rf *Raft) Debug(format string, a ...interface{}) {
	rf.mu.Lock()
	defer rf.mu.Lock()
	rf.DebugWithLock(format, a)
}

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
