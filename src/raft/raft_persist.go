package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
)

//
// 将 Raft 的持久状态保存到稳定存储中，
// 以后可以在崩溃和重新启动后检索它。
// 参见论文的图 2 了解什么应该是持久化的。
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// 恢复之前持久化的状态。
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.log = append(rf.log, RaftLog{})
		rf.currentTerm = 0
		rf.votedFor = -1
		return
	}

	// Your code here (2C).
	var logs []RaftLog
	var currentTerm, votedFor int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if (d.Decode(&currentTerm) != nil) ||
		(d.Decode(&votedFor) != nil) ||
		(d.Decode(&logs) != nil) {
		log.Fatalf("failed to readPersist")
	} else {
		rf.log = logs
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
	}
}
