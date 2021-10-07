package raft

import (
	"bytes"
	"log"
	// "log"

	"6.824/labgob"
)

//
// 将 Raft 的持久状态保存到稳定存储中，
// 以后可以在崩溃和重新启动后检索它。
// 参见论文的图 2 了解什么应该是持久化的。
//
func (rf *Raft) stateEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log.Entries)
	e.Encode(rf.Log.LastIncludedIndex)
	e.Encode(rf.Log.LastIncludedTerm)
	// defer log.Printf("#rf.log=%+v rf.CurrentTerm=%v rf.VotedFor=%v", rf.Log, rf.CurrentTerm, rf.VotedFor)
	return w.Bytes()
}

func (rf *Raft) persist() {
	data := rf.stateEncode()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateAndSnapShot(snapshot []byte) {
	data := rf.stateEncode()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// 恢复之前持久化的状态。
//WWW
func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.Log.Entries = append(rf.Log.Entries, RaftLog{})
		rf.Log.LastIncludedIndex = -1
		rf.Log.LastIncludedTerm = -1
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		return
	}

	// // var logs RaftLogs
	var Logss []RaftLog
	var CurrentTerm int
	var VotedFor int
	var LastIncludedIndex int
	var LastIncludedTerm int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if (d.Decode(&CurrentTerm) != nil) ||
		(d.Decode(&VotedFor) != nil) ||
		(d.Decode(&Logss) != nil) ||
		(d.Decode(&LastIncludedIndex) != nil) ||
		(d.Decode(&LastIncludedTerm) != nil) {
		log.Fatalf("failed to readPersist")
	} else {
		// log.Printf("@log=%+v CurrentTerm=%v VotedFor=%v", Logss, CurrentTerm, VotedFor)
		rf.Log.Entries = Logss
		rf.Log.LastIncludedIndex = LastIncludedIndex
		rf.Log.LastIncludedTerm = LastIncludedTerm
		if rf.Log.Entries == nil {
			rf.Log.Entries = append(rf.Log.Entries, RaftLog{})
		}
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		if rf.Log.LastIncludedIndex != -1 {
			rf.commitIndex = rf.Log.LastIncludedIndex
			rf.lastApplied = rf.Log.LastIncludedIndex
		}
		// log.Printf("$rf.log=%+v rf.CurrentTerm=%v rf.VotedFor=%v", rf.Log, rf.CurrentTerm, rf.VotedFor)
	}
}
