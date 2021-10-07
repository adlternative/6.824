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
func (rf *Raft) stateEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
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
	defer func() {
		log.Printf("S[%03d] [INIT] log=%+v CurrentTerm=%v VotedFor=%v\n", rf.me, rf.Log, rf.CurrentTerm, rf.VotedFor)
	}()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.Log.Entries = append(rf.Log.Entries, RaftLog{}) /* 空项 */
		rf.Log.LastIncludedIndex = -1
		rf.Log.LastIncludedTerm = -1
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		return
	}

	var Logs RaftLogs
	var CurrentTerm int
	var VotedFor int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if (d.Decode(&CurrentTerm) != nil) ||
		(d.Decode(&VotedFor) != nil) ||
		(d.Decode(&Logs) != nil) {
		log.Fatalf("failed to readPersist")
	} else {
		rf.Log = Logs
		if rf.Log.Entries == nil {
			log.Printf("eh? log.Entries is nil?\n")
			rf.Log.Entries = []RaftLog{}
		}
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		if rf.Log.LastIncludedIndex != -1 {
			rf.commitIndex = rf.Log.LastIncludedIndex
			rf.lastApplied = rf.Log.LastIncludedIndex
		}
	}
}
