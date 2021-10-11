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

	if rf == nil {
		log.Printf("S[%d] Eh? rf== nil", rf.me)
	} else {
		if rf.Log.Entries == nil {
			log.Printf("S[%d] Eh? rf.Log.Entries == nil", rf.me)
		} else {
			log.Printf("S[%d] #len(rf.log.Entries)=%v rf.CurrentTerm=%v rf.VotedFor=%v",
				rf.me, len(rf.Log.Entries), rf.CurrentTerm, rf.VotedFor)
		}
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil {
		log.Fatalf("failed to stateEncode")
	}

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
		rf.haveInit = true
		rf.InitWaitGroup.Done()
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
		lastIncludedTerm := rf.Log.LastIncludedTerm
		lastIncludedIndex := rf.Log.LastIncludedIndex

		rf.Log.LastIncludedTerm = -1
		rf.Log.LastIncludedIndex = -1

		if rf.Log.Entries == nil {
			log.Printf("eh? log.Entries is nil?\n")
			rf.Log.Entries = []RaftLog{}
		}
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor

		rf.mu.Lock()
		/* 如果我们有快照的话？ */
		if lastIncludedIndex != -1 {
			msg := &ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  lastIncludedTerm,
				SnapshotIndex: lastIncludedIndex,
			}
			go func() { rf.applyCh <- *msg }()
		} else {
			rf.InitWaitGroup.Done()
			rf.haveInit = true
		}
		rf.mu.Unlock()
	}
}
