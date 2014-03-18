package raft

import (
	"encoding/gob"

	"fmt"
)

// Raft Msg Commands
const (
	TERMINATE = iota

	REQUEST_VOTE
	REQUEST_VOTE_RESP

	APPEND_ENTRIES
	APPEND_ENTRIES_RESP
)

type raft_msg struct {
	Addr         int   // Source server
	Command      int8  // Raft Command
	Leader       int   // Leader ID
	LeaderCommit int64 // leader's commit idx
	PrevLogIndex int64 // index of last log
	PrevLogTerm  int64 // term of last log
	Result       bool  // response
	Term         int64 // Candidate's / Leader's Term
}

func (req *raft_msg) toString() string {
	return fmt.Sprintf("{CMD: %1d, ADR: %d, LDR: %d, TRM: %d, PLI: %d, PLT: %d, LCI: %d}",
		req.Command, req.Addr, req.Leader, req.Term,
		req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommit)
}

func init() {
	gob.Register(raft_msg{})
}
