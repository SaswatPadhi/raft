package raft

import (
	"encoding/gob"

	"fmt"
)

// Raft Msg RPCs
const (
	TERMINATE = iota

	REQUEST_VOTE
	REQUEST_VOTE_RESP

	APPEND_ENTRIES
	APPEND_ENTRIES_RESP
)

type raft_msg struct {
	Addr              int        // Source server
	RPC               int8       // Raft RPC
	Entries           []logEntry // entries to append
	Leader            int        // Leader ID
	LeaderCommitIndex int        // leader's commit idx
	PrevLogIndex      int        // index of last log
	PrevLogTerm       int64      // term of last log
	Result            bool       // response
	Term              int64      // Candidate's / Leader's Term
}

func (req *raft_msg) toString() string {
	return fmt.Sprintf("{CMD: %1d, ADR: %d, LDR: %d, TRM: %d, PLI: %d, PLT: %d, LCI: %d}",
		req.RPC, req.Addr, req.Leader, req.Term,
		req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommitIndex)
}

func init() {
	gob.Register(raft_msg{})
}
