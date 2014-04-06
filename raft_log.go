package raft

import (
	"errors"
)

type logEntry struct {
	Data  interface{}
	Index int
	Term  int64
}

type raft_log struct {
	commitIndex int
	lastApplied int
	entries     []logEntry
}

func (log *raft_log) addEntries(entries []logEntry) {
	log.entries = append(log.entries, entries...)
}

func (log *raft_log) createAndAddNewEntry(data interface{}, term int64) {
	log.entries = append(log.entries, logEntry{data, len(log.entries), term})
}

func (log *raft_log) getEntriesAtAndAfter(prevLogIndex int) ([]logEntry, int64) {
	if log.entries != nil {
		if prevLogIndex == 0 {
			return log.entries, -1
		} else {
			return log.entries[prevLogIndex:], log.entries[prevLogIndex-1].Term
		}
	} else {
		return nil, -1
	}
}

func (log *raft_log) getPrevIndex() int {
	if log.entries != nil {
		return log.entries[len(log.entries)-1].Index
	} else {
		return -1
	}
}

func (log *raft_log) getPrevTerm() int64 {
	if log.entries != nil {
		return log.entries[len(log.entries)-1].Term
	} else {
		return -1
	}
}

func (log *raft_log) check(prevTerm int64, prevIndex int) error {
	if prevIndex >= len(log.entries) || prevIndex < 0 {
		return nil
	} else {
		log.entries = log.entries[:prevIndex+1]
	}

	if log.entries[prevIndex].Term == prevTerm {
		return nil
	} else {
		return errors.New("Term conflict")
	}
}
