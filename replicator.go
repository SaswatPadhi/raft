package raft

import (
	"github.com/SaswatPadhi/cluster"

	"encoding/json"
	"io/ioutil"

	"bytes"
	"fmt"
	"time"
)

const (
	UNINITIALIZED = iota
)

// Deviation Thresholds
const (
	Election_Timeout_Deviation   = 0.35
	Heartbeat_Interval_Deviation = 0.05
)

// Replicator states
const (
	CANDIDATE = iota
	FOLLOWER
	LEADER

	ERROR
	STOPPED
)

type raft_config struct {
	Election_Timeout    int
	Heartbeat_Interval  int
	Cluster_Config_File string
}

// The interface that each replicator object must implement.
type Replicator interface {
	ElectionTimeout() time.Duration
	HeartbeatInterval() time.Duration
	Inbox() chan<- interface{}
	IsRunning() bool
	Id() int
	Leader() int
	Outbox() <-chan ClientResponse
	Start() error
	State() int8
	Stop() error
	Term() int64
}

type StateMachine interface {
	Apply(interface{}) interface{}
}

type ClientResponse struct {
	raft_resp bool
	sm_resp   interface{}
}

type replicator struct {
	election_timeout   time.Duration
	heartbeat_interval time.Duration
	inbox              chan interface{}
	internal_inbox     chan *raft_msg
	internal_outbox    chan *raft_msg
	leader             int
	log                *raft_log
	outbox             chan ClientResponse
	machine            StateMachine
	matchIndex         map[int]int
	nextIndex          map[int]int
	server             *cluster.Server
	state              int8
	stop               chan bool
	stopped            chan bool
	term               int64
	votes              int
	voted_for          int
}

/*======================================< PUBLIC INTERFACE >======================================*/

func NewReplicator(id int, sm StateMachine, config_file string) (r *replicator, err error) {
	INFO.Println(fmt.Sprintf("Creating replicator [id: %d, config: %s]", id, config_file))
	defer INFO.Println(fmt.Sprintf("Replicator creation returns [err: %s]", err))

	r = &replicator{
		election_timeout:   UNINITIALIZED,
		heartbeat_interval: UNINITIALIZED,
		inbox:              make(chan interface{}, 32),
		internal_inbox:     make(chan *raft_msg, 32),
		internal_outbox:    make(chan *raft_msg, 32),
		leader:             UNINITIALIZED,
		log:                &raft_log{-1, -1, nil},
		outbox:             make(chan ClientResponse, 32),
		machine:            sm,
		matchIndex:         nil,
		nextIndex:          nil,
		server:             nil,
		state:              ERROR,
		stop:               make(chan bool, 2),
		stopped:            make(chan bool),
		term:               0,
		votes:              0,
		voted_for:          UNINITIALIZED,
	}

	data, err := ioutil.ReadFile(config_file)
	if err != nil {
		return
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	replicator_config := raft_config{}
	if err = decoder.Decode(&replicator_config); err != nil {
		return
	}
	r.election_timeout = time.Duration(randIntDev(replicator_config.Election_Timeout, Election_Timeout_Deviation)) * time.Millisecond
	r.heartbeat_interval = time.Duration(randIntDev(replicator_config.Heartbeat_Interval, Heartbeat_Interval_Deviation)) * time.Millisecond

	s, err := cluster.NewServer(id, replicator_config.Cluster_Config_File)
	if err != nil {
		return
	}
	r.server = s
	r.state = STOPPED

	return
}

func (r *replicator) ElectionTimeout() time.Duration {
	return r.election_timeout
}

func (r *replicator) HeartbeatInterval() time.Duration {
	return r.heartbeat_interval
}

func (r *replicator) Id() int {
	return r.server.Pid()
}

func (r *replicator) Inbox() chan<- interface{} {
	return r.inbox
}

func (r *replicator) IsRunning() bool {
	return r.state != STOPPED && r.state != ERROR
}

func (r *replicator) Leader() int {
	return r.leader
}

func (r *replicator) Outbox() <-chan ClientResponse {
	return r.outbox
}

func (r *replicator) Start() (err error) {
	if r.state == STOPPED {
		r.state = FOLLOWER

		r.server.Start()
		go r.monitorInternalInbox()
		go r.monitorInternalOutbox()
		go r.serve()
	}

	return
}

func (r *replicator) State() int8 {
	return r.state
}

func (r *replicator) Stop() (err error) {
	INFO.Println(fmt.Sprintf("Stopping replicator %d", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d has fully stopped", r.server.Pid()))

	if r.state != STOPPED && r.state != ERROR {
		r.stop <- true
		r.stop <- true

		r.server.Stop()

		r.internal_inbox <- &raft_msg{
			RPC:  TERMINATE,
			Addr: r.server.Pid(),
		}

		<-r.stopped
	}

	return
}

func (r *replicator) Term() int64 {
	return r.term
}

/*======================================< MONITOR ROUTINES >======================================*/

func (r *replicator) monitorInternalInbox() {
	INFO.Println(fmt.Sprintf("Inbox monitor for replicator %d started", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Inbox monitor for replicator %d stopped", r.server.Pid()))

	for {
		select {
		case env := <-r.server.Inbox():
			new_msg := env.Msg.(raft_msg)
			INFO.Println(fmt.Sprintf(" Replic %d <- %s", r.server.Pid(), new_msg.toString()))
			r.internal_inbox <- &new_msg
		case <-r.stop:
			return
		}
	}
}

func (r *replicator) monitorInternalOutbox() {
	INFO.Println(fmt.Sprintf("Outbox monitor for replicator %d started", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Outbox monitor for replicator %d stopped", r.server.Pid()))

	for {
		select {
		case msg := <-r.internal_outbox:
			INFO.Println(fmt.Sprintf(" Replic %d -> %s", r.server.Pid(), msg.toString()))
			target := msg.Addr
			msg.Addr = r.server.Pid()
			env := &cluster.Envelope{
				Pid:   target,
				MsgId: UNINITIALIZED,
				Msg:   msg,
			}
			r.server.Outbox() <- env
		case <-r.stop:
			return
		}
	}
}

/*=======================================< SERVE ROUTINES >========================================*/

func (r *replicator) serve() {
	INFO.Println(fmt.Sprintf("Serve routine for replicator %d started", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Serve routine for replicator %d stopped", r.server.Pid()))

	for {
		switch r.state {
		case CANDIDATE:
			r.serveAsCandidate()
		case FOLLOWER:
			r.serveAsFollower()
		case LEADER:
			r.serveAsLeader()
		case STOPPED:
			r.stopped <- true
			return
		}
	}
}

func (r *replicator) serveAsCandidate() {
	INFO.Println(fmt.Sprintf("Replicator %d is now serving as CANDIDATE", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d is no more a CANDIDATE", r.server.Pid()))

	r.state = CANDIDATE
	r.leader = UNINITIALIZED
	for r.state == CANDIDATE {
		// Increment the replicator's term & self-vote
		r.term++
		r.votes = 1
		r.voted_for = r.server.Pid()

		// Broadcast a RequestVote
		r.internal_outbox <- &raft_msg{
			RPC:          REQUEST_VOTE,
			Addr:         cluster.BROADCAST,
			Term:         r.term,
			PrevLogIndex: r.log.getPrevIndex(),
			PrevLogTerm:  r.log.getPrevTerm(),
		}

		timed_out := false
		for !timed_out && r.state == CANDIDATE {
			// Become leader if have majority votes
			if r.votes > len(r.server.Peers())/2 {
				r.state = LEADER
				break
			}

			select {
			case <-r.inbox:
				r.outbox <- ClientResponse{
					raft_resp: false,
					sm_resp:   nil,
				}

			case msg := <-r.internal_inbox:
				r.handleRaftMessage(msg)

			case <-time.After(r.election_timeout):
				timed_out = true
			}
		}
	}
}

func (r *replicator) serveAsFollower() {
	INFO.Println(fmt.Sprintf("Replicator %d is now serving as FOLLOWER", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d is no more a FOLLOWER", r.server.Pid()))

	r.state = FOLLOWER
	for r.state == FOLLOWER {
		select {
		case <-r.inbox:
			r.outbox <- ClientResponse{
				raft_resp: false,
				sm_resp:   nil,
			}

		case msg := <-r.internal_inbox:
			r.handleRaftMessage(msg)

		case <-time.After(r.election_timeout):
			r.state = CANDIDATE
		}
	}
}

func (r *replicator) serveAsLeader() {
	INFO.Println(fmt.Sprintf("Replicator %d is now serving as LEADER", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d is no more a LEADER", r.server.Pid()))

	r.state = LEADER
	r.leader = r.server.Pid()
	r.nextIndex = make(map[int]int)
	r.matchIndex = make(map[int]int)

	for _, peer := range r.server.Peers() {
		r.matchIndex[peer] = -1
		r.nextIndex[peer] = len(r.log.entries)
	}

	r.broadcastAppendEntries()

	for r.state == LEADER {
		select {
		case cmd := <-r.inbox:
			r.handleStateMachineCommand(cmd)

		case msg := <-r.internal_inbox:
			r.handleRaftMessage(msg)

		case <-time.After(r.heartbeat_interval):
			r.broadcastAppendEntries()
		}
	}
}

/*========================================< LEADER HELPERS >=========================================*/

func (r *replicator) broadcastAppendEntries() {
	INFO.Println(fmt.Sprintf("Replicator %d is now sending AEs.", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d has sent AEs.", r.server.Pid()))

	for _, peer := range r.server.Peers() {
		entries, pTerm := r.log.getEntriesAtAndAfter(r.nextIndex[peer])
		r.internal_outbox <- &raft_msg{
			RPC:               APPEND_ENTRIES,
			Addr:              peer,
			Term:              r.term,
			Leader:            r.leader,
			Entries:           entries,
			PrevLogTerm:       pTerm,
			PrevLogIndex:      r.nextIndex[peer] - 1,
			LeaderCommitIndex: r.log.commitIndex,
		}
	}
}

func (r *replicator) handleStateMachineCommand(cmd interface{}) {
	r.log.createAndAddNewEntry(cmd, r.term)

	if len(r.server.Peers()) == 0 {
		r.log.commitIndex = len(r.log.entries) - 1
		if r.machine != nil {
			r.outbox <- ClientResponse{
				raft_resp: true,
				sm_resp:   r.machine.Apply(cmd),
			}
		}
		r.log.lastApplied = r.log.commitIndex
	}
}

/*========================================< RPC HANDLERS >=========================================*/

func (r *replicator) handleRaftMessage(msg *raft_msg) {
	if msg.Term > r.term {
		r.term = msg.Term
		r.voted_for = UNINITIALIZED
		if r.state != FOLLOWER {
			r.state = FOLLOWER
			r.leader = UNINITIALIZED
		}
	}

	switch msg.RPC {
	case TERMINATE:
		r.handleTerminate(msg)

	case REQUEST_VOTE:
		r.handleRequestVote(msg)
	case REQUEST_VOTE_RESP:
		if r.state == CANDIDATE && msg.Result == true {
			r.votes++
		}

	case APPEND_ENTRIES:
		r.handleAppendEntries(msg)
	case APPEND_ENTRIES_RESP:
		if r.state == LEADER {
			if msg.Result == true {
				r.matchIndex[msg.Addr] = msg.PrevLogIndex
				r.nextIndex[msg.Addr] = r.matchIndex[msg.Addr] + 1

				synced := 0
				var minMatchIndex int = len(r.log.entries)
				for _, index := range r.matchIndex {
					if index > r.log.commitIndex {
						synced++
						if index < minMatchIndex {
							minMatchIndex = index
						}
					}
				}

				if synced > len(r.server.Peers())/2 && r.log.entries[minMatchIndex].Term == r.term {
					r.log.commitIndex = minMatchIndex
					if r.log.commitIndex > r.log.lastApplied && r.machine != nil {
						for i := r.log.lastApplied + 1; i <= r.log.commitIndex; i++ {
							r.outbox <- ClientResponse{
								raft_resp: true,
								sm_resp:   r.machine.Apply(r.log.entries[i].Data),
							}
						}
						r.log.lastApplied = r.log.commitIndex
					}
				}
			} else {
				r.nextIndex[msg.Addr]--
			}
		}
	}
}

func (r *replicator) handleTerminate(msg *raft_msg) {
	INFO.Println(fmt.Sprintf("Replicator %d has received a TERMINATE request.", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d is now STOPPED.", r.server.Pid()))

	r.state = STOPPED
	if msg.Addr != r.server.Pid() {
		EROR.Println("TERMINATE RPC received from another server! ABORTING")
	}
}

func (r *replicator) handleRequestVote(msg *raft_msg) {
	INFO.Println(fmt.Sprintf("Replicator %d has received a REQUEST_VOTE request from %d.", r.server.Pid(), msg.Addr))

	resp := &raft_msg{
		Term:   r.term,
		Addr:   msg.Addr,
		RPC:    REQUEST_VOTE_RESP,
		Result: false,
	}

	if msg.Term == r.term && r.voted_for == UNINITIALIZED && r.log.getPrevTerm() <= msg.PrevLogTerm && r.log.getPrevIndex() <= msg.PrevLogIndex {
		r.voted_for = msg.Addr
		resp.Result = true
	}

	r.internal_outbox <- resp
}

func (r *replicator) handleAppendEntries(msg *raft_msg) {
	INFO.Println(fmt.Sprintf("Replicator %d has received an APPEND_ENTRIES request from %d.", r.server.Pid(), msg.Addr))

	resp := &raft_msg{
		Term:   r.term,
		Addr:   msg.Addr,
		RPC:    APPEND_ENTRIES_RESP,
		Result: false,
	}

	if msg.Term == r.term {
		r.leader = msg.Leader
		if err := r.log.check(msg.PrevLogTerm, msg.PrevLogIndex); err == nil {
			resp.Result = true
			r.log.addEntries(msg.Entries)
			r.log.commitIndex = msg.LeaderCommitIndex
			if r.log.commitIndex > r.log.lastApplied && r.machine != nil {
				for i := r.log.lastApplied + 1; i <= r.log.commitIndex; i++ {
					r.outbox <- ClientResponse{
						raft_resp: true,
						sm_resp:   r.machine.Apply(r.log.entries[i].Data),
					}
				}
				r.log.lastApplied = r.log.commitIndex
			}
			resp.PrevLogIndex = len(r.log.entries) - 1
		}
	}

	r.internal_outbox <- resp
}
