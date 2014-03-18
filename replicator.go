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
	IsRunning() bool
	Leader() int
	Start() error
	State() int8
	Stop() error
	Term() int64
}

type replicator struct {
	election_timeout   time.Duration
	heartbeat_interval time.Duration
	inbox              chan *raft_msg
	leader             int
	outbox             chan *raft_msg
	server             *cluster.Server
	state              int8
	stop               chan bool
	stopped            chan bool
	stop_heartbeat     chan bool
	term               int64
	votes              int
	voted_for          int
}

func NewReplicator(id int, config_file string) (r *replicator, err error) {
	INFO.Println(fmt.Sprintf("Creating replicator [id: %d, config: %s]", id, config_file))
	defer INFO.Println(fmt.Sprintf("Replicator creation returns [err: %s]", err))

	r = &replicator{
		election_timeout:   UNINITIALIZED,
		heartbeat_interval: UNINITIALIZED,
		inbox:              make(chan *raft_msg, 32),
		leader:             UNINITIALIZED,
		outbox:             make(chan *raft_msg, 32),
		server:             nil,
		state:              ERROR,
		stop:               make(chan bool, 2),
		stopped:            make(chan bool),
		stop_heartbeat:     make(chan bool),
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

func (r *replicator) Leader() int {
	return r.leader
}

func (r *replicator) IsRunning() bool {
	return r.state != STOPPED && r.state != ERROR
}

func (r *replicator) Start() (err error) {
	if r.state == STOPPED {
		r.state = FOLLOWER
		r.server.Start()

		go r.monitorInbox()
		go r.monitorOutbox()
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
		// Stop the monitorinbox and monitoroutbox
		r.stop <- true
		r.stop <- true

		// Stop the underlying cluster server
		r.server.Stop()

		// Issue a TERMINATE command to be picked up by current serve routine
		r.inbox <- &raft_msg{
			Command: TERMINATE,
			Addr:    r.server.Pid(),
		}

		<-r.stopped
	}

	return
}

func (r *replicator) Term() int64 {
	return r.term
}

func (r *replicator) monitorInbox() {
	INFO.Println(fmt.Sprintf("Inbox monitor for replicator %d started", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Inbox monitor for replicator %d stopped", r.server.Pid()))

	for {
		select {
		case env := <-r.server.Inbox():
			new_msg := env.Msg.(raft_msg)
			INFO.Println(fmt.Sprintf(" Replic %d <- %s", r.server.Pid(), new_msg.toString()))
			r.inbox <- &new_msg
		case <-r.stop:
			return
		}
	}
}

func (r *replicator) monitorOutbox() {
	INFO.Println(fmt.Sprintf("Outbox monitor for replicator %d started", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Outbox monitor for replicator %d stopped", r.server.Pid()))

	for {
		select {
		case msg := <-r.outbox:
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
		r.outbox <- &raft_msg{
			Command: REQUEST_VOTE,
			Addr:    cluster.BROADCAST,
			Term:    r.term,
		}

		timed_out := false
		for !timed_out && r.state == CANDIDATE {
			// Become leader if have majority votes
			if r.votes > len(r.server.Peers())/2 {
				r.state = LEADER
				break
			}

			select {
			case msg := <-r.inbox:
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
		case msg := <-r.inbox:
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

	go r.heartbeat()

	for r.state == LEADER {
		select {
		case msg := <-r.inbox:
			r.handleRaftMessage(msg)
		}
	}
}

func (r *replicator) heartbeat() {
	INFO.Println(fmt.Sprintf("Replicator %d is now sending heartbeats.", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d is not sending heartbeats any more.", r.server.Pid()))

	for {
		select {
		case <-r.stop_heartbeat:
			return
		case <-time.After(r.heartbeat_interval):
			r.outbox <- &raft_msg{
				Command: APPEND_ENTRIES,
				Addr:    cluster.BROADCAST,
				Term:    r.term,
				Leader:  r.leader,
			}
		}
	}
}

func (r *replicator) handleRaftMessage(msg *raft_msg) {
	if msg.Term > r.term {
		r.term = msg.Term
		r.state = FOLLOWER
		r.voted_for = UNINITIALIZED
	}

	switch msg.Command {
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
	}
}

func (r *replicator) handleTerminate(msg *raft_msg) {
	INFO.Println(fmt.Sprintf("Replicator %d has received a TERMINATE request.", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d is now STOPPED.", r.server.Pid()))

	r.state = STOPPED
	if msg.Addr != r.server.Pid() {
		EROR.Println("TERMINATE command received from another server! ABORTING")
	}
}

func (r *replicator) handleRequestVote(msg *raft_msg) {
	INFO.Println(fmt.Sprintf("Replicator %d has received a REQUEST_VOTE request from %d.", r.server.Pid(), msg.Addr))

	resp := &raft_msg{
		Term:    r.term,
		Addr:    msg.Addr,
		Command: REQUEST_VOTE_RESP,
		Result:  false,
	}

	if msg.Term == r.term && r.voted_for == UNINITIALIZED {
		r.voted_for = msg.Addr
		resp.Result = true
	}

	r.outbox <- resp
}

func (r *replicator) handleAppendEntries(msg *raft_msg) {
	INFO.Println(fmt.Sprintf("Replicator %d has received an APPEND_ENTRIES request from %d.", r.server.Pid(), msg.Addr))

	resp := &raft_msg{
		Term:    r.term,
		Addr:    msg.Addr,
		Command: APPEND_ENTRIES_RESP,
		Result:  false,
	}

	if msg.Term == r.term {
		resp.Result = true
	}

	r.outbox <- resp
}
