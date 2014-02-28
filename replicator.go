package raft

import (
	"github.com/SaswatPadhi/cluster"

	"encoding/gob"
	"encoding/json"
	"io/ioutil"

	"bytes"
	"fmt"
	"log"
	"time"
)

const (
	UNINITIALIZED = iota
)

// Deviation Thresholds
const (
	Election_Timeout_Deviation   = 0.1
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

// Raft Commands
const (
	APPEND_ENTRIES = '0'
	PEER_VOTED     = '1'
	REQUEST_VOTE   = '2'
	TERMINATE      = '3'
)

type request struct {
	Addr    int
	Command byte
	Term    int64
}

func (req *request) toString() string {
	return fmt.Sprintf("{addr: %d,  comm: %s,  term: %d}", req.Addr, req.Command, req.Term)
}

type raft_config struct {
	Election_Timeout    int
	Heartbeat_Interval  int
	Cluster_Config_File string
}

// The interface that each replicator object must implement.
type Replicator interface {
	ElectionTimeout() time.Duration
	HeartbeatInterval() time.Duration
	IsLeader() bool
	IsRunning() bool
	Start()
	State() int8
	Stop()
	Term() int64
}

type replicator struct {
	election_timeout   time.Duration
	heartbeat_interval time.Duration
	inbox              chan *request
	leader             int
	outbox             chan *request
	server             *cluster.Server
	state              int8
	stop               chan bool
	stopped            chan bool
	stop_heartbeat     chan bool
	term               int64
	voted_for          int
}

func init() {
	gob.Register(request{})
}

func NewReplicator(id int, config_file string) (r *replicator, err error) {
	INFO.Println(fmt.Sprintf("Creating replicator [id: %d, config: %s]", id, config_file))
	defer INFO.Println(fmt.Sprintf("Replicator creation returns [err: %s]", err))

	r = &replicator{
		election_timeout:   UNINITIALIZED,
		heartbeat_interval: UNINITIALIZED,
		inbox:              make(chan *request, 32),
		leader:             UNINITIALIZED,
		outbox:             make(chan *request, 32),
		server:             nil,
		state:              ERROR,
		stop:               make(chan bool, 2),
		stopped:            make(chan bool),
		stop_heartbeat:     make(chan bool),
		term:               0,
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

func (r *replicator) IsLeader() bool {
	return r.server.Pid() == r.leader
}

func (r *replicator) IsRunning() bool {
	return r.state != STOPPED && r.state != ERROR
}

func (r *replicator) Start() {
	if r.state == STOPPED {
		r.state = FOLLOWER
		r.server.Start()

		go r.monitorInbox()
		go r.monitorOutbox()
		go r.serve()
	}
}

func (r *replicator) State() int8 {
	return r.state
}

func (r *replicator) Stop() {
	INFO.Println(fmt.Sprintf("Stopping replicator %d", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d has fully stopped", r.server.Pid()))

	if r.state != STOPPED && r.state != ERROR {

		// Stop the monitorinbox and monitoroutbox
		r.stop <- true
		r.stop <- true

		// Stop the underlying cluster server
		r.server.Stop()

		// Issue a TERMINATE command to be picked up by current serve routine
		r.inbox <- &request{
			Addr:    r.server.Pid(),
			Command: TERMINATE,
			Term:    r.term,
		}

		<-r.stopped
	}
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
			new_req := env.Msg.(request)
			INFO.Println(fmt.Sprintf(" Replic %d <- %s", r.server.Pid(), new_req.toString()))
			r.inbox <- &new_req
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
		case req := <-r.outbox:
			INFO.Println(fmt.Sprintf(" Replic %d -> %s", r.server.Pid(), req.toString()))
			target := req.Addr
			req.Addr = r.server.Pid()
			env := &cluster.Envelope{
				Pid:   target,
				MsgId: UNINITIALIZED,
				Msg:   req,
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
		votes_in_for := 1
		r.voted_for = r.server.Pid()

		// Broadcast a RequestVote
		r.outbox <- &request{
			Addr:    cluster.BROADCAST,
			Command: REQUEST_VOTE,
			Term:    r.term,
		}

		timed_out := false
		for !timed_out && r.state == CANDIDATE {
			// Become leader if have majority votes
			if votes_in_for > len(r.server.Peers())/2 {
				r.state = LEADER
				break
			}

			select {
			case req := <-r.inbox:
				if req.Command == TERMINATE {
					r.handleTerminate(req)
				} else if req.Command == REQUEST_VOTE {
					r.handleRequestVote(req)
				} else if req.Command == APPEND_ENTRIES {
					if r.handleAppendEntries(req) {
						r.state = FOLLOWER
					}
				} else if req.Command == PEER_VOTED {
					votes_in_for++
				}

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
		case req := <-r.inbox:
			if req.Command == TERMINATE {
				r.handleTerminate(req)
			} else if req.Command == REQUEST_VOTE {
				if r.handleRequestVote(req) {
					r.outbox <- &request{
						Addr:    req.Addr,
						Command: PEER_VOTED,
						Term:    r.term,
					}
				}
			} else if req.Command == APPEND_ENTRIES {
				r.handleAppendEntries(req)
			}

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
		case req := <-r.inbox:
			if req.Command == TERMINATE {
				r.stop_heartbeat <- true
				r.handleTerminate(req)
			} else if req.Command == APPEND_ENTRIES {
				r.handleAppendEntries(req)
			} else if req.Command == REQUEST_VOTE {
				if r.handleRequestVote(req) {
					r.outbox <- &request{
						Addr:    req.Addr,
						Command: PEER_VOTED,
						Term:    r.term,
					}
					r.state = FOLLOWER
				}
			}
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
			r.outbox <- &request{
				Addr:    cluster.BROADCAST,
				Command: APPEND_ENTRIES,
				Term:    r.term,
			}
		}
	}
}

func (r *replicator) handleTerminate(req *request) {
	INFO.Println(fmt.Sprintf("Replicator %d has received a TERMINATE request.", r.server.Pid()))
	defer INFO.Println(fmt.Sprintf("Replicator %d is now STOPPED.", r.server.Pid()))

	r.state = STOPPED
	if req.Addr != r.server.Pid() {
		log.Panicln("TERMINATE command received from another server! ABORTING")
	}
}

func (r *replicator) handleRequestVote(req *request) bool {
	INFO.Println(fmt.Sprintf("Replicator %d has received a REQUEST_VOTE request.", r.server.Pid()))

	if req.Term < r.term {
		return false
	}

	r.term = req.Term

	if r.voted_for != UNINITIALIZED {
		return false
	}
	r.voted_for = req.Addr

	return true
}

func (r *replicator) handleAppendEntries(req *request) bool {
	INFO.Println(fmt.Sprintf("Replicator %d has received an APPEND_ENTRIES request.", r.server.Pid()))

	if req.Term < r.term {
		return false
	}

	r.term = req.Term
	r.leader = req.Addr

	return true
}
