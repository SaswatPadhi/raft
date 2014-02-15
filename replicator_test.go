package raft

import (
	"github.com/SaswatPadhi/cluster"

	"encoding/json"
	"io/ioutil"

	"bytes"
	"io"
	"testing"
	"time"
)

const (
	CONFIG_FILE = "raft.json"
)

func RaftSetup(t *testing.T, start bool) []Replicator {
	data, err := ioutil.ReadFile(CONFIG_FILE)
	if err != nil {
		t.Fatal(err.Error())
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	replicator_config := raft_config{}
	if err = decoder.Decode(&replicator_config); err != nil {
		t.Error(err)
	}

	data, err = ioutil.ReadFile(replicator_config.Cluster_Config_File)
	replics := make([]Replicator, 0, bytes.Count(data, []byte{10})+1)
	decoder = json.NewDecoder(bytes.NewReader(data))

	for {
		var p cluster.Peer
		if err := decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		replic, err := NewReplicator(p.Pid, CONFIG_FILE)
		if err != nil {
			t.Error(err)
		} else {
			replics = append(replics, replic)
			if start {
				replic.Start()
			}
		}
	}

	return replics
}

func RaftTearDown(t *testing.T, replics []Replicator) {
	for _, replic := range replics {
		replic.Stop()
	}
}

// TEST: Checks if a Raft cluster could be brought up and down successfully.
func Test_RaftInitialize(t *testing.T) {
	RaftTearDown(t, RaftSetup(t, false))
}

// TEST: Checks if a Raft cluster could be brought up and down successfully after it has been started.
func Test_RaftInitializeAndStart(t *testing.T) {
	RaftTearDown(t, RaftSetup(t, true))
}

// TEST: Leader elected after finite time
func Test_LeaderElectionInFiniteTime(t *testing.T) {
	replics := RaftSetup(t, true)
	defer RaftTearDown(t, replics)

	leader_found := false
	max_iterations_to_monitor := len(replics) * len(replics)

	for i := 0; i <= max_iterations_to_monitor && !leader_found; i++ {
		<-time.After(replics[0].HeartbeatInterval())

		for _, replic := range replics {
			if replic.State() == LEADER {
				leader_found = true
				break
			}
		}
	}

	if !leader_found {
		t.Fatalf("No leader found.")
	}
}

// TEST: Leader election with minority failures
func Test_LeaderElectionWithMinorityFailures(t *testing.T) {
	replics := RaftSetup(t, false)
	defer RaftTearDown(t, replics)

	var i int
	leader_found := false
	max_iterations_to_monitor := len(replics) * len(replics)

	for i = 0; i < len(replics)/2; i++ {
		replics[i].Stop()
	}
	for ; i < len(replics); i++ {
		replics[i].Start()
	}

	for i = 0; i <= max_iterations_to_monitor && !leader_found; i++ {
		<-time.After(replics[0].HeartbeatInterval())

		for _, replic := range replics {
			if replic.State() == LEADER {
				leader_found = true
				break
			}
		}
	}

	if !leader_found {
		t.Fatalf("No leader found.")
	}
}

// TEST: Ensure only one leader exists
func Test_SingleLeaderElectedInFiniteTime(t *testing.T) {
	replics := RaftSetup(t, false)
	defer RaftTearDown(t, replics)

	leader_found := false
	max_iterations_to_monitor := len(replics) * len(replics)

	start_iters := make([]int, len(replics))
	for i := 0; i < len(replics); i++ {
		start_iters[i] = randInt(0, 2*len(replics))
	}

	for i := 0; i <= max_iterations_to_monitor; i++ {
		<-time.After(replics[0].HeartbeatInterval())

		for j := 0; j < len(replics); j++ {
			if start_iters[j] == i {
				replics[j].Start()
			}
		}

		leader_found = false
		for _, replic := range replics {
			if replic.State() == LEADER {
				// Check that there's only 1 leader
				if !leader_found {
					leader_found = true
				} else {
					t.Fatalf("Two leaders found.")
				}
			}
		}
	}

	if !leader_found {
		t.Fatalf("No leader found.")
	}
}

// TEST: Leader election with minority failures
func Test_SingleLeaderElectedWithMinorityFailures(t *testing.T) {
	replics := RaftSetup(t, false)
	defer RaftTearDown(t, replics)

	var i int
	leader_found := false
	max_iterations_to_monitor := len(replics) * len(replics)

	stop_iters := make([]int, len(replics))
	start_iters := make([]int, len(replics))

	for i = 0; i < len(replics)/2; i++ {
		start_iters[i] = randInt(len(replics), 2*len(replics))
		stop_iters[i] = randInt(2*len(replics), 3*len(replics))
	}
	for ; i < len(replics); i++ {
		start_iters[i] = randInt(0, len(replics))
		stop_iters[i] = -1
	}

	for i = 0; i <= max_iterations_to_monitor; i++ {
		<-time.After(replics[0].HeartbeatInterval())

		for j := 0; j < len(replics); j++ {
			if start_iters[j] == i {
				replics[j].Start()
			}
			if stop_iters[j] == i {
				replics[j].Stop()
			}
		}

		leader_found = false
		for _, replic := range replics {
			if replic.State() == LEADER {
				// Check that there's only 1 leader
				if !leader_found {
					leader_found = true
				} else {
					t.Fatalf("Two leaders found.")
				}
			}
		}
	}

	if !leader_found {
		t.Fatalf("No leader found.")
	}
}

// TEST: Leader election with majority failures
func Test_LeaderElectionWithMajorityFailures(t *testing.T) {
	replics := RaftSetup(t, false)
	defer RaftTearDown(t, replics)

	var i int
	max_iterations_to_monitor := len(replics) * len(replics)

	for i = 0; i <= len(replics)/2; i++ {
		replics[i].Stop()
	}
	for ; i < len(replics); i++ {
		replics[i].Start()
	}

	for i = 0; i <= max_iterations_to_monitor; i++ {
		<-time.After(replics[0].HeartbeatInterval())

		for _, replic := range replics {
			if replic.State() == LEADER {
				t.Fatalf("Leader found with majority failures.")
			}
		}
	}
}