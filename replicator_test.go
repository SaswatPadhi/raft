package raft

import (
	"github.com/SaswatPadhi/cluster"

	"encoding/json"
	"io/ioutil"

	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

const (
	CONFIG_FILE = "raft.json"

	MAX_N2_ITERATIONS = 4
)

/*=======================================< HELPER ROUTINES >=======================================*/

func RaftSetup(t *testing.T, sm StateMachine, do_start bool) []Replicator {
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

		replic, err := NewReplicator(p.Pid, sm, CONFIG_FILE)
		if err != nil {
			t.Error(err)
		} else {
			replics = append(replics, replic)
		}
	}

	if do_start {
		RaftStart(t, replics)
	}
	return replics
}

func RaftStart(t *testing.T, replics []Replicator) {
	for _, replic := range replics {
		if err := replic.Start(); err != nil {
			t.Error(err)
		}
	}
}

func RaftStop(t *testing.T, replics []Replicator) {
	for _, replic := range replics {
		if err := replic.Stop(); err != nil {
			t.Error(err)
		}
	}
}

/*========================================< TEST ROUTINES >========================================*/

// TEST: Checks if a Raft cluster could be brought up and down successfully.
func Test_RaftInitialize(t *testing.T) {
	RaftStop(t, RaftSetup(t, nil, false))
}

// TEST: Checks if a Raft cluster could be brought up and down successfully after it has been started.
func Test_RaftInitializeAndStart(t *testing.T) {
	RaftStop(t, RaftSetup(t, nil, true))
}

// TEST: Checks if a Raft cluster with the same config could be brought up and down successfully multiple times.
func Test_RaftMultipleUpDown(t *testing.T) {
	for i := 0; i < 64; i++ {
		RaftStop(t, RaftSetup(t, nil, true))
	}
}

// TEST: Leader elected after finite time
func Test_LeaderElectionInFiniteTime(t *testing.T) {
	replics := RaftSetup(t, nil, true)
	defer RaftStop(t, replics)

	leader_found := false
	max_iterations_to_monitor := MAX_N2_ITERATIONS * len(replics) * len(replics)

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
	replics := RaftSetup(t, nil, false)
	defer RaftStop(t, replics)

	var i int
	leader_found := false
	max_iterations_to_monitor := MAX_N2_ITERATIONS * len(replics) * len(replics)

	for i = len(replics) / 2; i < len(replics); i++ {
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
	replics := RaftSetup(t, nil, false)
	defer RaftStop(t, replics)

	leader_found := false
	max_iterations_to_monitor := MAX_N2_ITERATIONS * len(replics) * len(replics)

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

// TEST: Ensure only single leader with minority failures
func Test_SingleLeaderElectedWithMinorityFailures(t *testing.T) {
	replics := RaftSetup(t, nil, false)
	defer RaftStop(t, replics)

	var i int
	leader_found := false
	max_iterations_to_monitor := MAX_N2_ITERATIONS * len(replics) * len(replics)

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

// TEST: Ensure only single leader after temporary partitioning
func Test_SingleLeaderElectedAfterTempPartitioning(t *testing.T) {
	replics := RaftSetup(t, nil, true)
	defer RaftStop(t, replics)

	var i int
	leader_found := false
	max_iterations_to_monitor := MAX_N2_ITERATIONS * len(replics) * len(replics)

	stop_iters := make([]int, len(replics)/2)
	start_iters := make([]int, len(replics)/2)

	for i = 0; i < len(replics)/2; i++ {
		stop_iters[i] = randInt(len(replics), 2*len(replics))
		start_iters[i] = randInt(2*len(replics), 5*len(replics))
	}
	active_partition := replics[0].(*replicator).server.Peers()[(len(replics)-1)/2:]

	for i = 0; i <= max_iterations_to_monitor; i++ {
		<-time.After(replics[0].HeartbeatInterval())

		for j := 0; j < len(replics)/2; j++ {
			if start_iters[j] == i {
				replics[j].(*replicator).server.Blacklist(active_partition)
			}
			if stop_iters[j] == i {
				replics[j].(*replicator).server.Blacklist(nil)
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
	replics := RaftSetup(t, nil, false)
	defer RaftStop(t, replics)

	var i int
	max_iterations_to_monitor := MAX_N2_ITERATIONS * len(replics) * len(replics)

	for i = 1 + (len(replics) / 2); i < len(replics); i++ {
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

/*=======================================< KV STORE >=======================================*/

type KVMachine struct {
	dict map[string]string
}

type KVMachineResp struct {
	status string
	result string
}

func (kvm *KVMachine) Apply(cmd interface{}) interface{} {
	resp := &KVMachineResp{
		status: "error",
		result: "",
	}

	switch cmd.(type) {
	case string:
		parts := strings.Split(cmd.(string), " ")
		switch parts[0] {
		case "GET":
			if len(parts) > 1 {
				val, found := kvm.dict[parts[1]]
				if found {
					resp.status = "good"
					resp.result = val
				} else {
					resp.status = "bad"
				}
			}
		case "SET":
			if len(parts) > 2 {
				kvm.dict[parts[1]] = parts[2]
				resp.status = "good"
			}
		}
	}

	return resp
}

func Test_KVMachine(t *testing.T) {
	replics := RaftSetup(t, &KVMachine{dict: make(map[string]string)}, true)
	defer RaftStop(t, replics)

	var leader Replicator = nil
	max_iterations_to_monitor := MAX_N2_ITERATIONS * len(replics) * len(replics)

	for i := 0; i <= max_iterations_to_monitor && leader == nil; i++ {
		<-time.After(replics[0].HeartbeatInterval())

		for _, replic := range replics {
			if replic.State() == LEADER {
				leader = replic
				break
			}
		}
	}

	if leader == nil {
		t.Fatalf("No leader found.")
	}

	var req string
	var resp ClientResponse

	req = "SET Apple Fruit"
	leader.Inbox() <- req
	select {
	case <-time.After(replics[0].HeartbeatInterval() * time.Duration(len(replics)*MAX_N2_ITERATIONS)):
		t.Fatalf("Time out waiting for leader's response!")

	case resp = <-leader.Outbox():
		if !resp.raft_resp {
			t.Errorf("Leader ignored request!")
		} else {
			synced := 0
			for _, replic := range replics {
				if len(replic.(*replicator).log.entries) > 0 && replic.(*replicator).log.entries[0].Data.(string) == req {
					synced++
				}
			}

			if synced < (len(replics)+1)/2 {
				t.Errorf("Log not replicated on majority, but committed on leader!")
			}

			if resp.sm_resp.(*KVMachineResp).status != "good" {
				t.Errorf("KVMachine did not return good on SET.")
			}
		}
	}

	req = "SET Rose Flower"
	leader.Inbox() <- req
	select {
	case <-time.After(replics[0].HeartbeatInterval() * time.Duration(len(replics)*MAX_N2_ITERATIONS)):
		t.Fatalf("Time out waiting for leader's response!")

	case resp = <-leader.Outbox():
		if !resp.raft_resp {
			t.Errorf("Leader ignored request!")
		} else {
			synced := 0
			for _, replic := range replics {
				if len(replic.(*replicator).log.entries) > 1 && replic.(*replicator).log.entries[1].Data.(string) == req {
					synced++
				}
			}

			if synced < (len(replics)+1)/2 {
				t.Errorf("Log not replicated on majority, but committed on leader!")
			}

			if resp.sm_resp.(*KVMachineResp).status != "good" {
				t.Errorf("KVMachine did not return good on SET.")
			}
		}
	}

	req = "GET Apple"
	leader.Inbox() <- req
	select {
	case <-time.After(replics[0].HeartbeatInterval() * time.Duration(len(replics)*MAX_N2_ITERATIONS)):
		t.Fatalf("Time out waiting for leader's response!")

	case resp = <-leader.Outbox():
		if !resp.raft_resp {
			t.Errorf("Leader ignored request!")
		} else {
			synced := 0
			for _, replic := range replics {
				if len(replic.(*replicator).log.entries) > 2 && replic.(*replicator).log.entries[2].Data.(string) == req {
					synced++
				}
			}

			if synced < (len(replics)+1)/2 {
				t.Errorf("Log not replicated on majority, but committed on leader!")
			}

			if resp.sm_resp.(*KVMachineResp).status != "good" {
				t.Errorf("KVMachine did not return good on GET.")
			}

			if resp.sm_resp.(*KVMachineResp).result != "Fruit" {
				t.Errorf("KVMachine returned wrong value for key Apple.")
			}
		}
	}
}
