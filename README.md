raft
====

An implementation of the Raft Consensus algorithm in Go.

Raft is one of the consensus algorithms which was designed with a goal of making it
_understandable_. Using the Raft protocol, a cluster of nodes can maintain a replicated
state machine, which is kept in sync using a replicated log. The original paper on Raft
could be found @ [In the Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)

This project, `raft` is yet another attempt at implementing the `Raft` algorithm. There
have been numerous attempts at implementing the same and many of the good ones have been
cataloged @ [http://raftconsensus.github.io/](http://raftconsensus.github.io/).

The `raft` project is built on the [`cluster`](https://github.com/SaswatPadhi/cluster)
project.

## Install

```go
  go get github.com/SaswatPadhi/raft
  go test github.com/SaswatPadhi/raft
```

## Status

[&#x2714;] Leader Election  
[&#x2714;] Log Replication  
[&#x2717;] Log Compaction  
[&#x2714;] Unit Tests  


## Usage

### Configuration

`raft` accepts configurations as JSON objects which can be provided while setting up the
replicator. A typical raft configuration file would look something like:

```json
{
    "Election_Timeout": 400,
    "Heartbeat_Interval": 250,
    "Cluster_Config_File": "cluster.json"
}
```
The configuration file essentially indicates the values of various timeouts and the
relative location of the cluster configuration file. (For details on cluster configuration,
check [https://github.com/SaswatPadhi/cluster#usage](https://github.com/SaswatPadhi/cluster#usage))


### Replicators

A new replicator can then be setup as below:

```go
replic := raft.NewReplicator(2732,          // Unique Pid of this replicator
                             smachine,      // Pointer to a state machine (can be nil)
                             "raft.json")   // raft configuration in JSON
```
It is essential that this replicator should have an entry in the cluster configuration file.
Because all communication that the replicator does, ultimately happen through the cluster.

To create an abstract replicator (which would simply ignore all log related functionality),
one could pass nil as the second argument (for state machine pointer). For full raft
functionality, a concrete state machine instance must be passed to the replicator (which has been
demonstrated below).

### State Machines

Creating a state machine that can interact with a raft replicator is quite simple. The
state machine should adhere to the following API for raft to successfully communicate and
drive it:

```go
type StateMachine interface {
	Apply(interface{}) interface{}
}
```

Clearly, the state machine just needs to export one function for raft: `Apply`, which would accept
and return arbitrary structures.

An example of a distributed KV Store over raft has been demonstrated @ [`replicator_test.go#301`](https://github.com/SaswatPadhi/raft/blob/master/replicator_test.go#L301)

### Communication

After setting up a raft cluster, a client can send state machine commands to the raft layer which
after sufficient replication, would be applied to the leader's state machine and the result would
be returned to the client.

Two channels `Inbox()` and `Outbox()` enable this communcation to-&amp;from- clients.
A client can queue up state machine commands on any replicator (though only leader would actually
do anything useful) by sending data to the `Inbox()` channel of the replicator.
After replication and execution, the result would be sent to the `Outbox()` channel of the same
replicator, which the client can monitor.

=================

### Disclaimer ::

This project is **WIP** (work-in-progress), use at your own risk!

Many of the features might be partially implemented and would *not* been
thoroughly tested. The author shall not be held responsible for alien
invasions, nuclear catastrophe or extinction of chipmunks; arising due to
the use of this software.
