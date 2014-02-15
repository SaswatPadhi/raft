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

## Usage

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


A new replicator can then be setup as below:

```go
replic := raft.NewReplicator(2732,          // Unique Pid of this replicator
                             "raft.json")   // raft configuration in JSON
```
It is essential that this replicator should have an entry in the cluster configuration file.
Because all communication that the replicator does, ultimately happen through the cluster.


<br> <br>
- - - - -


### Disclaimer ::

This project is **WIP** (work-in-progress), use at your own risk!

Many of the features might be partially implemented and would *not* been
thoroughly tested. The author shall not be held responsible for alien
invasions, nuclear catastrophe or extinction of chipmunks; arising due to
the use of this software.
