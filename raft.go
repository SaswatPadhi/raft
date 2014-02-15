/*
	raft -- An implementation of the Raft Consensus protocol in Go.
	(https://github.com/SaswatPadhi/raft)

	Raft is one of the consensus algorithms which was designed with a goal of
	making it _understandable_. Using the Raft protocol, a cluster of nodes can
	maintain a replicated state machine, which is kept in sync using a replicated
	log. The original paper on Raft could be found @
	https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf

	The `raft` project is built on the [cluster](https://github.com/SaswatPadhi/cluster)
	project.
*/

package raft
