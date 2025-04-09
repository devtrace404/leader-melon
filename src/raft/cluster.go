package main

import (
	"fmt"
	"time"
)

func RunCluster(numNodes int) []RaftNode {
	cluster := make([]RaftNode, numNodes)

	for i := 0; i < numNodes; i++ {
		cluster[i] = NewNode(i)
	}

	for i := 0; i < numNodes; i++ {
		peers := make([]RaftNode, 0)
		for _, node := range cluster {
			if cluster[i].GetID() != node.GetID() {
				peers = append(peers, node)
			}
		}

		cluster[i].RegisterPeers(peers)
	}

	for i := 0; i < numNodes; i++ {
		go cluster[i].StartNodeAfter(time.Duration(i*10) * time.Second)
	}

	return cluster
}

func GetClusterLeaderNode(cluster []RaftNode) (RaftNode, error) {
	for _, node := range cluster {
		if node.GetElectionState() == Leader {
			return node, nil
		}
	}
	return nil, fmt.Errorf("no leader found")
}
