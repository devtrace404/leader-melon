package main

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	_leaderHeartbeatSendInterval = time.Second * 2
	_electionTimeout             = 3 * _leaderHeartbeatSendInterval
)

type electionState string

const (
	Leader    electionState = "Leader"
	Candidate electionState = "Candidate"
	Follower  electionState = "Follower"
)

type healthState string

const (
	NoStarted healthState = "NotStarted"
	Running   healthState = "Running"
	Stopped   healthState = "Stopped"
)

const (
	_stopNodeHTTPRequestHeader = "request.stop.node"
)

type RaftNode interface {
	GetID() int
	GetCurrentTerm() int
	GetNodeState() healthState
	AmILeader() bool
	RegisterPeers(peers []RaftNode)
	StartNode()
	StartNodeAfter(duration time.Duration)
	StopNode()
	RecieveHeartbeat(leader int, term int)
	VotedForCandidate(id int, term int) bool
}

type Node struct {
	mu                              sync.RWMutex
	id                              int
	previousTerm                    int
	currentTerm                     int
	votes                           int
	electionState                   electionState
	healthState                     healthState
	peers                           []RaftNode
	leaderHearbeatChan              chan struct{}
	lastHeartbeatRecievedFromLeader time.Time
	isleaderHeartbeatChanClosed     bool
	leaderHeartbeatSendInterval     time.Duration
	electionTimeout                 time.Duration
}

func NewNode(id int) RaftNode {
	return &Node{
		id:                          id,
		previousTerm:                0,
		currentTerm:                 0,
		votes:                       0,
		mu:                          sync.RWMutex{},
		electionState:               Follower,
		healthState:                 NoStarted,
		peers:                       nil,
		leaderHeartbeatSendInterval: _leaderHeartbeatSendInterval,
		electionTimeout:             _electionTimeout,
	}
}

func (n *Node) GetID() int {
	// Node ID remains constant, no need to acquire lock
	return n.id
}

func (n *Node) GetCurrentTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.currentTerm
}

func (n *Node) AmILeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.electionState == Leader
}

func (n *Node) RegisterPeers(peers []RaftNode) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.peers = peers
}

func (n *Node) StartNode() {
	n.StartNodeAfter(0)
}

func (n *Node) StartNodeAfter(duration time.Duration) {
	if n.GetNodeState() == Running {
		return
	}

	time.Sleep(duration)

	n.mu.Lock()
	n.setNodeState(Running)
	n.mu.Unlock()

	ticker := time.NewTicker(n.electionTimeout)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				if n.shouldPromoteAsCandidate() {
					n.startNewElection()
				}
			}
		}
	}()
}

func (n *Node) StopNode() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.getNodeState() == Stopped {
		return
	}

	if n.electionState == Leader {
		n.stopRunningAsLeader()
	}

	n.setElectionState(Follower)
	n.setNodeState(Stopped)
}

func (n *Node) setNodeState(state healthState) {
	n.healthState = state
}

func (n *Node) setElectionState(state electionState) {
	n.electionState = state
}

func (n *Node) GetNodeState() healthState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.getNodeState()
}

func (n *Node) getNodeState() healthState {
	return n.healthState
}

func (n *Node) shouldPromoteAsCandidate() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.healthState == Running && n.electionState == Follower && time.Now().Sub(n.lastHeartbeatRecievedFromLeader) > n.electionTimeout
}

func (n *Node) startNewElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	fmt.Printf("Node %d starting new election\n", n.id)
	n.electionState = Candidate
	n.votes = 1
	n.currentTerm = n.currentTerm + 1

	for _, peer := range n.peers {
		if peer.VotedForCandidate(n.id, n.currentTerm) {
			n.votes++
		}
	}

	if n.votes > (len(n.peers)+1)/2 {
		n.electionState = Leader
		n.previousTerm = n.currentTerm
		go n.runAsLeader()
	} else {
		n.currentTerm = n.currentTerm - 1
		n.votes = 0
		n.electionState = Follower
	}
}

func (n *Node) runAsLeader() {
	fmt.Printf("Node %d running as a leader\n", n.id)
	ticker := time.NewTicker(n.leaderHeartbeatSendInterval)
	n.leaderHearbeatChan = make(chan struct{})
	n.isleaderHeartbeatChanClosed = false

	for {
		select {
		case <-n.leaderHearbeatChan:
			return
		case <-ticker.C:
			n.mu.Lock()
			n.previousTerm = n.currentTerm
			n.currentTerm++
			n.mu.Unlock()
			for _, peer := range n.peers {
				peer.RecieveHeartbeat(n.id, n.currentTerm)
			}
		}
	}
}

func (n *Node) RecieveHeartbeat(leader int, term int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.healthState != Running {
		return
	}

	fmt.Printf("Node: %d, recieved heartbeat from leader Node: %d in in term: %d\n", n.id, leader, term)
	n.lastHeartbeatRecievedFromLeader = time.Now()
	if term > n.currentTerm {
		n.previousTerm = n.currentTerm
		n.currentTerm = term

		if n.electionState == Leader {
			n.stopRunningAsLeader()
		}
	}
}

func (n *Node) stopRunningAsLeader() {
	if n.isleaderHeartbeatChanClosed {
		return
	}
	n.isleaderHeartbeatChanClosed = true
	close(n.leaderHearbeatChan)
}

func (n *Node) VotedForCandidate(id int, term int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.healthState != Running {
		return false
	}

	return term > n.currentTerm
}

func main() {
	numNodes := 4
	nodes := make([]RaftNode, numNodes)

	for i := 0; i < numNodes; i++ {
		nodes[i] = NewNode(i)
	}

	for i := 0; i < numNodes; i++ {
		peers := make([]RaftNode, 0)
		for _, node := range nodes {
			if nodes[i].GetID() != node.GetID() {
				peers = append(peers, node)
			}
		}

		nodes[i].RegisterPeers(peers)
	}

	for i := 0; i < numNodes; i++ {
		go nodes[i].StartNodeAfter(time.Duration(i*10) * time.Second)
	}

	http.HandleFunc("/debug/leader", func(w http.ResponseWriter, r *http.Request) {
		leaderExists := false
		for _, node := range nodes {
			if node.AmILeader() {
				leaderExists = true
				fmt.Fprintf(w, fmt.Sprintf("Node: %d, is leader of term: %d\n", node.GetID(), node.GetCurrentTerm()))
			}
		}
		if !leaderExists {
			fmt.Fprintf(w, "No leader found\n")
		}
	})

	http.HandleFunc("/nodes/stop", func(w http.ResponseWriter, r *http.Request) {
		stopNodeID := r.Header.Get(_stopNodeHTTPRequestHeader)
		if stopNodeID == "" {
			fmt.Fprintf(w, "Node ID must be defined in header key: %s\n", _stopNodeHTTPRequestHeader)
			return
		}

		nodeFound := false
		for _, node := range nodes {
			if strconv.Itoa(node.GetID()) == stopNodeID {
				nodeFound = true
				node.StopNode()
			}
		}

		if !nodeFound {
			fmt.Fprintf(w, "Node: %s not found\n", stopNodeID)
		}
	})

	http.HandleFunc("/nodes/start", func(w http.ResponseWriter, r *http.Request) {
		startNodeID := r.Header.Get(_stopNodeHTTPRequestHeader)
		if startNodeID == "" {
			fmt.Fprintf(w, "Node ID must be defined in header key: %s\n", _stopNodeHTTPRequestHeader)
			return
		}

		nodeFound := false
		for _, node := range nodes {
			if strconv.Itoa(node.GetID()) == startNodeID {
				nodeFound = true
				node.StartNode()
			}
		}

		if !nodeFound {
			fmt.Fprintf(w, "Node: %s not found\n", startNodeID)
		}
	})

	http.HandleFunc("/nodes/state", func(w http.ResponseWriter, r *http.Request) {
		for _, node := range nodes {
			fmt.Fprintf(w, fmt.Sprintf("Node: %d, state: %s\n", node.GetID(), node.GetNodeState()))
		}
	})

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic("failed to start the http server")
	}

	fmt.Println("http server started")

	time.Sleep(time.Minute * 2)
}
