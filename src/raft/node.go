package main

import (
	"fmt"
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
	_stopNodeHTTPRequestHeader = "request.node.stop"
	_logSetHTTPRequestHeader   = "request.log.set"
	_logGetHTTPRequestHeader   = "request.log.get"
)

type RaftNode interface {
	GetID() int
	GetCurrentTerm() int
	GetNodeState() healthState
	GetElectionState() electionState
	AmILeader() bool
	SaveLog(log string) error
	GetLog(key int) (int, error)
	ListLog() (map[int]int, error)
	RegisterPeers(peers []RaftNode)
	StartNode()
	StartNodeAfter(duration time.Duration)
	StopNode()
	RecieveLog(leader int, term int, log string) error
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
	db                              Database
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
		db:                          NewDatabase(),
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

func (n *Node) GetElectionState() electionState {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.getElectionState()
}

func (n *Node) getElectionState() electionState {
	return n.electionState
}

func (n *Node) SaveLog(log string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.saveLog(log)
}

func (n *Node) saveLog(log string) error {
	err := n.fanOutLogs(log)
	if err != nil {
		return fmt.Errorf("failed to fan out logs: %w", err)
	}
	return n.parseAndSaveLog(log)
}

func (n *Node) parseAndSaveLog(log string) error {
	logType, err := ParseLogType(log)
	if err != nil {
		return fmt.Errorf("failed to parse log type of log: %w", err)
	}
	switch logType {
	case Set:
		setLog, err := ParseSetLog(log)
		if err != nil {
			return fmt.Errorf("failed to parse set log type: %w", err)
		}
		return n.db.Set(setLog.key, setLog.value)
	case Delete:
		deleteLog, err := ParseDeleteLog(log)
		if err != nil {
			return fmt.Errorf("failed to parse delete log type: %w", err)
		}
		return n.db.Delete(deleteLog.key)
	default:
		return fmt.Errorf("unsupported log type")
	}
}

func (n *Node) fanOutLogs(log string) error {
	var wg sync.WaitGroup
	var recievedSuccessCount int
	var allErrs error
	for _, peer := range n.peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := peer.RecieveLog(n.id, n.currentTerm, log)
			if err != nil {
				fmt.Printf("failed to send log %s to peer: %s in term: %d\n", log, peer.GetID(), n.currentTerm)
				allErrs = fmt.Errorf("%w; %w", allErrs, err)
			} else {
				recievedSuccessCount++
			}
		}()
	}

	wg.Wait()
	if recievedSuccessCount > int(len(n.peers)/2) {
		return nil
	} else {
		return allErrs
	}
}

func (n *Node) GetLog(key int) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.getLog(key)
}

func (n *Node) ListLog() (map[int]int, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.listLog()
}

func (n *Node) listLog() (map[int]int, error) {
	return n.db.List()
}

func (n *Node) getLog(key int) (int, error) {
	return n.db.Get(key)
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

func (n *Node) RecieveLog(leader int, term int, log string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.recieveLog(leader, term, log)
}

func (n *Node) recieveLog(leader int, term int, log string) error {
	if n.healthState != Running {
		return fmt.Errorf("node %d is not healhty", n.id)
	}

	fmt.Printf("Node: %d, recieved log: %s from leader Node: %d in in term: %d\n", n.id, log, leader, term)

	// TODO logic needs to be added when to accept/reject log

	return n.parseAndSaveLog(log)
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

// With Log Replication,
// When leader recieves log, it sends it out to all the nodes. Once marjority of the nodes have
// successfully written the log, leader saves it in the DB, else rejects the write

// If a follower is behind, it will reject the appendLogs until leader asynchronously sends the logs

// Also, leader election algorithm will change based on the fact that follower will vote for the candidate
// with logs index greater than theirs log index.
