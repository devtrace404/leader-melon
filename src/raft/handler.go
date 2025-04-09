package main

import (
	"fmt"
	"net/http"
	"strconv"
)

type handler struct {
	cluster []RaftNode
}

func NewHandler(cluster []RaftNode) *handler {
	return &handler{
		cluster: cluster,
	}
}

func (h *handler) RegisterAllHandlers() {
	http.HandleFunc("/debug/leader", h.debugLeader)
	http.HandleFunc("/nodes/stop", h.stopNode)
	http.HandleFunc("/nodes/start", h.startNode)
	http.HandleFunc("/nodes/state", h.nodesState)
	http.HandleFunc("/log/set", h.logSet)
	http.HandleFunc("/log/get", h.logGet)
	http.HandleFunc("/log/list", h.logList)
}

func (h *handler) debugLeader(w http.ResponseWriter, r *http.Request) {
	leaderExists := false
	for _, node := range h.cluster {
		if node.AmILeader() {
			leaderExists = true
			fmt.Fprintf(w, fmt.Sprintf("Node: %d, is leader of term: %d\n", node.GetID(), node.GetCurrentTerm()))
		}
	}
	if !leaderExists {
		fmt.Fprintf(w, "No leader found\n")
	}
}

func (h *handler) stopNode(w http.ResponseWriter, r *http.Request) {
	stopNodeID := r.Header.Get(_stopNodeHTTPRequestHeader)
	if stopNodeID == "" {
		fmt.Fprintf(w, "Node ID must be defined in header key: %s\n", _stopNodeHTTPRequestHeader)
		return
	}

	nodeFound := false
	for _, node := range h.cluster {
		if strconv.Itoa(node.GetID()) == stopNodeID {
			nodeFound = true
			node.StopNode()
		}
	}

	if !nodeFound {
		fmt.Fprintf(w, "Node: %s not found\n", stopNodeID)
	}
}

func (h *handler) startNode(w http.ResponseWriter, r *http.Request) {
	startNodeID := r.Header.Get(_stopNodeHTTPRequestHeader)
	if startNodeID == "" {
		fmt.Fprintf(w, "Node ID must be defined in header key: %s\n", _stopNodeHTTPRequestHeader)
		return
	}

	nodeFound := false
	for _, node := range h.cluster {
		if strconv.Itoa(node.GetID()) == startNodeID {
			nodeFound = true
			node.StartNode()
		}
	}

	if !nodeFound {
		fmt.Fprintf(w, "Node: %s not found\n", startNodeID)
	}
}

func (h *handler) nodesState(w http.ResponseWriter, r *http.Request) {
	for _, node := range h.cluster {
		fmt.Fprintf(w, fmt.Sprintf("Node: %d, state: %s\n", node.GetID(), node.GetNodeState()))
	}
}

func (h *handler) logSet(w http.ResponseWriter, r *http.Request) {
	logHeader := r.Header.Get(_logSetHTTPRequestHeader)
	if logHeader == "" {
		http.Error(w, fmt.Sprintf("http header: %s is missing or not set", _logSetHTTPRequestHeader), http.StatusBadRequest)
		return
	}
	leader, err := GetClusterLeaderNode(h.cluster)
	if err != nil {
		http.Error(w, fmt.Sprintf("no leader found: %w", err.Error()), http.StatusInternalServerError)
		return
	}

	_, err = ParseLogType(logHeader)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid log format: %s", err.Error()), http.StatusBadRequest)
		return
	}

	err = leader.SaveLog(logHeader)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to save log: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, fmt.Sprintf("successfully submitted log\n"))
}

func (h *handler) logGet(w http.ResponseWriter, r *http.Request) {
	keyHeader := r.Header.Get(_logGetHTTPRequestHeader)
	if keyHeader == "" {
		http.Error(w, fmt.Sprintf("http header:%s is missing or not set", _logGetHTTPRequestHeader), http.StatusBadRequest)
		return
	}
	key, err := strconv.Atoi(keyHeader)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid http header:%s, value must be integer", _logGetHTTPRequestHeader), http.StatusBadRequest)
		return
	}

	leader, err := GetClusterLeaderNode(h.cluster)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get cluster leader: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	value, err := leader.GetLog(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get log: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, fmt.Sprintf("key: %d, value: %d\n", key, value))
}

func (h *handler) logList(w http.ResponseWriter, r *http.Request) {
	leader, err := GetClusterLeaderNode(h.cluster)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get cluster leader: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	data, err := leader.ListLog()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to list log: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	for key, val := range data {
		fmt.Fprintf(w, fmt.Sprintf("key: %d, value: %d\n", key, val))
	}
}
