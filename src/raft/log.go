package main

import (
	"fmt"
	"strconv"
	"strings"
)

type LogType string

const (
	Set     LogType = "Set"
	Delete  LogType = "Delete"
	Unknown LogType = "Unknown"
)

func ParseLogType(rawLog string) (LogType, error) {
	splits := strings.Split(rawLog, " ")
	if len(splits) == 3 && splits[0] == string(Set) {
		if _, err := strconv.Atoi(splits[1]); err != nil {
			return Unknown, fmt.Errorf("failed to convert set log key: %s to integer", splits[1])
		}
		if _, err := strconv.Atoi(splits[2]); err != nil {
			return Unknown, fmt.Errorf("failed to convert set log value: %s to integer", splits[2])
		}
		return Set, nil
	}

	if len(splits) == 2 && splits[0] == string(Delete) {
		if _, err := strconv.Atoi(splits[1]); err != nil {
			return Unknown, fmt.Errorf("failed to convert delete log key: %s to integer", splits[1])
		}
		return Delete, nil
	}
	return Unknown, fmt.Errorf("log command: %s not supported", rawLog)
}

type SetLog struct {
	key   int
	value int
}

func ParseSetLog(rawLog string) (*SetLog, error) {
	splits := strings.Split(rawLog, " ")
	if len(splits) != 3 {
		return nil, fmt.Errorf("unexpected set log format: %s", rawLog)
	}
	if splits[0] != string(Set) {
		return nil, fmt.Errorf("expected %s, got %s", string(Set), splits[0])
	}
	key, err := strconv.Atoi(splits[1])
	if err != nil {
		return nil, fmt.Errorf("failed to extract integer key from %s", splits[1])
	}
	value, err := strconv.Atoi(splits[2])
	if err != nil {
		return nil, fmt.Errorf("failed to extract integer value from %s", splits[2])
	}
	return &SetLog{key: key, value: value}, nil
}

type DeleteLog struct {
	key int
}

func ParseDeleteLog(rawLog string) (*DeleteLog, error) {
	splits := strings.Split(rawLog, " ")
	if len(splits) != 2 {
		return nil, fmt.Errorf("unexpected delete log format: %s", rawLog)
	}
	if splits[0] != string(Delete) {
		return nil, fmt.Errorf("expected %s, got %s", string(Delete), splits[0])
	}
	key, err := strconv.Atoi(splits[1])
	if err != nil {
		return nil, fmt.Errorf("failed to extract integer key from %s", splits[1])
	}
	return &DeleteLog{key: key}, nil
}
