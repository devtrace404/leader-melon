package main

import (
	"fmt"
	"sync"
)

type Database interface {
	Set(key, value int) error
	Delete(key int) error
	Get(key int) (int, error)
	List() (map[int]int, error)
}

type database struct {
	mu   sync.RWMutex
	data map[int]int
}

func NewDatabase() Database {
	return &database{
		mu:   sync.RWMutex{},
		data: make(map[int]int),
	}
}

func (db *database) Set(key, value int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.set(key, value)
}

func (db *database) Get(key int) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.get(key)
}

func (db *database) List() (map[int]int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.list()
}

func (db *database) Delete(key int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.delete(key)
}

func (db *database) set(key, value int) error {
	db.data[key] = value
	return nil
}

func (db *database) delete(key int) error {
	delete(db.data, key)
	return nil
}

func (db *database) get(key int) (int, error) {
	value, ok := db.data[key]
	if !ok {
		return 0, fmt.Errorf("key %d does not exists in db", key)
	}
	return value, nil
}

func (db *database) list() (map[int]int, error) {
	data := make(map[int]int, len(db.data))

	for key, val := range db.data {
		data[key] = val
	}

	return data, nil
}
