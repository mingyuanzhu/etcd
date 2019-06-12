// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/wait"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- Request // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]string // current committed key-value pairs
	snapshotter *snap.Snapshotter
	idWorker    *IDWorker
	wait        wait.Wait
}

type IDWorker struct {
	index *uint64
}

func NewIDWorker() *IDWorker {
	var index uint64
	return &IDWorker{
		index: &index,
	}
}

func (w *IDWorker) getID() uint64 {
	return atomic.AddUint64(w.index, 1)
}

type Request struct {
	Id   uint64
	Data []kv
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- Request, commitC <-chan *Request, errorC <-chan error, wait wait.Wait) *kvstore {
	idWorker := NewIDWorker()
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter, idWorker: idWorker, wait: wait}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

func (s *kvstore) Propose(kvs []kv) error {

	id := s.idWorker.getID()
	request := Request{
		Id:   id,
		Data: kvs,
	}
	ch := s.wait.Register(id)
	s.proposeC <- request
	select {
	case resp := <-ch:
		if resp != nil {
			return resp.(error)
		}
		return nil
	case <-time.After(time.Second * 3):
		s.wait.Trigger(id, nil)
		return errors.New("timeout")
	}
}

func (s *kvstore) readCommits(commitC <-chan *Request, errorC <-chan error) {
	for req := range commitC {
		if req == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				continue
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		s.mu.Lock()
		for _, kv := range req.Data {
			s.kvStore[kv.Key] = kv.Val
		}
		s.mu.Unlock()
		s.wait.Trigger(req.Id, nil)

	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
