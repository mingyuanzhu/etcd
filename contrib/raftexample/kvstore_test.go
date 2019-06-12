// Copyright 2016 The etcd Authors
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
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func Test_kvstore_snapshot(t *testing.T) {
	tm := map[string]string{"foo": "bar"}
	s := &kvstore{kvStore: tm}

	v, _ := s.Lookup("foo")
	if v != "bar" {
		t.Fatalf("foo has unexpected value, got %s", v)
	}

	data, err := s.getSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.kvStore = nil

	if err := s.recoverFromSnapshot(data); err != nil {
		t.Fatal(err)
	}
	v, _ = s.Lookup("foo")
	if v != "bar" {
		t.Fatalf("foo has unexpected value, got %s", v)
	}
	if !reflect.DeepEqual(s.kvStore, tm) {
		t.Fatalf("store expected %+v, got %+v", tm, s.kvStore)
	}
}

func BenchmarkIDWork(b *testing.B) {
	worker := NewIDWorker()
	for i := 0; i < b.N; i++ {
		worker.getID()
	}
}

func TestNewIDWorker(t *testing.T) {
	worker := NewIDWorker()
	for i := 0; i < 10; i++ {
		id := worker.getID()
		if uint64(i+1) != id {
			t.Fatalf("except %d actual %d", i+1, id)
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	req := new(Request)
	req.Data = []kv{{
		Key: "k_10_10",
		Val: "v_10_10",
	}}
	req.Id = 1
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(req); err != nil {
		b.Fatal(err)
	} else {
		buf.Bytes()
	}
}

func BenchmarkDecode(b *testing.B) {
	req := new(Request)
	req.Data = []kv{{
		Key: "k_10_10",
		Val: "v_10_10",
	}}
	req.Id = 1
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(req)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 1; i++ {
		var req *Request
		dec := gob.NewDecoder(bytes.NewBuffer(buf.Bytes()))
		if err := dec.Decode(&req); err != nil {
			b.Fatal("raftexample: could not decode message, ", err)
		}
	}
}

func TestPutKey(t *testing.T) {
	mu := new(sync.RWMutex)
	m := make(map[string]string)
	startTime := time.Now().Unix()
	for i := 0; i < 10000; i++ {
		mu.Lock()
		m[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
		mu.Unlock()
	}
	fmt.Println("put key elapse ", time.Now().Unix()-startTime)
}
