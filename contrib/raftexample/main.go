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
	"flag"
	"github.com/gorilla/mux"
	"go.etcd.io/etcd/pkg/wait"
	"log"
	"strconv"
	"strings"

	"go.etcd.io/etcd/raft/raftpb"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	clusterIDs := flag.String("ids", "1", "comma separated cluster ids")
	localID := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	proposeReqC := make(chan Request)
	defer close(proposeC)
	defer close(proposeReqC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }

	ids := strings.Split(*clusterIDs, ",")
	addrs := strings.Split(*cluster, ",")

	if len(ids) != len(addrs) {
		log.Fatal("cluster count should same with the ids count")
	}

	peers := make([]Peer, len(ids))
	peer := Peer{}

	for i, s := range ids {
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			log.Fatalf("parse ID error %v \n", err)
		}
		peers[i] = Peer{ID: id, Addr: addrs[i]}
		if id == uint64(*localID) {
			peer = peers[i]
		}
	}

	if peer.Addr == "" {
		log.Fatalf("can not find local Addr from cluster by ID %d \n", *localID)
	}
	// to handle the async request
	wait := wait.New()

	rc, commitC, errorC, snapshotterReady := newRaftNode(peer, peers, *join, getSnapshot, proposeReqC, confChangeC, wait)
	log.Println("start node successfully")
	router := mux.NewRouter()
	serveMembersHttpKVAPI(router, *kvport+1, confChangeC, rc)
	log.Println("start member server successfully")

	kvs = newKVStore(<-snapshotterReady, proposeReqC, commitC, errorC, wait)
	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(router, kvs, *kvport)
	log.Println("start kvstore successfully")

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
