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
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"go.etcd.io/etcd/raft/raftpb"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
	raftNode    *raftNode
}

type StartRaftParam struct {
	Peers []Peer `json: "peers"`
	Self  Peer   `json: "self"`
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(router *mux.Router, kvStore *kvstore, port int) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: router,
	}
	// get key
	router.HandleFunc("/keys/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		if v, ok := kvStore.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	}).Methods(http.MethodGet)
	// put key
	router.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		m := new(map[string]string)
		if err := json.Unmarshal(v, m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(*m) == 0 {
			w.Write([]byte("success"))
			return
		}
		kvs := make([]kv, 0)
		for k, v := range *m {
			kvs = append(kvs, kv{Key: k, Val: v})
		}
		if err := kvStore.Propose(kvs); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.Write([]byte("success"))
		fmt.Printf("request elapse %d \n", time.Now().Sub(start).Nanoseconds()/1000/1000)
	}).Methods(http.MethodPost)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

}

// serveMembersHttpKVAPI manage the raft node
func serveMembersHttpKVAPI(router *mux.Router, port int, confChangeC chan<- raftpb.ConfChange, rc *raftNode) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: router,
	}
	// get member status
	router.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{}
		response["status"] = rc.node.Status().RaftState.String()
		response["id"] = rc.node.Status().ID
		response["leader"] = rc.node.Status().Lead
		response["peers"] = rc.peers
		if responseJSON, err := json.Marshal(response); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			w.Write(responseJSON)
		}
	}).Methods(http.MethodGet)

	router.HandleFunc("/members/configs", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		peers := new([]Peer)
		err := decoder.Decode(peers)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		err = rc.UpdatePeers(context.TODO(), peers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	}).Methods(http.MethodPost)

	router.HandleFunc("/members/snapshots", func(w http.ResponseWriter, r *http.Request) {
		err := rc.makeSnapshot(context.TODO())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	}).Methods(http.MethodPost)

	router.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		param := new(StartRaftParam)
		err := decoder.Decode(param)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		rc.StartRaftBySpecialPeers(context.TODO(), param.Peers, param.Self)
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	}).Methods(http.MethodPost)

	router.HandleFunc("/members/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		if id == "" {
			http.Error(w, "member id is null", http.StatusBadRequest)
		}
		nodeId, err := strconv.ParseUint(id, 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}
		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	}).Methods(http.MethodDelete)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

}
