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
	"github.com/gorilla/mux"
	"go.etcd.io/etcd/raft/raftpb"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
	raftNode    *raftNode
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()

	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if v, ok := h.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

type StartRaftParam struct {
	Peers []Peer `json: "peers"`
	Self  Peer   `json: "self"`
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(router *mux.Router, kv *kvstore, port int) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: router,
	}
	// get key
	router.HandleFunc("/key/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		if v, ok := kv.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	}).Methods(http.MethodGet)
	// put key
	router.HandleFunc("/key/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		kv.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.Write([]byte("success"))
	}).Methods(http.MethodPost)

	//router.Handle("/**", &httpKVAPI{
	//	store:       kv,
	//	confChangeC: confChangeC,
	//	raftNode:    rc,
	//})

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

}

// serveMembersHttpKVAPI manage the raft node
func serveMembersHttpKVAPI(router *mux.Router, port int,  confChangeC chan<- raftpb.ConfChange, rc *raftNode) {
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
