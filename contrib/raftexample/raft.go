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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/wait"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

type Peer struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

func (p Peer) String() string {
	return fmt.Sprintf("ID: %d Addr: %s", p.ID, p.Addr)
}

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan Request         // proposed messages (k,v)
	confChangeC chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *Request        // entries committed to log (k,v)
	errorC      chan<- error           // errors from raft session

	peer        Peer   // client ID for raft session
	peers       []Peer // raft peer URLs
	join        bool   // node is joining an existing cluster
	waldir      string // path to WAL directory
	snapdir     string // path to snapshot directory
	getSnapshot func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	// TODO: raft entry will use so many memory until make snapshot will release the memory
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	wait wait.Wait
}

var defaultSnapshotCount uint64 = 100

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(peer Peer, peers []Peer, join bool, getSnapshot func() ([]byte, error), proposeC <-chan Request,
	confChangeC chan raftpb.ConfChange, wait wait.Wait) (*raftNode, <-chan *Request, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *Request)
	errorC := make(chan error)

	hash := md5.New()
	hash.Write([]byte(peer.Addr))
	peerHash := fmt.Sprintf("%x", hash.Sum(nil))

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		peer:        peer,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%s", peerHash),
		snapdir:     fmt.Sprintf("raftexample-%s-snap", peerHash),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay

		wait: wait,
	}

	if recordPeers, err := rc.getRecordPeers(); err != nil {
		log.Fatalln("parse peers from file error")
	} else {
		if len(recordPeers) > 0 {
			rc.peers = recordPeers
		}
	}

	oldwal := wal.Exist(rc.waldir)
	// all new node should wait call API to start raft
	if oldwal || peer.ID == uint64(100) { // temp hard code
		go rc.startRaft()
	}
	return rc, commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			var req *Request
			dec := gob.NewDecoder(bytes.NewBuffer(ents[i].Data))
			if err := dec.Decode(&req); err != nil {
				log.Println("raftexample: could not decode message, ", err)
			}
			select {
			case rc.commitC <- req:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			log.Printf("apply conf context %v \n", string(cc.Context))
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
				// append peer
				exist := false
				for _, peer := range rc.peers {
					if peer.ID == cc.NodeID {
						exist = true
						break
					}
				}
				if !exist {
					rc.peers = append(rc.peers, Peer{ID: cc.NodeID, Addr: string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.peer.ID) {
					log.Println("I've been removed from the cluster! You can shut down.")
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
				// remove peer
				newPeers := make([]Peer, 0)
				for _, peer := range rc.peers {
					if peer.ID != cc.NodeID {
						newPeers = append(newPeers, peer)
					}
				}
				rc.peers = newPeers
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.peer.ID)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	// TODO : can do something to update the confState peers
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil so client knows commit channel is current
	rc.commitC <- nil
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) StartRaftBySpecialPeers(ctx context.Context, peers []Peer, peer Peer) error {
	if len(peers) == 0 {
		return errors.New("peers can not be 0")
	}
	rc.peers = peers
	rc.join = true
	rc.peer = peer
	rc.startRaft()
	return nil
}

func (rc *raftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	// store the init peers to file
	rc.recordPeers(rc.peers)

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: rc.peers[i].ID}
	}
	c := &raft.Config{
		ID:                        rc.peer.ID,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}
	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.peer.ID),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(rc.peer.ID, 10)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for i := range rc.peers {
		if rc.peers[i].ID != rc.peer.ID {
			rc.transport.AddPeer(types.ID(rc.peers[i].ID), []string{rc.peers[i].Addr})
		}
	}

	go rc.reportStatus()
	go rc.serveRaft()
	go rc.serveChannels()
}

func (rc *raftNode) reportStatus() {
	for range time.NewTicker(time.Second * 5).C {
		log.Printf("ID=%d applied=%d status=%s lead=%d peers_size=%d peers=%v \n", rc.node.Status().ID, rc.node.Status().Applied, rc.node.Status().RaftState.String(), rc.node.Status().Lead, len(rc.peers), rc.peers)
	}
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index

	rc.commitC <- nil // trigger kvstore to load snapshot
}

var snapshotCatchUpEntriesN uint64 = 100

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	if err := rc.makeSnapshot(context.TODO()); err != nil {
		log.Println("make snapshot err", err)
	}
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()
	// TODO: tick
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					var buf bytes.Buffer
					if err := gob.NewEncoder(&buf).Encode(prop); err != nil {
						log.Println("encode prop err", err)
						rc.wait.Trigger(prop.Id, err)
					} else {
						// blocks until accepted by raft state machine
						err := rc.node.Propose(context.TODO(), buf.Bytes())
						if err != nil {
							rc.wait.Trigger(prop.Id, err)
							log.Printf("propose error")
						}
					}
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			//rc.maybeTriggerSnapshot()
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peer.Addr)
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (rc *raftNode) UpdatePeers(ctx context.Context, peers *[]Peer) error {
	if raft.StateLeader != rc.node.Status().RaftState {
		return errors.New("this node is not leader")
	}
	if peers == nil {
		return nil
	}
	log.Printf("update new peers %v \n", peers)
	// remove does not exist peers
	oldPeers := rc.peers
	for _, oldPeer := range oldPeers {
		exist := false
		for _, peer := range *peers {
			if oldPeer.ID == peer.ID {
				exist = true
				break
			}
		}
		if exist {
			continue
		}
		log.Printf("update peers to remove node %d \n", oldPeer.ID)
		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: oldPeer.ID,
		}
		rc.confChangeC <- cc
	}
	// add new peers
	for _, newPeer := range *peers {
		exist := false
		for _, oldPeer := range oldPeers {
			if oldPeer.ID == newPeer.ID {
				exist = true
				break
			}
		}
		if exist {
			continue
		}
		log.Printf("update peers to add node %d \n", newPeer.ID)
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  newPeer.ID,
			Context: []byte(newPeer.Addr),
		}
		rc.confChangeC <- cc
	}
	return nil
}

func (rc *raftNode) recordPeers(peers []Peer) error {
	oldwal := wal.Exist(rc.waldir)
	if !oldwal {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
			return err
		}
	}
	data, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fmt.Sprintf("%s/%s", rc.waldir, "peers.json"), data, 0750)
	return err
}

func (rc *raftNode) getRecordPeers() ([]Peer, error) {
	oldwal := wal.Exist(rc.waldir)
	if !oldwal {
		return []Peer{}, nil
	}
	data, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", rc.waldir, "peers.json"))
	if err != nil {
		return nil, err
	}
	peers := new([]Peer)
	if err := json.Unmarshal(data, peers); err != nil {
		return nil, err
	}
	return *peers, nil
}

func (rc *raftNode) makeSnapshot(ctx context.Context) error {
	if rc.getSnapshot == nil {
		return errors.New("make snapshot method empty")
	}
	currentIndex := rc.appliedIndex
	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", currentIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(currentIndex-1, &rc.confState, data)
	if err != nil {
		return err
	}
	rc.recordPeers(rc.peers)
	if err := rc.saveSnap(snap); err != nil {
		return err
	}
	if err := rc.raftStorage.Compact(currentIndex- 1); err != nil {
		log.Println("compacted log err", err)
	}
	log.Printf("compacted log at index %d", currentIndex - 1)
	rc.snapshotIndex = currentIndex
	return nil
}
