package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
)

func TestDecodePeer(t *testing.T) {
	input := "[{\"id\": 100, \"addr\": \"http://localhost:12379\"}, {\"id\": 200, \"addr\": \"http://localhost:22379\"}]"
	peers := new([]Peer)
	err := json.Unmarshal([]byte(input), peers)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(100) != (*peers)[0].ID {
		t.Fatal((*peers)[0].ID)
	}
	fmt.Println(peers)
}

func TestURLParse(t *testing.T) {
	if url, err := url.Parse("google.com"); err != nil {
		t.Error(err)
	} else {
		fmt.Println("host ", url.Host)
	}
}