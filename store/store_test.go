package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// Test_DefaultStoreOpen tests that the store can be opened.
func Test_DefaultStoreOpen(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := NewDefaultStore("node0", tmpDir, "127.0.0.1:0")
	defer os.RemoveAll(tmpDir)
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(false); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
}

// Test_StoreOpenSingleNode tests that a command can be applied to the log
func Test_DefaultStoreOpenSingleNode(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := NewDefaultStore("node0", tmpDir, "127.0.0.1:0")
	defer os.RemoveAll(tmpDir)

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	if err := s.Set("foo", "store", "bar"); err != nil {
		t.Fatalf("failed to set key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	var value interface{}
	err := s.gs.Get("foo", "store", value)
	if err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if value != "bar" {
		t.Fatalf("key has wrong value: %s", value)
	}

	if err := s.Delete("foo", "store"); err != nil {
		t.Fatalf("failed to delete key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = s.gs.Get("foo", "store", value)
	if err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if value != "" {
		t.Fatalf("key has wrong value: %s", value)
	}

}
