package store

import (
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func newStore(t *testing.T) *HaSqliteDBStore {
	store, err := NewHaSqliteDBStore()
	assert.NoError(t, err)
	return store
}

func TestGetDbId(t *testing.T) {
	store := newStore(t)
	id, ok, err := store.getDBIdByPath("test.db")
	assert.NoError(t, err)
	log.Printf("id:%d,%v", id, ok)
}
