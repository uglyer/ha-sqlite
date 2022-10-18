package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func newStore(t *testing.T) *HaSqliteDBStore {
	store, err := NewHaSqliteDBStore()
	assert.NoError(t, err)
	return store
}

func TestGetDbId(t *testing.T) {
	store := newStore(t)
	_, _, err := store.getDBIdByPath("test.db")
	assert.NoError(t, err)
}

func TestCreateDb(t *testing.T) {
	store := newStore(t)
	id, err := store.createDBByPath("test.db")
	assert.NoError(t, err)
	assert.NotEqual(t, 0, id)
	queryId, ok, err := store.getDBIdByPath("test.db")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, id, queryId)
}
