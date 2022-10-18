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
	_, _, err := store.GetDBIdByPath("test.db")
	assert.NoError(t, err)
}

func TestCreateDb(t *testing.T) {
	store := newStore(t)
	id, err := store.CreateDBByPath("test.db")
	assert.NoError(t, err)
	assert.NotEqual(t, 0, id)
	queryId, ok, err := store.GetDBIdByPath("test.db")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, id, queryId)
}

func TestCreateSameDb(t *testing.T) {
	store := newStore(t)
	id, err := store.CreateDBByPath("test.db")
	assert.NoError(t, err)
	assert.NotEqual(t, 0, id)
	_, err = store.CreateDBByPath("test.db")
	assert.Error(t, err)
	queryId, ok, err := store.GetDBIdByPath("test.db")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, id, queryId)
}

func TestGetDbPath(t *testing.T) {
	store := newStore(t)
	path := "test.db"
	id, err := store.CreateDBByPath(path)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, id)
	queryPath, err := store.GetDBPathById(id)
	assert.NoError(t, err)
	assert.Equal(t, path, queryPath)
}
