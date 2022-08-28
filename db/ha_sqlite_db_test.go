package db_test

import (
	"context"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"testing"
)

func Test_OpenDB(t *testing.T) {
	store, err := db.NewHaSqliteDB()
	if err != nil {
		t.Fatalf("Error NewHaSqliteDB:%v", err)
	}
	openResp, err := store.Open(context.Background(), &proto.OpenRequest{Dsn: ":memory:"})
	if err != nil {
		t.Fatalf("Error NewHaSqliteDB:%v", err)
	}
	log.Printf("openResp.DbId:%d", openResp.DbId)
}
