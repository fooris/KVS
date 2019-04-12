package KVS

import (
	"os"
	"testing"
)

func TestGeneral(t *testing.T) {
	os.Remove("test-db.db")
	db, err := Open("test-db.db")
	if err != nil {
		t.Fatal(err)
	}

	//test counting
	if count := db.CountPairs(); count != 0 {
		t.Fatal("count expected: 0 but was:", count)
	}

	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}

	// test counting
	if count := db.CountPairs(); count != 1 {
		t.Fatal("count expected: 1 but was:", count)
	}

	// get it back
	var val string
	if err := db.Get("key1", &val); err != nil {
		t.Fatal(err)
	} else if val != "value1" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}



	db.Close()
}