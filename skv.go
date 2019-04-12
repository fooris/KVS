package KVS

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
)

type KeyValueStore struct {
	db *bolt.DB
}

var (
	ErrNotFound = errors.New("not found")
	ErrBadValue = errors.New("bad value")
	bucketName  = []byte("defaultKV")
)

func Open(path string) (*KeyValueStore, error) {
	db, err := bolt.Open(path, 0640, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &KeyValueStore{db: db}, nil
}

func (kvs *KeyValueStore) Close() error {
	return kvs.db.Close()
}

func (kvs *KeyValueStore) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(value)

	return kvs.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(bucketName).Put([]byte(key), buf.Bytes())
		return err
	})
}

func (kvs *KeyValueStore) Get(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		k, v := c.Seek([]byte(key))
		if k == nil || string(k) != key {
			return ErrNotFound
		}
		if value == nil {
			return nil
		}
		d := gob.NewDecoder(bytes.NewReader(v))
		return d.Decode(value)
	})
}

func (kvs *KeyValueStore) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		k, _ := c.Seek([]byte(key))
		if k == nil || string(k) != key {
			return ErrNotFound
		} else {
			return c.Delete()
		}
	})
}

/*
 * ugly? yes, inefficient? also yes
 * but i only needed this hacky solution atm
 * TODO: create elegant solution
 */
func (kvs *KeyValueStore) CountPairs() uint64 {
	var ret uint64 = 0
	kvs.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucketName))

		b.ForEach(func(k, v []byte) error {
			atomic.AddUint64(&ret, 1)
			return nil
		})
		return nil
	})
	return atomic.LoadUint64(&ret)
}
