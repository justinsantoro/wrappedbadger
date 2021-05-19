package wrappedbadger

import (
	"encoding/json"
	"errors"
	"fmt"
)

func wrapError(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}

func badgerError(err error) error {
	return wrapError("badger error", err)
}

var ErrCacheStoreNoValue = errors.New("no value")

type CacheStore struct {
	*Store
	Key []byte
}

func (c *CacheStore) Close() error {
	return c.Store.Close()
}

func (c *CacheStore) Load(v interface{}) error {
	b, err := c.Get(c.Key)
	if err != nil {
		return badgerError(err)
	}
	if b == nil {
		return ErrCacheStoreNoValue
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("error unmarshaling: %v", err)
	}
	return nil
}

func (c *CacheStore) Save(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("error marshaling: %v", err)
	}
	err = c.Set(c.Key, b)
	if err != nil {
		return badgerError(err)
	}
	return nil
}
