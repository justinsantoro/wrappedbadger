package wrappedbadger

import (
	"encoding/json"
	"fmt"
)

func wrapError(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}

func badgerError(err error) error {
	return wrapError("badger error", err)
}

type CacheStore struct {
	*Store
	Key []byte
}

func (c *CacheStore) Load(v interface{}) error {
	b, err := c.Get(c.Key)
	if err != nil {
		return badgerError(err)
	}
	if v != nil {
		err := json.Unmarshal(b, v)
		if err != nil {
			return fmt.Errorf("error unmarshaling: %v", err)
		}
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
