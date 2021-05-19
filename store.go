package wrappedbadger

import (
	"errors"
	badger "github.com/dgraph-io/badger/v2"
	"time"
)

var ErrBreakIter = errors.New("done iterating")

type Store struct {
	DB *badger.DB
}

//OpenDb opens a Store with the default
//badger db settings minus logging in dir.
func OpenDefaultStore(dir string) (d *Store, err error) {
	d = new(Store)
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	d.DB, err = badger.Open(opts)
	return
}

//Close closes the Store
func (Store *Store) Close() error {
	return Store.DB.Close()
}

//IsClosed returns true if the underlying Store is closed
func (Store *Store) IsClosed() bool {
	return Store.DB.IsClosed()
}

//Get returns a copy of the value with the given key
func (Store *Store) Get(key []byte) ([]byte, error) {
	var v []byte
	err := Store.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			v = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return nil, err
		}
		return nil, nil
	}
	return v, nil
}

//Sets the value at the given key
func (Store *Store) Set(key []byte, value []byte) error {
	return Store.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
}

//SetWithMetadata sets a value with a metadata byte
func (Store *Store) SetWithMetadata(key []byte, value []byte, meta byte) error {
	return Store.DB.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value).WithMeta(meta)
		return txn.SetEntry(e)
	})
}

//SetWithTTL sets a value with a Time To Live attribute
func (Store *Store) SetWithTTL(key []byte, value []byte, ttl time.Duration) error {
	return Store.DB.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value).WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

//Iterate values iterates over keys matching the giving prefix, running f func
//on each value. f may return ErrBreakIter to break out of iteration early.
func (Store *Store) IterateValues(prefix []byte, f func(v []byte) error) error {
	return Store.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(f)
			if err != nil {
				if err != ErrBreakIter {
					return err
				}
				break
			}
		}
		return nil
	})
}

//IterateKeys iterates over keys only. The underlying iterator
//does not prefetch any values. The bytes of each key is passed to f. Iteration can be stopped
//early if f returns ErrBreakIter
func (Store *Store) IterateKeys(prefix []byte, f func(k []byte) error) error {
	return Store.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			err := f(it.Item().Key())
			if err != nil {
				if err != ErrBreakIter {
					return err
				}
				break
			}
		}
		return nil
	})
}

//SparseRead allows doing a sparse read of values. The underlying iterator does not prefetch any values.
//The bytes of each key is passed into kfunc. If kfunc returns true, nil then the value of the key will be passed
//into vfunc. If either functions return ErrBreakIter than iteration will be stopped early.
func (Store *Store) SparseRead(prefix []byte, kfunc func(k []byte) (bool, error), vfunc func(v []byte) error) error {
	return Store.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			ok, err := kfunc(it.Item().Key())
			if err != nil {
				if err != ErrBreakIter {
					return err
				}
				break
			}
			if ok {
				err = it.Item().Value(vfunc)
				if err != nil {
					if err != ErrBreakIter {
						return err
					}
					break
				}
			}
		}
		return nil
	})
}
