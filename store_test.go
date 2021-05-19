package wrappedbadger

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

var (
	st      *Store
	prefix  = []byte{0}
	keyvals = map[string][]byte{
		string(append(prefix, 0)): {0},
		string(append(prefix, 1)): {1},
		string(append(prefix, 2)): {2},
	}
)

func breaker() func(x []byte) error {
	return func(v []byte) error {
		switch v[len(v)-1] {
		case 1:
			return ErrBreakIter
		case 2:
			return errors.New("ErrBreakIter didn't break iteration")
		}
		return nil
	}
}

//TestStore_IterateValues ensures store.IterateValues
//breaks out of iteration when f returns ErrBreakIter
func TestStore_IterateValues(t *testing.T) {
	if err := st.IterateValues(prefix, breaker()); err != nil {
		t.Error(err)
	}
}

//TestStore_IterateKeys ensures store.IterateValues
//breaks out of iteration when f returns ErrBreakIter
func TestStore_IterateKeys(t *testing.T) {
	if err := st.IterateKeys(prefix, breaker()); err != nil {
		t.Error(err)
	}
}

//TestStore_SparseRead Ensures store.SparseRead breaks
// out of iteration when either kfunc or vfunc returns
// ErrBreakIter
func TestStore_SparseRead(t *testing.T) {
	niler := func(x []byte) error { return nil }
	nilKey := func(x []byte) (bool, error) { return true, nil }
	breakKey := func(x []byte) (bool, error) {
		return false, breaker()(x)
	}
	if err := st.SparseRead(prefix, breakKey, niler); err != nil {
		t.Error("kfunc didn't trigger break: ", err)
	}
	if err := st.SparseRead(prefix, nilKey, breaker()); err != nil {
		t.Error("vfunc didn't trigger break: ", err)
	}
}

func TestMain(m *testing.M) {
	//setup temp dir
	var err error
	dir, err := ioutil.TempDir("", "wrappedbadger_store_test_")
	if err != nil {
		fmt.Println("error creating temp dir: ", err)
		os.Exit(1)
	}
	fmt.Println("created testdir: ", dir)

	//open store
	st, err = OpenDefaultStore(dir)
	if err != nil {
		fmt.Println("error creating store: ", err)
		os.Exit(1)
	}

	//pre-fill store
	for k, v := range keyvals {
		err = st.Set([]byte(k), v)
		if err != nil {
			fmt.Println("error filling test db: ", err)
			os.Exit(1)
		}
	}

	//run tests
	m.Run()

	//cleanup
	fmt.Println("cleaning up...")
	if err := st.Close(); err != nil {
		fmt.Println("error closing store: ", err)
	}
	if err = os.RemoveAll(dir); err != nil {
		fmt.Println("error cleaning up test dir: ", err)
		os.Exit(1)
	}
}
