package wrappedbadger

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

var dir string

type TestData struct {
	Data1 int
	Data2 int
}

func TestCache(t *testing.T) {
	//setup temp dir
	dir, err := ioutil.TempDir("", "wrappedbadger_cache_test_")
	if err != nil {
		t.Error("error creating temp dir: ", err)
		t.FailNow()
	}
	fmt.Println("created testdir: ", dir)

	//load an empty cache store
	loadc := func() *CacheStore {
		//open store
		st, err := OpenDefaultStore(dir)
		if err != nil {
			t.Fatal("error creating store: ", err)
		}
		return &CacheStore{
			Store: st,
			Key:   []byte{0},
		}
	}

	closec := func(c *CacheStore) {
		err := c.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	var c *CacheStore

	defer func() {
		//cleanup
		fmt.Println("cleaning up...")
		if c != nil {
			closec(c)
		}
		if err = os.RemoveAll(dir); err != nil {
			fmt.Println("error cleaning up test dir: ", err)
			os.Exit(1)
		}
	}()

	c = loadc()

	d := &TestData{
		Data1: 1,
		Data2: 2,
	}
	//add data to the cache store
	t.Run("Save", func(t *testing.T) {
		if err := c.Save(d); err != nil {
			t.Error(err)
			t.FailNow()
		}
	})

	// close the cache
	closec(c)
	c = nil

	//reopen cache
	c = loadc()
	d2 := &TestData{}

	//load persisted data
	t.Run("Load", func(t *testing.T){
		if err := c.Load(d2); err != nil {
			t.Error(err)
			t.FailNow()
		}
	})

	t.Run("Check", func(t *testing.T){
		if !(d.Data1 == d2.Data1 || d.Data2 == d2.Data2) {
			t.Errorf("data was not persisted correctly: %v : %v ", d, d2)
		}
	})
}
