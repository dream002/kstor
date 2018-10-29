package kstordb

import (
	//"log"
	"runtime"
	"sync"
	"testing"

	"strconv"
)

var wg sync.WaitGroup

func Test_SetKeyValue(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for i := 1; i < 6; i++ {
		wg.Add(1)
		go mysettest(i*10000, t)
	}
	wg.Wait()
}

func mysettest(nums int, t *testing.T) {
	for i := nums; i < nums+200; i++ {
		k := "test" + strconv.Itoa(i)
		v := strconv.Itoa(i + 20)
		if err := SetKeyValue(k, v, "mybucket"); err == nil {
			t.Log("pass")
		} else {
			t.Error("error")
		}
	}
	wg.Done()
}

func mydeletetest(nums int, t *testing.T) {
	for i := nums; i < nums+200; i++ {
		k := "test" + strconv.Itoa(i)
		if err := DeleteKeyValue(k, "mybucket"); err == nil {
			t.Log("pass")
		} else {
			t.Error("error")
		}
	}
	wg.Done()
}

func Test_DeleteKeyValue(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for i := 1; i < 6; i++ {
		wg.Add(1)
		go mydeletetest(i*10000, t)
	}
	wg.Wait()
}
