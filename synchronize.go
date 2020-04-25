package main

import (
	"fmt"
	"io"
	"sync"

	"github.com/golang-collections/collections/set"
)


func synchronize(store1 ObjectStore, store2 ObjectStore, addrPref string) {
	var wg sync.WaitGroup
	wg.Add(2)

	setSource := set.New()
	go func() {
		fmt.Println("indexing source...")
		for m := range store1.GetInfos(addrPref) {
			setSource.Insert(m)
		}
		fmt.Printf("source: %v objects\n", setSource.Len())
		wg.Done()
	}()

	setTarget := set.New()
	go func() {
		fmt.Println("indexing target...")
		for m := range store2.GetInfos(addrPref) {
			setTarget.Insert(m)
		}
		fmt.Printf("target: %v objects\n", setTarget.Len())
		wg.Done()
	}()

	wg.Wait()

	diffAdd := setSource.Difference(setTarget)
	fmt.Printf("source except target: %v objects\n", diffAdd.Len())

	diffSub := setTarget.Difference(setSource)
	fmt.Printf("target except source: %v objects\n", diffSub.Len())

	var diffAddInfos []ObjectInfo
	diffAdd.Do(func(info interface{}) {
		diffAddInfos = append(diffAddInfos, info.(ObjectInfo))
	})

	lenDiff := len(diffAddInfos)
	for i, diffInfo := range diffAddInfos {
		address := diffInfo.Address

		writer, err := store2.GetWriter(address)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			continue
		}
		defer writer.Close()

		reader, err := store1.GetReader(address)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			continue
		}
		defer reader.Close()

		fmt.Printf("%v of %v | Copy %+v \n", i+1, lenDiff, address)
		_, err = io.Copy(writer, reader)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
		}
	}
}
