package main

import (
	"fmt"
	"io"

	"github.com/golang-collections/collections/set"
)


func synchronize(store1 ObjectStore, store2 ObjectStore) {

	fmt.Println("indexing source...")
	infosSourceCh := store1.GetInfos()

	fmt.Println("indexing target...")
	infosTargetCh := store2.GetInfos()
	
	setSource := set.New()
	for m := range infosSourceCh {
		setSource.Insert(m)
	}
	fmt.Printf("source: %v objects\n", setSource.Len())

	setTarget := set.New()
	for m := range infosTargetCh {
		setTarget.Insert(m)
	}
	fmt.Printf("target: %v objects\n", setTarget.Len())


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
