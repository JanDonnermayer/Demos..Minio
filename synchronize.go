package main

import (
	"fmt"
	"io"
	"sync"
	"github.com/golang-collections/collections/set"
)

func synchronize(source ObjectStore, target ObjectStore) {
	synchronize2(source, target, "")
}

func synchronize2(source ObjectStore, target ObjectStore, addrPref string) {
	var wg sync.WaitGroup
	wg.Add(2)

	fmt.Printf("synchronize '%+v' -> '%+v'\n", source, target)

	setSource := set.New()
	go func() {
		fmt.Println("indexing source store...")
		for m := range source.GetInfos(addrPref) {
			setSource.Insert(m)
		}
		fmt.Printf("source: %v objects\n", setSource.Len())
		wg.Done()
	}()

	setTarget := set.New()
	go func() {
		fmt.Println("indexing target store...")
		for m := range target.GetInfos(addrPref) {
			setTarget.Insert(m)
		}
		fmt.Printf("target: %v objects\n", setTarget.Len())
		wg.Done()
	}()

	wg.Wait()

	diffAdd := setSource.Difference(setTarget)
	fmt.Printf("source except target: %v objects\n", diffAdd.Len())

	var diffAddInfos []ObjectInfo
	diffAdd.Do(func(info interface{}) {
		diffAddInfos = append(diffAddInfos, info.(ObjectInfo))
	})

	diffSub := setTarget.Difference(setSource)
	fmt.Printf("target except source: %v objects\n", diffSub.Len())

	var diffSubInfos []ObjectInfo
	diffSub.Do(func(info interface{}) {
		diffSubInfos = append(diffSubInfos, info.(ObjectInfo))
	})

	wg.Add(2)

	go func() {
		copy(source, target, diffAddInfos)
		wg.Done()
	}()

	//go func() {
	//	delete(target, diffSubInfos)
	//	wg.Done()
	//}()

	wg.Wait()
}

func delete(target ObjectStore, infos []ObjectInfo) {
	lenTotal := len(infos)
	for i, diffInfo := range infos {
		address := diffInfo.Address

		fmt.Printf("%v of %v | Delete %+v \n", i+1, lenTotal, address)
		err := target.Delete(address)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
		}
	}
}

func copy(source ObjectStore, target ObjectStore, infos []ObjectInfo) {
	lenTotal := len(infos)
	for i, diffInfo := range infos {
		address := diffInfo.Address

		writer, err := target.GetWriter(address)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			continue
		}
		defer writer.Close()

		reader, err := source.GetReader(address)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			continue
		}
		defer reader.Close()

		fmt.Printf("%v of %v | Copy %+v \n", i+1, lenTotal, address)
		_, err = io.Copy(writer, reader)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
		}
	}
}
