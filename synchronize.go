package main

import (
	"fmt"
	"github.com/golang-collections/collections/set"
	"io"
	"sync"
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

	getDiff := func(set1 *set.Set, set2 *set.Set) []ObjectInfo {
		diff := set1.Difference(set2)
		fmt.Printf("source except target: %v objects\n", diff.Len())

		var diffInfos []ObjectInfo
		diff.Do(func(info interface{}) {
			diffInfos = append(diffInfos, info.(ObjectInfo))
		})

		return diffInfos
	}

	delete(target, getDiff(setTarget, setSource))
	copy(source, target, getDiff(setSource, setTarget))
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
