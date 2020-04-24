package main

import (
	"fmt"
	"io"

	"github.com/golang-collections/collections/set"
)


func synchronize(store1 ObjectStore, store2 ObjectStore) {

	fmt.Println("obtaining infosSource...")
	infosSourceCh := make(chan ObjectInfo)
	go store1.GetInfos(infosSourceCh)

	fmt.Println("obtaining infosTarget...")
	infosTargetCh := make(chan ObjectInfo)
	go store2.GetInfos(infosTargetCh)

	set1 := set.New()
	for m := range infosSourceCh {
		set1.Insert(m)
	}
	fmt.Printf("obtained %v infosSource.\n", set1.Len())

	set2 := set.New()
	for m := range infosTargetCh {
		set2.Insert(m)
	}
	fmt.Printf("obtained %v infosTarget.\n", set2.Len())

	diffAdd := set1.Difference(set2)
	fmt.Printf("obtained %v additive differences \n", diffAdd.Len())

	diffSub := set2.Difference(set1)
	fmt.Printf("obtained %v subtractive differences \n", diffSub.Len())
	
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
