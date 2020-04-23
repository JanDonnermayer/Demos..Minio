package main

import (
	"fmt"
	"github.com/golang-collections/collections/set"
)

func main() {
	directory := "C:/Users/jan/AppData/Local/Temp/.minio-share/src3"
	bucket := "test"

	fmt.Println("obtaining file infos...")
	infosFile, err := getInfosFS(directory)
	if err != nil {
		panic(err)
	}
	println(len(infosFile))

	fmt.Println("obtaining object infos...")
	infosMinio := getInfosMinio(bucket)
	println(len(infosMinio))

	minioSet := set.New()
	for _, m := range infosMinio {
		minioSet.Insert(m)
	}
	fmt.Printf("%v distinct objects \n", minioSet.Len())

	fileSet := set.New()
	for _, m := range infosFile {
		fileSet.Insert(m)
	}
	fmt.Printf("%v distinct files \n", fileSet.Len())

	fmt.Println("obtaining differences...")
	diff := minioSet.Difference(fileSet)
	println(diff.Len())
}