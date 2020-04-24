package main

import (
	"fmt"
	"io"

	"github.com/golang-collections/collections/set"
	"github.com/minio/minio-go/v6"
)

func main() {

	endpoint := "localhost:9001"
	accessKeyID := "minio"
	secretAccessKey := "minio123"
	useSSL := false

	minioClient, err := minio.New(
		endpoint,
		accessKeyID,
		secretAccessKey,
		useSSL,
	)
	if err != nil {
		panic(err)
	}

	minioStore := MinioObjectStore{
		Client: minioClient,
		Bucket: "test",
	}
	fmt.Println("obtaining minio infos...")
	infosMinioCh := make(chan ObjectInfo)
	go minioStore.GetInfos(infosMinioCh)

	fsStore := FsObjectStore{
		RootDirectory: "C:/Users/jan/AppData/Local/Temp/.minio-share/src3",
	}
	fmt.Println("obtaining file infos...")
	infosFsCh := make(chan ObjectInfo)
	go fsStore.GetInfos(infosFsCh)

	minioSet := set.New()
	for m := range infosMinioCh {
		minioSet.Insert(m)
	}
	fmt.Printf("obtained %v minio infos.\n", minioSet.Len())

	fileSet := set.New()
	for m := range infosFsCh {
		fileSet.Insert(m)
	}
	fmt.Printf("obtained %v file infos.\n", minioSet.Len())

	diff := minioSet.Difference(fileSet)
	fmt.Printf("obtained %v differences \n", diff.Len())

	var diffInfos []ObjectInfo
	diff.Do(func(info interface{}) {
		diffInfos = append(diffInfos, info.(ObjectInfo))
	})

	lenDiff := len(diffInfos)
	for i, diffInfo := range diffInfos {
		address := diffInfo.Address

		writer, err := fsStore.GetWriter(address)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			continue
		}
		defer writer.Close()

		reader, err := minioStore.GetReader(address)
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
