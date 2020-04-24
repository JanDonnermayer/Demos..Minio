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
	fmt.Println("obtaining object infos...")
	infosMinio := minioStore.GetInfos()
	println(len(infosMinio))

	minioSet := set.New()
	for _, m := range infosMinio {
		minioSet.Insert(m)
	}

	fsStore := FsObjectStore{
		RootDirectory: "C:/Users/jan/AppData/Local/Temp/.minio-share/src3",
	}
	fmt.Println("obtaining file infos...")
	infosFile, err := fsStore.GetInfos()
	if err != nil {
		panic(err)
	}
	println(len(infosFile))

	fileSet := set.New()
	for _, m := range infosFile {
		fileSet.Insert(m)
	}

	diff := minioSet.Difference(fileSet)
	fmt.Printf("obtained %v differences \n", diff.Len())

	var diffInfos []ObjectInfo
	diff.Do(func(info interface{}) {
		diffInfos = append(diffInfos, info.(ObjectInfo))
	})

	for _, diffInfo := range diffInfos {
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

		fmt.Printf("Copy %+v \n", address)
		_, err = io.Copy(writer, reader)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
		}
	}

}
