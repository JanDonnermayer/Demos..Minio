package main

import (
	"github.com/minio/minio-go/v6"
	"log"
)

func countObjects() {
	endpoint := "localhost:9001"
	accessKeyID := "minio"
	secretAccessKey := "minio123"
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	doneCh := make(chan struct{})

    defer close(doneCh)

	log.Println("obtaining objects...")

	length := 0

	for object := range minioClient.ListObjects("test", "", true, doneCh) {
		if object.Err != nil {
			log.Println(object.Err)
			return
		}
		length ++
	}

	log.Println(length)

}