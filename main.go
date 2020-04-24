package main

import (
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

	fsStore := FsObjectStore{
		RootDirectory: "C:/Users/jan/AppData/Local/Temp/.minio-share/src3",
	}

	synchronize(minioStore, fsStore)
}
