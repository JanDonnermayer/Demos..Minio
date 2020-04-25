package main

import (
	"github.com/minio/minio-go/v6"
)

func getMinioStore(bucketName string) ObjectStore {
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

	return MinioObjectStore{
		Client: minioClient,
		Bucket: bucketName,
	}
}

func getFileStore(path string) ObjectStore {
	return FsObjectStore{
		RootDirectory: path,
	}
}

func main() {

	store1 := getFileStore("C:/Users/jan/AppData/Local/Temp/.minio-share/src3")
	store2 := getFileStore("C:/Users/jan/AppData/Local/Temp/.minio-share/src2")

	//synchronize2(store1, store2, "Bachelorarbeit")
	synchronize(store1, store2)
}
