package main

import (
	"log"
	"strings"
	"github.com/minio/minio-go/v6"
)

func getMetaMinio(info minio.ObjectInfo) ObjectMeta {
	etag := strings.ReplaceAll(info.ETag, "\"", "")

	return ObjectMeta{
		Size: info.Size,
		ETag: etag,
	}
}

func getAddressMinio(info minio.ObjectInfo) ObjectAddress {
	segments := strings.Split(info.Key, "/")
	key := segments[len(segments)-1]
	route := segments[:len(segments)-1]

	return ObjectAddress{
		Key:   key,
		Route: route,
	}
}

func getInfoMinio(info minio.ObjectInfo) ObjectInfo {
	return ObjectInfo {
		Meta:    getMetaMinio(info),
		Address: getAddressMinio(info),
	}
}

func getInfosMinio(bucketName string) []ObjectInfo {
	endpoint := "localhost:9001"
	accessKeyID := "minio"
	secretAccessKey := "minio123"
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(
		endpoint,
		accessKeyID,
		secretAccessKey,
		useSSL,
	)

	if err != nil {
		log.Fatalln(err)
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	var infos []ObjectInfo

	for info := range minioClient.ListObjects(bucketName, "", true, doneCh) {
		if info.Err != nil {
			log.Println(info.Err)
		}

		infos = append(infos, getInfoMinio(info))
	}

	return infos
}
