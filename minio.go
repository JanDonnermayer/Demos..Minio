package main

import (
	"github.com/minio/minio-go/v6"
	"io"
	"log"
	"strings"
)

type MinioObjectStore struct {
	Bucket string
	Client *minio.Client
}

func getReaderMinio(store *MinioObjectStore, address ObjectAddress) (io.ReadCloser, error) {
	objectName := address.Route + "/" + address.Key
	return store.Client.GetObject(
		store.Bucket,
		objectName,
		minio.GetObjectOptions{},
	)
}

// toDo: getWriter

func getMetaMinio(info minio.ObjectInfo) ObjectMeta {
	etag := strings.ReplaceAll(info.ETag, "\"", "")

	return ObjectMeta{
		Size: info.Size,
		ETag: etag,
	}
}

func getAddressMinio(info minio.ObjectInfo) ObjectAddress {
	nonempty := func(s string) bool { return s != "" }
	segments := filter(strings.Split(info.Key, "/"), nonempty)

	return ObjectAddress{
		Key:   segments[len(segments)-1],
		Route: strings.Join(segments[:len(segments)-1], "/"),
	}
}

func getInfoMinio(info minio.ObjectInfo) ObjectInfo {
	return ObjectInfo{
		Meta:    getMetaMinio(info),
		Address: getAddressMinio(info),
	}
}

func getInfosMinio(store *MinioObjectStore) []ObjectInfo {

	doneCh := make(chan struct{})
	defer close(doneCh)

	var infos []ObjectInfo
	for info := range store.Client.ListObjects(store.Bucket, "", true, doneCh) {
		if info.Err != nil {
			log.Println(info.Err)
		}

		objInfo := getInfoMinio(info)

		infos = append(infos, objInfo)
	}

	return infos
}
