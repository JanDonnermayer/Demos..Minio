package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type FsObjectStore struct {
	RootDirectory string
}

func getReaderFS(store *FsObjectStore, address ObjectAddress) (io.ReadCloser, error) {
	path := store.RootDirectory + "/" + address.Route + "/" + address.Key
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func getWriterFS(store *FsObjectStore, address ObjectAddress) (io.WriteCloser, error) {
	path := store.RootDirectory + "/" + address.Route + "/" + address.Key
	return os.Create(path)
}

func getMetaFS(path string, info os.FileInfo) ObjectMeta {

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return ObjectMeta{
		Size: info.Size(),
		ETag: hex.EncodeToString(h.Sum(nil)),
	}
}

func getAddressFS(relPath string) ObjectAddress {
	nonempty := func(s string) bool { return s != "" }
	segments := filter(strings.Split(relPath, "/"), nonempty)

	return ObjectAddress{
		Key:   segments[len(segments)-1],
		Route: strings.Join(segments[:len(segments)-1], "/"),
	}
}

func getInfosFS(store *FsObjectStore) ([]ObjectInfo, error) {
	var infos []ObjectInfo

	err := filepath.Walk(store.RootDirectory, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		normPath := strings.ReplaceAll(path, "\\", "/")
		relPath := strings.ReplaceAll(normPath, store.RootDirectory, "")

		objInfo := ObjectInfo{
			Meta:    getMetaFS(normPath, info),
			Address: getAddressFS(relPath),
		}

		infos = append(infos, objInfo)
		return nil
	})

	return infos, err
}
