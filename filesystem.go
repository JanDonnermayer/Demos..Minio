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
	segments := strings.Split(relPath, "/")

	return ObjectAddress{
		Key: segments[len(segments)-1],
		Route: segments[:len(segments)-1],
	}
}

func getInfosFS(directory string) ([]ObjectInfo, error) {
	var infos []ObjectInfo

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		relPath := strings.ReplaceAll(path, directory, "")

		objInfo := ObjectInfo {
			Meta:    getMetaFS(path, info),
			Address: getAddressFS(relPath),
		}

		infos = append(infos, objInfo)
		return nil
	})

	return infos, err
}
