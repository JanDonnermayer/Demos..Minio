package main

import (
	// "crypto/sha256"
	"github.com/cespare/xxhash"
	"io"
	"log"
	"os"
	"path/filepath"
)

func getMeta(path string, info os.FileInfo) ObjectMeta {

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return ObjectMeta{
		name: info.Name(),
		size: info.Size(),
		hash: h.Sum(nil),
	}
}

func getMetas(directory string) ([]ObjectMeta, error) {
	var metas []ObjectMeta

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			metas = append(metas, getMeta(path, info))
		}
		return nil
    })
    return metas, err

}
