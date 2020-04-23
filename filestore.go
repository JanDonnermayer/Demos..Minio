package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

func countFiles() {
	var files []string
	var hashes [][]byte

	fmt.Println("obtaining files...")

	root := "C:/Users/jan/AppData/Local/Temp/.minio-share/src3"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {

		if !info.IsDir() {

			files = append(files, path)

			f, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			h := sha256.New()
			if _, err := io.Copy(h, f); err != nil {
				log.Fatal(err)
			}

			hashes = append(hashes, h.Sum(nil))
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	println(len(files))
}
