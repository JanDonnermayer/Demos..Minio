package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type FsObjectStore struct {
	RootDirectory string
}

func (store FsObjectStore) GetReader(address ObjectAddress) (io.ReadCloser, error) {
	path := filepath.Join(store.RootDirectory, address.Route, address.Key)
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (store FsObjectStore) GetWriter(address ObjectAddress) (io.WriteCloser, error) {

	directory := filepath.Join(store.RootDirectory, address.Route)
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		errDir := os.MkdirAll(directory, os.ModePerm)
		if errDir != nil {
			return nil, err
		}
	}

	path := filepath.Join(directory, address.Key)
	return os.Create(path)
}

func (store FsObjectStore) GetMeta(address ObjectAddress) ObjectMeta {
	path := filepath.Join(store.RootDirectory, address.Route, address.Key)

	stat, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	if stat.IsDir() {
		panic("Path is directory!")
	}

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		panic(err)
	}

	// ToDo: impl large files handling where MD5 is concatenation
	return ObjectMeta{
		Size: stat.Size(),
		ETag: hex.EncodeToString(h.Sum(nil)),
	}
}

func (store FsObjectStore) GetAddress(relPath string) ObjectAddress {
	nonempty := func(s string) bool { return s != "" }
	segments := filter(strings.Split(relPath, "/"), nonempty)

	return ObjectAddress{
		Key:   segments[len(segments)-1],
		Route: strings.Join(segments[:len(segments)-1], "/"),
	}
}


func (store FsObjectStore) GetAddresses() <-chan ObjectAddress {
	resultsCh := make(chan ObjectAddress)

	go func() {
		filepath.Walk(store.RootDirectory, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				normPath := strings.ReplaceAll(path, "\\", "/")
				relPath := strings.ReplaceAll(normPath, store.RootDirectory, "")
				resultsCh <- store.GetAddress(relPath)
			}
			return nil
		})
		close(resultsCh)
	}()

	return resultsCh
}

func (store FsObjectStore) getInfosInternal(addresses <-chan ObjectAddress) <-chan ObjectInfo {
	resultsCh := make(chan ObjectInfo)
	
	go func() {
		for address := range addresses {
			meta := store.GetMeta(address)
			resultsCh <- ObjectInfo{
				Meta:    meta,
				Address: address,
			}
		}
		close(resultsCh)
	}()

	return resultsCh
}

func (store FsObjectStore) GetInfos() <-chan ObjectInfo {
	addresses := store.GetAddresses()

	getInfos := func () <-chan ObjectInfo  {
		return store.getInfosInternal(addresses)
	}

	// Process the addresses using multiple consumers
	return mergeAtomic(
		getInfos(), getInfos(), 
		getInfos(), getInfos(),
	)
}
