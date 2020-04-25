package main

import "io"

type ObjectMeta struct {
	Size int64
	ETag string
}

type ObjectAddress struct {
	Key   string
	Route string
}

type ObjectInfo struct {
	Meta    ObjectMeta
	Address ObjectAddress
}

type ObjectStore interface {
	GetReader(address ObjectAddress) (io.ReadCloser, error)
	GetWriter(address ObjectAddress) (io.WriteCloser, error)
	GetInfos(addressPrefix string) <-chan ObjectInfo
	Delete(address ObjectAddress) error
}
