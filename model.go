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
	GetReader(address ObjectAddress) io.ReadCloser
	GetWriter(address ObjectAddress) io.WriteCloser
	GetInfos() []ObjectInfo
}
