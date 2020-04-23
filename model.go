package main

type ObjectMeta struct {
	Size int64
	ETag string
}

type ObjectAddress struct {
	Key   string
	Route []string
}

type ObjectInfo struct {
	Meta    ObjectMeta
	Address ObjectAddress
}
