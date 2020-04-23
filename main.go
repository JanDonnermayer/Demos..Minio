package main

import (
	"fmt"
)

func main() {
	root := "C:/Users/jan/AppData/Local/Temp/.minio-share/src3"
	fmt.Println("obtaining files...")

	metas, err := getMetas(root)
	if err != nil {
		panic(err)
	}

	println(len(metas))
}