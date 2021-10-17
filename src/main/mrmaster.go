package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"mit6824/src/mr"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	//dir := os.Args[1]
	//files, err := ioutil.ReadDir(dir)
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, err.Error())
	//	os.Exit(1)
	//}
	//filePaths := make([]string, 0)
	//for i, _ := range files {
	//	if !files[i].IsDir() {
	//		filePaths = append(filePaths, dir+"/"+files[i].Name())
	//	}
	//}
	//m := mr.MakeMaster(filePaths, 10)
	//for m.Done() == false {
	//	//fmt.Println("完了没")
	//	time.Sleep(time.Second)
	//}
	//
	//time.Sleep(time.Second)
}
