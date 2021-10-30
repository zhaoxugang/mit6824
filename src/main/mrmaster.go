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
	"time"
)

var isStop bool

func dosometh() {
	for {
		if isStop {
			fmt.Println("isStop")
		}
		time.Sleep(1 * time.Second)
		fmt.Println("running")
	}
}

func main() {
	isStop = false
	go dosometh()
	time.Sleep(9 * time.Second)
	fmt.Println("Stop!!")
	isStop = true
	//defer func() {
	//	if e := recover(); e != nil {
	//		fmt.Printf("报错了,%v\n", e)
	//	}
	//}()
	//if len(os.Args) < 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
	//	os.Exit(1)
	//}
	//
	//m := mr.MakeMaster(os.Args[1:], 10)
	//for m.Done() == false {
	//	time.Sleep(time.Second)
	//}
	//fmt.Println("停止了。。。")
	//time.Sleep(time.Second)

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
