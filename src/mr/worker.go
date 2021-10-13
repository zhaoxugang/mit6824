package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Slaver struct {
	SlaverName string
	// 正在处理中的任务
	ProcessingTasks []*Task
	CompleteTasks   []*Task
	reduceTaskCh    chan *Task
	// 上一次活跃时间
	LastActiveTm int64
	lock         sync.Mutex
	State        SlaverState

	mapf         func(string, string) []KeyValue
	reducef      func(string, []string) string
	intermediate [][]KeyValue
}

//
// start a thread that listens for RPCs from worker.go
//
func (s *Slaver) server() {
	rpc.Register(s)
	rpc.HandleHTTP()
	sockname := slaverSock(s.SlaverName)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (s *Slaver) executeTask(task *Task) error {
	if task.TaskMeta.TaskState == READY {
		reply := &TaskProcessReply{}
		// 设置任务状态为执行中
		task.SetStateWithCondition(READY, RUNNING)
		ok := callWorker(s.SlaverName, SLAVER_EXECUTE_TASK, task, reply)
		if ok {
			return nil
		} else {
			task.SetStateWithCondition(RUNNING, READY)
			return errors.New("执行任务失败")
		}
	} else {
		fmt.Println("任务已经在执行中请勿重复指派")
		return nil
	}
}

func (s *Slaver) RealExecuteTask(task *Task, reply *TaskProcessReply) error {
	if task.TaskMeta.TaskType == MAP_TASK {
		s.realExecuteMapTask(task)
	} else if task.TaskMeta.TaskType == REDUCE_TASK {
		s.realExecuteReduceTask(task)
	} else {
		fmt.Println("未知任务类型")
	}

	return nil
}

func (s *Slaver) deleteProcessingTask(id int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, _ := range s.ProcessingTasks {
		if s.ProcessingTasks[i].TaskMeta.TaskId == id {
			s.ProcessingTasks = append(s.ProcessingTasks[0:i], s.ProcessingTasks[i+1:]...)
		}
	}
}

func (s *Slaver) addProcessingTask(task *Task) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ProcessingTasks = append(s.ProcessingTasks, task)
}

func (s *Slaver) moveTaskToCompletedTask(id int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var task *Task
	for i, _ := range s.ProcessingTasks {
		if s.ProcessingTasks[i].TaskMeta.TaskId == id {
			task = s.ProcessingTasks[i]
			s.ProcessingTasks = append(s.ProcessingTasks[0:i], s.ProcessingTasks[i+1:]...)
		}
	}
	if task == nil {
		fmt.Println("未在正在执行中列表中找到对应任务")
		return
	}
	isExists := false
	for i, _ := range s.CompleteTasks {
		if s.CompleteTasks[i].TaskMeta.TaskId == task.TaskMeta.TaskId {
			isExists = true
			break
		}
	}
	if !isExists {
		s.CompleteTasks = append(s.CompleteTasks, task)
	}
	// 修改任务状态
	ok := task.SetStateWithCondition(RUNNING, COMPLETE)
	if !ok {
		fmt.Println("修改任务状态失败")
	}
}

func (s *Slaver) realExecuteMapTask(task *Task) {
	fmt.Println("realExecuteMapTask")
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			// 调用处理master处理失败接口
			task.TaskMeta.TaskState = FAILED
			args := &TaskProcessInfo{
				TaskMeta:   task.TaskMeta,
				SlaverName: s.SlaverName,
			}
			reply := &TaskProcessReply{}
			call(MASTER_TASK_FEIL, args, reply)
		}
	}()
	for i, _ := range task.TaskData {
		// 获取文件路径
		filename := task.TaskData[i].Str
		// 读取文件内容
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := s.mapf(filename, string(content))
		// 将hash kv
		for i, _ := range kva {
			kv := kva[i]
			// todo 由master指定分片数
			idx := ihash(kv.Key) % task.TaskMeta.PieceNum
			if s.intermediate == nil {
				s.intermediate = make([][]KeyValue, task.TaskMeta.PieceNum)
			}
			kvs := s.intermediate[idx]
			kvs = append(kvs, kv)
			s.intermediate[idx] = kvs
		}
	}
	// 通知Master任务执行成功
	task.TaskMeta.TaskState = COMPLETE
	args := &TaskProcessInfo{
		TaskMeta:   task.TaskMeta,
		SlaverName: s.SlaverName,
	}
	reply := &TaskProcessReply{}
	call(MASTER_TASK_COMPLETED, args, reply)
}

// 执行reduce任务
func (s *Slaver) realExecuteReduceTask(task *Task) {
	fmt.Println("realExecuteReduceTask")
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			// 调用处理master处理失败接口
			task.TaskMeta.TaskState = FAILED
			taskFailInfo := &TaskFailInfo{
				PieceIndex: task.TaskData[0].Nu,
				FailReason: UNKONW,
			}
			args := &TaskProcessInfo{
				TaskMeta:     task.TaskMeta,
				SlaverName:   s.SlaverName,
				TaskFailInfo: taskFailInfo,
			}
			reply := &TaskProcessReply{}
			call(MASTER_TASK_FEIL, args, reply)
		}
	}()
	kvs, failReason := s.fetchIntermediateKv(task.TaskData[0].Nu, task.Slavers)
	if failReason == SUCCESS {
		kvalues := make(map[string][]string, 0)
		for i, _ := range kvs {
			k := kvs[i].Key
			kvalues[k] = append(kvalues[k], kvs[i].Key)
		}
		for k, _ := range kvalues {
			resultStr := s.reducef(k, kvalues[k])
			fmt.Print(resultStr)
		}
		// 通知Master任务执行成功
		task.TaskMeta.TaskState = COMPLETE
		args := &TaskProcessInfo{
			TaskMeta:   task.TaskMeta,
			SlaverName: s.SlaverName,
		}
		reply := &TaskProcessReply{}
		call(MASTER_TASK_COMPLETED, args, reply)
	} else {
		// 调用处理master处理失败接口
		task.TaskMeta.TaskState = FAILED
		taskFailInfo := &TaskFailInfo{
			PieceIndex: task.TaskData[0].Nu,
			FailReason: failReason,
		}
		args := &TaskProcessInfo{
			TaskMeta:     task.TaskMeta,
			SlaverName:   s.SlaverName,
			TaskFailInfo: taskFailInfo,
		}
		reply := &TaskProcessReply{}
		s.State = PAUSE
		call(MASTER_TASK_FEIL, args, reply)
	}
}

func (s *Slaver) popTaskToReadyTask(taskId int) *Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	var task *Task
	for i, _ := range s.ProcessingTasks {
		if s.ProcessingTasks[i].TaskMeta.TaskId == taskId {
			task = s.ProcessingTasks[i]
			s.ProcessingTasks = append(s.ProcessingTasks[0:i], s.ProcessingTasks[i+1:]...)
		}
	}
	if task == nil {
		fmt.Println("未在正在执行中列表中找到对应任务")
		return nil
	}
	// 修改任务状态
	ok := task.SetStateWithCondition(RUNNING, READY)
	if !ok {
		fmt.Println("修改任务状态失败")
		return nil
	} else {
		return task
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	slaver := &Slaver{
		SlaverName: generateName(),
		mapf:       mapf,
		reducef:    reducef,
	}
	// 启动rpc服务
	slaver.server()
	// 启动心跳
	slaver.startHeartBeat()
}

func (s *Slaver) FetchMapResult(args *FetchMapResultArgs, reply *FetchMapResultReply) error {
	pieceIdx := args.PieceIdx
	reply.Kvs = s.intermediate[pieceIdx]
	return nil
}

// 解除pause
func (s *Slaver) UnPauseReduce(args *UnPauseArgs, reply *UnPauseReply) error {
	s.lock.Lock()
	s.lock.Unlock()
	s.State = AVALIABLE
	return nil
}

func (s *Slaver) fetchIntermediateKv(pieceIdx int, slaverList []*Slaver) ([]KeyValue, TaskFailReason) {
	for _, slaver := range slaverList {
		args := &FetchMapResultArgs{PieceIdx: pieceIdx}
		reply := &FetchMapResultReply{}
		ok := callWorker(slaver.SlaverName, SLAVER_FETCH_MAP_RESULT, args, reply)
		if ok {
			return reply.Kvs, SUCCESS
		} else {
			return nil, MAP_CRASH
		}
	}
	return nil, SUCCESS
}

func generateName() string {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	bytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func (s *Slaver) startHeartBeat() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("recover %v\n", err)
		}
	}()
	for {
		select {
		case <-time.NewTicker(time.Duration(1) * time.Second).C:
			args := &HeartBeatArgs{Name: s.SlaverName}
			s.callHeartBeat(args)
		}
	}
}

func (s *Slaver) callHeartBeat(args *HeartBeatArgs) *HeartBeatReply {

	reply := &HeartBeatReply{}
	ok := call(MASTER_HEART_BEAT, args, reply)
	if !ok {
		fmt.Println("master is dead")
	}
	if reply.Alive {
		fmt.Println("master is alive")
	} else {
		fmt.Println("master is dead")
	}
	return reply
}

func (s *Slaver) setStateWithCondition(avaliable SlaverState, only SlaverState) {

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func callWorker(workerName string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := slaverSock(workerName)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//==========测试=======
func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
