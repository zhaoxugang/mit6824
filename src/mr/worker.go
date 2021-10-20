package mr

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
	// 待执行任务管道
	readyTaskChan chan *Task
}

//
// start a thread that listens for RPCs from worker.go
//
func (s *Slaver) server() {
	rpc.Register(s)
	rpc.HandleHTTP()
	fmt.Printf("start server,slaverName=%s\n", s.SlaverName)
	sockname := slaverSock(s.SlaverName)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (s *Slaver) executeTask(task *Task) (err error) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("调度任务失败，taskId=%d\n", task.TaskMeta.TaskId)
			task.SetStateWithCondition(RUNNING, READY)
			err = errors.New("执行任务失败")
		}
	}()
	if task.TaskMeta.TaskState == READY {
		reply := &TaskProcessReply{}
		// 设置任务状态为执行中
		task.SetStateWithCondition(READY, RUNNING)
		fmt.Println("开始调度===")
		ok := callWorker(s.SlaverName, SLAVER_EXECUTE_TASK, task, reply)
		fmt.Printf("结束调度===,%s\n", ok)
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
	s.readyTaskChan <- task
	return nil
}

func (s *Slaver) taskProcessor() {
	for {
		select {
		case task := <-s.readyTaskChan:
			if task.TaskMeta.TaskType == MAP_TASK {
				s.realExecuteMapTask(task)
			} else if task.TaskMeta.TaskType == REDUCE_TASK {
				fmt.Printf("realExecuteReduceTask,taskId=%d\n", task.TaskMeta.TaskId)
				s.realExecuteReduceTask(task)
			} else {
				fmt.Println("未知任务类型")
			}
		}
	}
}

func (s *Slaver) deleteProcessingTask(id int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, _ := range s.ProcessingTasks {
		if s.ProcessingTasks[i].TaskMeta.TaskId == id {
			fmt.Printf("从执行中列表删除taskId=%d\n", id)
			s.ProcessingTasks = append(s.ProcessingTasks[0:i], s.ProcessingTasks[i+1:]...)
		}
	}
}

func (s *Slaver) addProcessingTask(task *Task) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Printf("将任务添加至执行中列表，taskId=%d,slaverName=%s\n", task.TaskMeta.TaskId, s.SlaverName)
	s.ProcessingTasks = append(s.ProcessingTasks, task)
}

func (s *Slaver) moveTaskToCompletedTask(id int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var task *Task
	for i, _ := range s.ProcessingTasks {
		if s.ProcessingTasks[i].TaskMeta.TaskId == id {
			task = s.ProcessingTasks[i]
			fmt.Printf("将任务迁移值已完成列表taskId=%d\n", task.TaskMeta.TaskId)
			s.ProcessingTasks = append(s.ProcessingTasks[0:i], s.ProcessingTasks[i+1:]...)
			break
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
	//fmt.Println("realExecuteMapTask")
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("执行map异常")
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
		fmt.Printf("{%s}Map处理文件：%s\n", s.SlaverName, task.TaskData[i].Str)
		// 将hash kv
		for i, _ := range kva {
			kv := kva[i]
			// todo 由master指定分片数
			idx := ihash(kv.Key) % task.TaskMeta.PieceNum
			if kv.Key == "AGREE" {
				fmt.Printf("{%s}命中Agree,%s，idx=%d\n", s.SlaverName, kv.Value, idx)
			}
			if s.intermediate == nil {
				fmt.Printf("task.TaskMeta.PieceNum=%d\n", task.TaskMeta.PieceNum)
				s.intermediate = make([][]KeyValue, task.TaskMeta.PieceNum)
			}
			//fmt.Printf("分片索引:%d\n", idx)
			kvs := s.intermediate[idx]
			kvs = append(kvs, kv)
			s.intermediate[idx] = kvs
		}
		fmt.Printf("{%s}执行map成功:%s\n", s.SlaverName, task.TaskData[i].Str)
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
	defer func() {
		fmt.Printf("realExecuteReduceTask执行完成taskId=%d\n", task.TaskMeta.TaskId)
		if err := recover(); err != nil {
			fmt.Printf("执行报错，err=%v\n", err)
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
	oname := fmt.Sprintf("mr-out-%d", task.TaskData[0].Nu)
	fmt.Printf("输出文件：%s\n", oname)
	ofile, _ := os.Create(oname)
	defer func() {
		// 关闭文件
		ofile.Sync()
		ofile.Close()
	}()
	fmt.Printf("reduce任务执行，task_id=%d\n", task.TaskMeta.TaskId)
	kvs, failReason := s.fetchIntermediateKv(task.TaskData[0].Nu, task.Slavers)
	if failReason == SUCCESS {
		kvalues := make(map[string][]string, 0)
		for i, _ := range kvs {
			k := kvs[i].Key
			kvalues[k] = append(kvalues[k], kvs[i].Value)
		}
		fmt.Printf("{%d}reduce执行开始==value=%v\n", task.TaskMeta.TaskId, kvs)
		for k, _ := range kvalues {
			//fmt.Printf("开始执行%s，piece=%d\n", k, task.TaskData[0].Nu)
			if k == "AGREE" {
				fmt.Printf("Reduce命中Agree,%s，key=%v\n", k, kvalues)
			}
			resultStr := s.reducef(k, kvalues[k])
			fmt.Fprintf(ofile, "%v %v\n", k, resultStr)
		}
		fmt.Printf("{%d}reduce执行完成==value=%v\n", task.TaskMeta.TaskId, kvs)
		// 通知Master任务执行成功
		task.TaskMeta.TaskState = COMPLETE
		args := &TaskProcessInfo{
			TaskMeta:   task.TaskMeta,
			SlaverName: s.SlaverName,
		}
		reply := &TaskProcessReply{}
		call(MASTER_TASK_COMPLETED, args, reply)
		path, _ := filepath.Abs(ofile.Name())
		fmt.Printf("reduce输出输出路径：%s\n", path)

	} else {
		// 调用处理master处理失败接口
		fmt.Printf("fetch数据失败，pause任务，taskId=%d\n", task.TaskMeta.TaskId)
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
			fmt.Printf("将任务状态变更为待执行taskId=%d\n", task.TaskMeta.TaskId)
			s.ProcessingTasks = append(s.ProcessingTasks[0:i], s.ProcessingTasks[i+1:]...)
		}
	}
	if task == nil {
		//fmt.Println("未在正在执行中列表中找到对应任务")
		return nil
	}
	// 修改任务状态
	ok := task.SetStateWithCondition(RUNNING, READY)
	if !ok {
		//fmt.Println("修改任务状态失败")
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
		SlaverName:    generateName(),
		mapf:          mapf,
		reducef:       reducef,
		readyTaskChan: make(chan *Task, 100),
	}
	// 启动rpc服务
	slaver.server()
	// 启动map任务processor
	go slaver.taskProcessor()
	// 启动心跳
	slaver.startHeartBeat()
}

func (s *Slaver) FetchMapResult(args *FetchMapResultArgs, reply *FetchMapResultReply) error {
	pieceIdx := args.PieceIdx
	fmt.Printf("MAP:%d\n", len(s.intermediate))
	if pieceIdx < len(s.intermediate) {
		reply.Kvs = s.intermediate[pieceIdx]
	}
	//fmt.Printf("Map返回的kvs：%v\n", reply.Kvs)
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
	resultList := make([]KeyValue, 0)
	for _, slaver := range slaverList {
		//fmt.Printf("开始拉数,%s,%d\n", slaver.SlaverName, pieceIdx)
		args := &FetchMapResultArgs{PieceIdx: pieceIdx}
		reply := &FetchMapResultReply{}
		ok := callWorker(slaver.SlaverName, SLAVER_FETCH_MAP_RESULT, args, reply)
		if ok {
			resultList = append(resultList, reply.Kvs...)
		} else {
			return nil, MAP_CRASH
		}
	}
	//fmt.Printf("{%s}Reduce拉取数据：%d,pieceNum=%d\n", s.SlaverName, resultList, pieceIdx)
	return resultList, SUCCESS
}

func generateName() string {
	f, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
	b := make([]byte, 16)
	f.Read(b)
	f.Close()
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return string(uuid)
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
			//fmt.Printf("slaver心跳,%s\n", s.SlaverName)
			args := &HeartBeatArgs{Name: s.SlaverName}
			s.callHeartBeat(args)
		}
	}
}

func (s *Slaver) callHeartBeat(args *HeartBeatArgs) *HeartBeatReply {
	defer func() {
		if e := recover(); e != nil {
			fmt.Println("worker 心跳失败")
		}
	}()
	reply := &HeartBeatReply{}
	ok := call(MASTER_HEART_BEAT, args, reply)
	if !ok {
		fmt.Println("master is dead")
	}
	if reply.Alive {
		//fmt.Println("master is alive")
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
func maybeCrash() {
	max := big.NewInt(1000)
	os.Exit(1)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 330 {
		// crash!
	} else if rr.Int64() < 660 {
		os.Exit(1)
		// delay for a while.
		maxms := big.NewInt(10 * 1000)
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	}
}

func Map(filename string, contents string) []KeyValue {
	maybeCrash()

	kva := []KeyValue{}
	kva = append(kva, KeyValue{"a", filename})
	kva = append(kva, KeyValue{"b", strconv.Itoa(len(filename))})
	kva = append(kva, KeyValue{"c", strconv.Itoa(len(contents))})
	kva = append(kva, KeyValue{"d", "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
