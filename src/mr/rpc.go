package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"sync"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Master接口名称
const (
	// 任务执行完成
	MASTER_TASK_COMPLETED = "MasterServer.TaskCompleted"
	// 任务执行失败
	MASTER_TASK_FEIL = "MasterServer.TaskFail"
	// 心跳
	MASTER_HEART_BEAT = "MasterServer.HeartBeat"
)

// Slaver接口名称
const (
	// 执行任务
	SLAVER_EXECUTE_TASK     = "Slaver.RealExecuteTask"
	SLAVER_FETCH_MAP_RESULT = "Slaver.FetchMapResult"
	SLAVER_UNPAUSE_REDUCE   = "slaver.UnPauseReduce"
)

type SlaverState int8

const (
	AVALIABLE SlaverState = iota + 1
	DROP_ONLY
	// 因map slaver异常而暂停执行后续任务
	PAUSE
)

type TaskType int16

const (
	MAP_TASK TaskType = iota + 1
	REDUCE_TASK
)

type TaskState int16

const (
	READY TaskState = iota + 1
	RUNNING
	COMPLETE
	FAILED
)

type TaskFailReason int16

const (
	SUCCESS = iota + 1
	MAP_CRASH
	UNKONW
)

type UnPauseArgs struct {
}

type UnPauseReply struct {
}

// 心跳
type HeartBeatArgs struct {
	// worker名称
	Name string
}

type HeartBeatReply struct {
	// master是否可用
	Alive bool
}

// 任务执行信息
type TaskProcessInfo struct {
	// 任务元数据
	TaskMeta *TaskMeta
	// slaver名称
	SlaverName string
	// 任务失败的信息
	TaskFailInfo *TaskFailInfo
}

// 任务执行失败信息
type TaskFailInfo struct {
	// 失败原因
	FailReason TaskFailReason
	// SlaverName
	SlaverName string
	// 失败的pieceIndex
	PieceIndex int
}

// 任务执行返回
type TaskProcessReply struct {
}

// 任务信息
type Task struct {
	// 任务元数据
	TaskMeta *TaskMeta
	// 任务数据
	TaskData []*TaskData
	// slaver列表
	Slavers []*Slaver

	lock sync.Mutex
}

type TaskOperateType int8

// 任务操作信息
type TaskOperation struct {
	// 任务数据
	Task *Task
}

// 拉取map结果数据请求参数
type FetchMapResultArgs struct {
	PieceIdx int
}

//拉取map结果数据响应
type FetchMapResultReply struct {
	Kvs []KeyValue
}

func (t *Task) SetStateWithCondition(expect TaskState, target TaskState) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.TaskMeta.TaskState == expect {
		t.TaskMeta.TaskState = target
		return true
	}
	return false
}

// 任务元信息
type TaskMeta struct {
	// 任务id
	TaskId int
	// 任务类型
	TaskType TaskType
	// 任务状态
	TaskState TaskState
	// 处理结果切片数
	PieceNum int
}

// 任务数据
type TaskData struct {
	Str    string
	Nu     int
	StrArr []string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func slaverSock(workerName string) string {
	s := "/var/tmp/824-mr-"
	s += workerName
	return s
}
