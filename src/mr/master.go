package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// map slaver名称与信息的映射关系
	mapSlavers map[string]*Slaver
	// reduce slaver名称与信息的映射关系
	reduceSlavers map[string]*Slaver
	// map slaver名称
	mapSlaverNames []string
	// reduce slaver名称
	reduceSlaverNames []string
	// 待指派Map任务队列
	unAssignMapTaskChan chan *Task
	// 待指派Reduce任务队列
	unAssignReduceTaskChan chan *Task
	// map任务完成队列
	mapTaskDoneChan chan *TaskProcessInfo
	// reduce任务完成队列
	reduceTaskDoneChan chan *TaskProcessInfo
	// map任务失败队列
	mapTaskFailChan chan *TaskProcessInfo
	// reduce任务失败队列
	reduceTaskFailChan chan *TaskProcessInfo
	// 用于生成任务id
	GlobalTaskId int
	// map任务结束后生成的分片数
	pieceNum int
	lock     sync.Mutex
	// 执行中的maptask
	unCompletedMapTask map[int]struct{}
	// 执行中的reduceTask
	unCompletedReduceTask map[int]struct{}
	reduceSlaversPaused   bool
}

type MasterServer struct {
	master *Master
}

func (m *Master) init() {
	m.mapSlavers = make(map[string]*Slaver, 1)
	m.reduceSlavers = make(map[string]*Slaver, 1)
	m.unAssignMapTaskChan = make(chan *Task, 100)
	m.unAssignReduceTaskChan = make(chan *Task, 100)
	m.unCompletedMapTask = make(map[int]struct{})
	m.unCompletedReduceTask = make(map[int]struct{})
	m.mapTaskDoneChan = make(chan *TaskProcessInfo, 100)
	m.reduceTaskDoneChan = make(chan *TaskProcessInfo, 100)
	m.mapTaskFailChan = make(chan *TaskProcessInfo, 100)
	m.reduceTaskFailChan = make(chan *TaskProcessInfo, 100)
}

// 心跳
func (ms *MasterServer) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	//fmt.Printf("master心跳,%s\n", args.Name)
	reply.Alive = true
	var slaver *Slaver
	m := ms.master
	// reduce
	if _, ok := m.mapSlavers[args.Name]; !ok {
		slaver = &Slaver{SlaverName: args.Name, State: AVALIABLE}
		m.mapSlaverNames = append(m.mapSlaverNames, args.Name)
		m.addMapSlaver(slaver)
	} else {
		slaver = m.mapSlavers[args.Name]
		slaver.State = AVALIABLE
	}
	slaver.LastActiveTm = time.Now().Unix()
	// map
	if _, ok := m.reduceSlavers[args.Name]; !ok {
		slaver = &Slaver{SlaverName: args.Name, State: AVALIABLE}
		m.reduceSlaverNames = append(m.reduceSlaverNames, args.Name)
		m.addReduceSlaver(slaver)
	} else {
		slaver = m.reduceSlavers[args.Name]
		slaver.State = AVALIABLE
	}
	slaver.LastActiveTm = time.Now().Unix()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (ms *MasterServer) server() {
	rpc.Register(ms)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	isDone := len(m.unAssignReduceTaskChan) == 0 && len(m.unCompletedReduceTask) == 0
	fmt.Printf("Done unAssignReduceTaskChan size=%d, unCompletedReduceTask size=%d,isDone=%s\n", len(m.unAssignReduceTaskChan), len(m.unCompletedReduceTask), isDone)
	return isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := &Master{}
	ms := &MasterServer{
		master: m,
	}
	// 初始化
	m.init()
	// 设置reduce切片数
	m.pieceNum = nReduce
	// 启动rpc server
	ms.server()
	// 校验worker是否存活
	go m.checkAndCleanSlaverAlive()
	// 分配map任务
	go m.assignMapTask()
	// 分配reduce任务
	go m.assignReduceTask()
	// 处理map任务完成请求
	go m.mapTaskCompleted()
	// 处理reduce完成请求
	go m.reduceTaskCompleted()
	// 将任务导入unAssignMapTaskChan中等待分配
	fmt.Println("开始分配Map任务")
	for i, _ := range files {
		task := m.generateMapTask(files[i])
		m.unCompletedMapTask[task.TaskMeta.TaskId] = struct{}{}
		m.unAssignMapTaskChan <- task

		fmt.Println(len(m.unAssignMapTaskChan))
	}
	fmt.Println("开始分配Reduce任务")
	for i := 0; i < m.pieceNum; i++ {
		task := m.generateReduceTask(i)
		m.addUnCompletedReduceTask(task.TaskMeta.TaskId)
		m.unAssignReduceTaskChan <- task
	}
	return m
}

// slaver完成任务时调用的接口
func (ms *MasterServer) TaskCompleted(args *TaskProcessInfo, reply *TaskProcessReply) error {
	m := ms.master
	if args.TaskMeta.TaskType == MAP_TASK {
		m.mapTaskDoneChan <- args
	} else if args.TaskMeta.TaskType == REDUCE_TASK {
		m.reduceTaskDoneChan <- args
	} else {
		fmt.Println("不能识别任务类型")
		return errors.New("不能识别任务类型")
	}
	return nil
}

// map任务完成
func (m *Master) mapTaskCompleted() error {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("mapTaskCompleted报错了,%v\n", e)
		}
	}()
	for {
		select {
		case taskProcessInfo := <-m.mapTaskDoneChan:
			if taskProcessInfo.TaskMeta.TaskState == COMPLETE {
				// 获取slaver注册的信息
				slaver := m.mapSlavers[taskProcessInfo.SlaverName]
				// 将任务从执行中列表表移到已完成队列中
				slaver.moveTaskToCompletedTask(taskProcessInfo.TaskMeta.TaskId)
				// 从未完成map中删除
				delete(m.unCompletedMapTask, taskProcessInfo.TaskMeta.TaskId)
				fmt.Println("unCompletedMapTask size=%d", len(m.unCompletedMapTask))
				if len(m.unCompletedMapTask) == 0 {
					// unpause
					fmt.Println("UnPause1,%d", len(m.reduceSlavers))
					m.unPauseSlavers(m.reduceSlavers)
				}
			}
		}
	}
}

// reduce任务完成
func (m *Master) reduceTaskCompleted() error {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("reduceTaskCompleted报错了,%v\n", e)
		}
	}()
	for {
		select {
		case taskProcessInfo := <-m.reduceTaskDoneChan:
			if taskProcessInfo.TaskMeta.TaskState == COMPLETE {
				// 获取slaver注册的信息
				slaver := m.reduceSlavers[taskProcessInfo.SlaverName]
				fmt.Printf("reduce任务完成，taskId=%d，taskSize=%d\n", taskProcessInfo.TaskMeta.TaskId, len(slaver.ProcessingTasks))
				// 将任务从执行中列表表移到已完成队列中
				slaver.moveTaskToCompletedTask(taskProcessInfo.TaskMeta.TaskId)
				// 将任务从未完成列表中删除
				m.delUnCompletedReduceTask(taskProcessInfo.TaskMeta.TaskId)
			}
		}
	}
}

// task执行失败
func (ms *MasterServer) TaskFail(args *TaskProcessInfo, reply *TaskProcessReply) error {
	m := ms.master
	if args.TaskMeta.TaskType == MAP_TASK {
		m.mapTaskFailChan <- args
	} else if args.TaskMeta.TaskType == REDUCE_TASK {
		m.reduceTaskFailChan <- args
	} else {
		fmt.Println("不能识别任务类型")
		return errors.New("不能识别任务类型")
	}
	return nil
}

// map任务失败
func (m *Master) mapTaskFail() error {
	for {
		select {
		case taskProcessInfo := <-m.mapTaskFailChan:
			if taskProcessInfo.TaskMeta.TaskState == FAILED {
				// 获取slaver注册的信息
				slaver := m.mapSlavers[taskProcessInfo.SlaverName]
				// 将任务从执行中类表弹出
				task := slaver.popTaskToReadyTask(taskProcessInfo.TaskMeta.TaskId)
				m.unCompletedMapTask[task.TaskMeta.TaskId] = struct{}{}
				// 重新指派
				m.unAssignMapTaskChan <- task
			}
		}
	}
}

// reduce任务失败
func (m *Master) reduceTaskFail() error {
	for {
		select {
		case taskProcessInfo := <-m.reduceTaskFailChan:
			if taskProcessInfo.TaskMeta.TaskState == FAILED {
				// 判断是否因为map访问失败导致的
				if taskProcessInfo.TaskFailInfo != nil && taskProcessInfo.TaskFailInfo.FailReason == MAP_CRASH {
					// 因为map slaver crash导致的任务失败
					slaverName := taskProcessInfo.TaskFailInfo.SlaverName
					if slaver, ok := m.mapSlavers[slaverName]; ok {
						// 1.修改slaver的状态，防止别的reduce slaver上报相同异常导致重复处理
						// 2.重新指派该map slaver上的所有map任务
						m.unAvailableReduceSlaver(slaver)
						// 重新指派reduce任务
						taskData := &TaskData{
							Nu: taskProcessInfo.TaskFailInfo.PieceIndex,
						}
						task := &Task{
							TaskMeta: taskProcessInfo.TaskMeta,
							TaskData: []*TaskData{taskData},
						}
						m.unAssignReduceTaskChan <- task
					}
				}
			}
		}
	}
}

// 指派mapTask
func (m *Master) assignMapTask() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("assignMapTask报错了,%v\n", e)
		}
	}()
	for {
		var task *Task
		select {
		case task = <-m.unAssignMapTaskChan:
			slaver := m.getFreeMapSlaver()
			fmt.Printf("assignMapTask,slaver=%v\n", slaver)
			if slaver != nil {
				// 将任务添加到slaver的processing task列表中
				slaver.addProcessingTask(task)
				// map slaver执行任务
				err := slaver.executeTask(task)
				fmt.Printf("调用执行任务成功{%d}，%v\n", task.TaskMeta.TaskState, err)
				if err != nil {
					// 重新加入未指派队列
					m.unAssignMapTaskChan <- task
					// 从slaver的执行中列表删除
					//slaver.deleteProcessingTask(task.TaskMeta.TaskId)
					// 将slaver置为不可用
					m.unAvailableMapSlaver(slaver)
				}
			} else {
				// 重新加入未指派队列
				m.unAssignMapTaskChan <- task
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// 指派reduceTask
func (m *Master) assignReduceTask() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("assignReduceTask报错了,%v\n", e)
		}
	}()
	for {
		var task *Task
		select {
		case task = <-m.unAssignReduceTaskChan:
			for !m.isMapTaskAllDone() {
				// map 任务还未完成继续等待
				time.Sleep(1 * time.Second)
				//fmt.Println("等待map任务完成")
			}
			fmt.Printf("指派reduce任务，taskId=%d\n", task.TaskMeta.TaskId)
			if m.reduceSlaversPaused {
				fmt.Println("Unpause2")
				m.unPauseSlavers(m.reduceSlavers)
			}
			slaver := m.getFreeReduceSlaver()
			fmt.Printf("assignReduceTask,slaver=%v\n", slaver)
			if slaver != nil {
				slavers := make([]*Slaver, 0, len(m.mapSlavers))
				for _, v := range m.mapSlavers {
					slavers = append(slavers, v)
				}
				task.Slavers = slavers
				// 将任务添加到slaver的processing task列表中
				slaver.addProcessingTask(task)
				fmt.Printf("||正在执行中的reduce任务，taskId=%d\n", len(slaver.ProcessingTasks))
				// reduce slaver执行任务
				err := slaver.executeTask(task)
				fmt.Printf("调用执行任务成功，%v\n", err)
				if err != nil {
					// 重新加入未指派队列
					m.unAssignReduceTaskChan <- task
					// 从slaver的执行中列表删除
					//slaver.deleteProcessingTask(task.TaskMeta.TaskId)
					// 将slaver置为不可用
					m.unAvailableReduceSlaver(slaver)
				}
			} else {
				fmt.Printf("重新加入未指派队列,taskId=%d\n", task.TaskMeta.TaskId)
				// 重新加入未指派队列
				m.unAssignReduceTaskChan <- task
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// 找到一个空闲的slaver
func (m *Master) getFreeMapSlaver() *Slaver {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i, _ := range m.mapSlaverNames {
		slaver, ok := m.mapSlavers[m.mapSlaverNames[i]]
		fmt.Printf("{%s}ind Free slaver,state={%d}, processingSize=%d\n",
			slaver.SlaverName, slaver.State, len(slaver.ProcessingTasks))
		if ok && slaver.State == AVALIABLE && len(slaver.ProcessingTasks) == 0 {
			return slaver
		}
	}
	return nil
}

// 找到一个空闲的slaver
func (m *Master) getFreeReduceSlaver() *Slaver {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i, _ := range m.reduceSlaverNames {
		slaver, ok := m.reduceSlavers[m.reduceSlaverNames[i]]
		if ok && slaver.State == AVALIABLE && len(slaver.ProcessingTasks) == 0 {
			return slaver
		}
	}
	return nil
}

// 生成Map任务
func (m *Master) generateMapTask(file string) *Task {
	m.GlobalTaskId++
	taskMeta := &TaskMeta{
		TaskId:    m.GlobalTaskId,
		TaskType:  MAP_TASK,
		TaskState: READY,
		PieceNum:  m.pieceNum,
	}
	taskData := make([]*TaskData, 0, 1)
	taskData = append(taskData, &TaskData{
		Str: file,
	})
	return &Task{
		TaskMeta: taskMeta,
		TaskData: taskData,
	}

}

func (m *Master) checkAndCleanSlaverAlive() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("checkAndCleanSlaverAlive报错了,%v\n", e)
		}
	}()
	for {
		select {
		case <-time.NewTicker(time.Duration(3) * time.Second).C:
			dropList := make([]*Slaver, 0)
			for _, slaver := range m.mapSlavers {
				// 9秒没有活动意味着worker已经挂了
				//fmt.Printf("map slaver 挂了：%d,%d, %d\n", time.Now().Unix(),slaver.LastActiveTm, 9 * time.Second.Seconds())
				if (time.Now().Unix() - slaver.LastActiveTm) > 9 {
					fmt.Printf("map slaver is dead, %v\n", slaver)
					if slaver.State == DROP_ONLY {
						dropList = append(dropList, slaver)
					}
					// 将slaver从列表中删除，并将任务状态加入待指派列表中
					m.unAvailableMapSlaver(slaver)
				}
			}
			m.dropMapSlavers(dropList)

			dropList = dropList[0:0]
			for _, slaver := range m.reduceSlavers {
				fmt.Printf("reduce slaver is alive, %v,taskSize=%d, completedSize=%d\n", slaver.SlaverName, len(slaver.ProcessingTasks), len(slaver.CompleteTasks))
				// 9秒没有活动意味着worker已经挂了
				if (time.Now().Unix() - slaver.LastActiveTm) > 9 {
					fmt.Printf("reduce slaver is dead, %v,taskSize=%d\n", slaver.SlaverName, len(slaver.ProcessingTasks))
					if slaver.State == DROP_ONLY {
						dropList = append(dropList, slaver)
					}
					// 将slaver从列表中删除，并将任务状态加入待指派列表中
					m.unAvailableReduceSlaver(slaver)
				}
			}
			m.dropReduceSlavers(dropList)
		}
	}
}

func (m *Master) unAvailableMapSlaver(slaver *Slaver) {
	slaver.lock.Lock()
	defer slaver.lock.Unlock()
	switch slaver.State {
	case AVALIABLE:
		// 将状态置为DROP_ONLY
		slaver.State = DROP_ONLY
	case DROP_ONLY:
		// 迁移执行中的任务
		for i, _ := range slaver.ProcessingTasks {
			m.unCompletedMapTask[slaver.ProcessingTasks[i].TaskMeta.TaskId] = struct{}{}
			slaver.ProcessingTasks[i].SetStateWithCondition(RUNNING, READY)
			m.unAssignMapTaskChan <- slaver.ProcessingTasks[i]
		}

		// 如果是map则重新执行已完成的任务
		for i, _ := range slaver.CompleteTasks {
			m.unCompletedMapTask[slaver.CompleteTasks[i].TaskMeta.TaskId] = struct{}{}
			slaver.CompleteTasks[i].SetStateWithCondition(COMPLETE, READY)
			m.unAssignMapTaskChan <- slaver.CompleteTasks[i]
		}
	}
}

func (m *Master) unAvailableReduceSlaver(slaver *Slaver) {
	slaver.lock.Lock()
	defer slaver.lock.Unlock()
	switch slaver.State {
	case AVALIABLE:
		// 将状态置为DROP_ONLY
		slaver.State = DROP_ONLY
	case DROP_ONLY:
		// 迁移执行中的任务
		fmt.Printf("##正在执行中的reduce任务，taskId=%d\n", len(slaver.ProcessingTasks))
		for i, _ := range slaver.ProcessingTasks {
			slaver.ProcessingTasks[i].SetStateWithCondition(RUNNING, READY)
			fmt.Printf("重新指派reduce任务，taskId=%d\n", slaver.ProcessingTasks[i].TaskMeta.TaskId)
			m.unAssignReduceTaskChan <- slaver.ProcessingTasks[i]
		}
	}
}

func (m *Master) dropMapSlavers(list []*Slaver) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i, _ := range list {
		delete(m.mapSlavers, list[i].SlaverName)
		idx := -1
		for j, _ := range m.mapSlaverNames {
			if list[i].SlaverName == m.mapSlaverNames[j] {
				idx = j
			}
		}
		if idx >= 0 {
			m.mapSlaverNames = append(m.mapSlaverNames[0:idx], m.mapSlaverNames[idx+1:]...)
		}
	}
}

func (m *Master) dropReduceSlavers(list []*Slaver) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i, _ := range list {
		delete(m.reduceSlavers, list[i].SlaverName)
		idx := -1
		for j, _ := range m.reduceSlaverNames {
			if list[i].SlaverName == m.reduceSlaverNames[j] {
				idx = j
			}
		}
		if idx >= 0 {
			m.reduceSlaverNames = append(m.reduceSlaverNames[0:idx], m.reduceSlaverNames[idx+1:]...)
		}
	}
}

// 校验map任务是否全部完成
func (m *Master) isMapTaskAllDone() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.unCompletedMapTask) == 0
}

func (m *Master) generateReduceTask(piece int) *Task {
	m.GlobalTaskId++
	taskMeta := &TaskMeta{
		// 任务id
		TaskId: m.GlobalTaskId,
		// 任务类型
		TaskType: REDUCE_TASK,
		// 任务状态
		TaskState: READY,
	}
	taskData := &TaskData{
		Nu: piece,
	}
	return &Task{
		TaskMeta: taskMeta,
		TaskData: []*TaskData{taskData},
	}
}

func (m *Master) unPauseSlavers(slavers map[string]*Slaver) error {
	for i, _ := range slavers {
		slaver := slavers[i]
		if slaver.State != AVALIABLE {
			continue
		}
		fmt.Printf("解除pause{%d}，%s\n", slaver.State, slaver.SlaverName)
		args := &UnPauseArgs{}
		reply := &UnPauseReply{}
		ok := callWorker(slaver.SlaverName, SLAVER_UNPAUSE_REDUCE, args, reply)
		if ok {
			fmt.Println("解除pause成功")
		} else {
			fmt.Println("解除pause失败")
			return errors.New("解除pause失败")
		}
	}
	return nil
}

func (m *Master) addUnCompletedReduceTask(taskId int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.unCompletedReduceTask[taskId] = struct{}{}
}

func (m *Master) delUnCompletedReduceTask(taskId int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.unCompletedReduceTask, taskId)
	fmt.Printf("unCompletedReduceTask size:%d\n", len(m.unCompletedReduceTask))
}

func (m *Master) addMapSlaver(slaver *Slaver) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.mapSlavers[slaver.SlaverName] = slaver
}

func (m *Master) addReduceSlaver(slaver *Slaver) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.reduceSlavers[slaver.SlaverName] = slaver
}
