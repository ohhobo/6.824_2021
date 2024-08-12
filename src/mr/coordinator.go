package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 全局互斥锁
var mu sync.Mutex

// 任务类型
type TaskType int

// 任务状态
type TaskState int

// mapreduce当前状态
type Phase int

// 枚举任务类型
const (
	MapTask TaskType = iota
	ReduceTask
)

// 枚举任务状态
const (
	Working TaskState = iota
	Waiting
	Finished
)

// 枚举mapreduce当前状态
const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type Task struct {
	// Your definitions here.
	TaskId    int
	TaskType  TaskType
	TaskState TaskState
	InputFile []string //map任务输入一个文件，reduce任务输入多个中间文件
	ReduceNum int      //传入多少个reducer
	ReduceKth int      //第几个reduce
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	CurrentPhase   Phase
	TaskIdForGen   int //生成唯一任务id
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	TaskMap        map[int]*Task
	MapNum         int
	ReduceNum      int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//	func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//		reply.Y = args.X + 1
//		return nil
//	}
//
// coordinator分配任务
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	//分配任务上锁防止多个worker竞争，保证并发安全
	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("Coordinator gets a request from worker")

	//根据当前状态分配任务
	switch c.CurrentPhase {
	case MapPhase:
		//有未分配的任务
		if len(c.MapTaskChan) > 0 {
			taskp := <-c.MapTaskChan
			if taskp.TaskState == Waiting {
				reply.Answer = TaskGetted
				reply.Task = *taskp
				taskp.TaskState = Working
				taskp.StartTime = time.Now()
				c.TaskMap[taskp.TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
			}
		} else { //没有未分配的任务，检查map是否执行完，执行完转向reduce
			reply.Answer = WaitPlz
			//检查map是否执行完毕，转到下一阶段
			if c.checkMapTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(c.ReduceTaskChan) > 0 {
			taskp := <-c.ReduceTaskChan
			if taskp.TaskState == Waiting {
				reply.Answer = TaskGetted
				reply.Task = *taskp
				taskp.TaskState = Working
				taskp.StartTime = time.Now()
				c.TaskMap[taskp.TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
			}
		} else { //没有未分配的任务，检查reduce是否执行完，执行完转向alldone
			reply.Answer = WaitPlz
			if c.checkReduceTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case AllDone:
		reply.Answer = FinishAndExit
		fmt.Println("All tasks have been finished!")
	default:
		panic("The phase undefined!!!")
	}
	return nil

}

// coordinator收到worker任务完成rpc，更新任务状态
func (c *Coordinator) UpdateTaskState(args *FinArgs, reply *FinReply) error {
	mu.Lock()
	defer mu.Unlock()
	id := args.TaskId
	fmt.Printf("Task[%d] has been finished.\n", id)
	c.TaskMap[id].TaskState = Finished
	return nil
}

// 生成一个map任务 等待worker的rpc请求
func (c *Coordinator) MakeMapTask(files []string) {
	fmt.Println("begin make a map task")
	for _, file := range files {
		id := c.GenTaskId()
		input := []string{file}
		//生成一个map任务
		mapTask := Task{
			TaskId:    id,
			TaskType:  MapTask,
			TaskState: Waiting,
			InputFile: input,
			ReduceNum: c.ReduceNum,
		}
		fmt.Printf("make a map task %d\n", id)
		c.MapTaskChan <- &mapTask
	}

}

// 生成一个reduce任务
func (c *Coordinator) MakeReduceTask() {
	fmt.Println("begin make a reduce task")
	rn := c.ReduceNum
	//返回当前工作目录的根路径
	dir, _ := os.Getwd()
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < rn; i++ {
		id := c.GenTaskId()
		input := []string{}
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-") &&
				strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				input = append(input, file.Name())
			}
		}
		//生成一个reduce任务
		reduceTask := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			TaskState: Waiting,
			InputFile: input,
			ReduceNum: c.ReduceNum,
			ReduceKth: i,
		}
		fmt.Printf("make a reduce task %d\n", id)
		c.ReduceTaskChan <- &reduceTask
	}

}

// 检查map阶段任务是否完成，如果是则转入reduce阶段
func (c *Coordinator) checkMapTaskDone() bool {
	var (
		mapDoneNum   = 0
		mapUnDoneNum = 0
	)
	for _, v := range c.TaskMap {
		if v.TaskType == MapTask {
			if v.TaskState == Finished {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		}
	}
	//map任务全部做完
	if mapDoneNum == c.MapNum && mapUnDoneNum == 0 {
		return true
	}
	return false
}

func (c *Coordinator) checkReduceTaskDone() bool {
	var (
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range c.TaskMap {
		if v.TaskType == ReduceTask {
			if v.TaskState == Finished {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	//reduce任务全部做完
	if reduceDoneNum == c.ReduceNum && reduceUnDoneNum == 0 {
		return true
	}
	return false
}

// 转向下一个阶段
func (c *Coordinator) toNextPhase() {
	switch c.CurrentPhase {
	case MapPhase:
		c.CurrentPhase = ReducePhase
		c.MakeReduceTask()
	case ReducePhase:
		c.CurrentPhase = AllDone
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *Example
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.CurrentPhase == AllDone {
		ret = true
	}

	return ret
}

// 生成任务唯一id
func (c *Coordinator) GenTaskId() int {
	res := c.TaskIdForGen
	c.TaskIdForGen++
	return res
}

// 认为超过十秒就是崩溃，分配给其他的worker
func (c *Coordinator) CrashHandle() {
	for {
		time.Sleep(2 * time.Second) //每两秒检查一次
		mu.Lock()                   //访问master加锁
		if c.CurrentPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, task := range c.TaskMap {
			if task.TaskState == Working && time.Since(task.StartTime) > 10*time.Second {
				fmt.Printf("Task[%d] is crashed!\n", task.TaskId)
				task.TaskState = Waiting //改为等待状态
				switch task.TaskType {
				case MapTask:
					c.MapTaskChan <- task
				case ReduceTask:
					c.ReduceTaskChan <- task
				}
				delete(c.TaskMap, task.TaskId)
			}
		}
		mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("begin  make a coordinator")
	c := Coordinator{
		CurrentPhase:   MapPhase, //开始为map阶段
		TaskIdForGen:   0,        //开始为0，后面自增
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		TaskMap:        make(map[int]*Task, len(files)+nReduce),
		MapNum:         len(files),
		ReduceNum:      nReduce,
	}

	// Your code here.
	c.MakeMapTask(files)

	//监听worker的rpc调用
	c.server()
	//crash
	go c.CrashHandle()
	return &c
}
