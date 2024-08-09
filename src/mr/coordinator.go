package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

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
	TaskState int
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
// 生成一个map任务 等待worker的rpc请求
func (c *Coordinator) MakeMapTask(files []string) {

}

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

	return ret
}

// 生成任务唯一id
func (c *Coordinator) GenTaskId() int {
	res := c.TaskIdForGen
	c.TaskIdForGen++
	return res
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

	//监听worker的rpc调用
	c.server()
	return &c
}
