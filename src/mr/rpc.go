package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// 请求分配任务的reply标识
type RequestReplyFlag int

// 枚举worker请求任务的reply标识
const (
	TaskGetted    RequestReplyFlag = iota //分配任务成功
	WaitPlz                               //没有分配任务
	FinishAndExit                         //工作完成退出
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 获得一个任务，不用传参，coordinator会根据当前mr状态返回具体类型的任务
type TaskArgs struct {
}

type TaskReply struct {
	Answer RequestReplyFlag
	Task   Task
}

type FinArgs struct {
	TaskId int
}

type FinReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
