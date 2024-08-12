package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 定义排序类型
type ByKey []KeyValue

// 排序三要素
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	loop := true
	for loop {
		re := RequestTask()
		switch re.Answer {
		case TaskGetted:
			task := re.Task
			switch task.TaskType {
			case MapTask:
				fmt.Printf("A worker get a map task and taskId is %d\n", task.TaskId)
				PerformMapTask(mapf, &task)
				FinishTaskAndReport(task.TaskId)
			case ReduceTask:
				fmt.Printf("A worker get a reduce task and taskId is %d\n", task.TaskId)
				PerformReduceTask(reducef, &task)
				FinishTaskAndReport(task.TaskId)
			}
		case WaitPlz:
			time.Sleep(time.Second)
		case FinishAndExit:
			loop = false
		default:
			fmt.Println("request task error!")
		}
	}
}

func RequestTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	//分配任务
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Println("call error")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	//在一个原始连接上依次执行客户端创建一个连接，然后用该连接调用newclient
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

// 执行map任务
func PerformMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}              //map处理后的中间文件
	for _, fileName := range task.InputFile { //map阶段每个worker输入事实上只有一个文件
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()
		kva := mapf(fileName, string(content))
		intermediate = append(intermediate, kva...)
	}
	//将map处理后的中间文件写入磁盘，命名格式为mr-X-Y，其中X为map任务id，
	//Y是ihash(key)%nreduce后的值，这样就建立了key与被分配给的reduce任务的映射关系
	rn := task.ReduceNum
	for i := 0; i < rn; i++ {
		midFileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		midFile, _ := os.Create(midFileName)
		enc := json.NewEncoder(midFile) //创建一个将数据写入w的编码器
		for _, kv := range intermediate {
			if ihash(kv.Key)%rn == i {
				enc.Encode(&kv) // Encode将v的json编码写入输出流，并会写入一个换行符
			}
		}
		midFile.Close()
	}
}

func PerformReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := shuffle(task.InputFile) //洗牌排序
	dir, _ := os.Getwd()
	//临时文件，不断地写入
	tmpFile, err := os.CreateTemp(dir, "mr-out-tmpfile-")
	if err != nil {
		log.Fatal("cannot create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values) //调用reduce函数
		//将结果写入临时文件 显示reduce结果
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFile.Close()
	oname := "mr-out-" + strconv.Itoa(task.ReduceKth)
	os.Rename(dir+tmpFile.Name(), dir+oname) //将临时文件重命名为最终文件
}

func shuffle(files []string) []KeyValue {
	kva := []KeyValue{} //中间文件kv
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			//Decode从输入流读取下一个json编码值并保存在v指向的值里
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva)) //对kva进行排序
	return kva
}

// 通知coordinator任务完成
func FinishTaskAndReport(id int) {
	args := FinArgs{TaskId: id}
	reply := FinReply{}
	ok := call("Coordinator.UpdateTaskState", &args, &reply)
	if !ok {
		log.Println("call failed")
	}
}
