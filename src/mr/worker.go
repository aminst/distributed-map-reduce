package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func runPartition(taskId int, intermediate []KeyValue, nReduce int) {
	randomPrefix := rand.Int()

	sort.Sort(ByKey(intermediate))

	for _, kv := range intermediate {
		reduceNum := ihash(kv.Key) % nReduce
		fileName := fmt.Sprintf("temp-%d-%d-%d", randomPrefix, reduceNum, taskId)
		file, _ := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		file.Close()
	}

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("temp-%d-%d-%d", randomPrefix, i, taskId)
		newFileName := fmt.Sprintf("map-%d-%d", i, taskId)
		os.Rename(fileName, newFileName)
	}
}

func runMap(mapf func(string, string) []KeyValue, taskId int, fileName string, nReduce int) {
	intermediate := []KeyValue{}
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
	runPartition(taskId, intermediate, nReduce)
	CallForDoneTask("map", taskId)
}

func readMapIntermediateFile(fileName string) []KeyValue {
	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	for _, line := range strings.Split(string(content), "\n") {
		if len(line) == 0 {
			continue
		}
		kv := KeyValue{}
		fmt.Sscanf(line, "%s %s", &kv.Key, &kv.Value)
		intermediate = append(intermediate, kv)
	}
	return intermediate
}

func runReduce(reducef func(string, []string) string, taskId int) {
	intermediate := []KeyValue{}
	oname := fmt.Sprintf("mr-out-%d", taskId)
	ofile, _ := os.Create(oname)

	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatalf("cannot read directory: %v", err)
	}
	for _, f := range files {
		if strings.HasPrefix(f.Name(), fmt.Sprintf("map-%d-", taskId)) {
			intermediate = append(intermediate, readMapIntermediateFile(f.Name())...)
		}
	}
	sort.Sort(ByKey(intermediate))
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
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	CallForDoneTask("reduce", taskId)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := CallForSendTask()
		if reply.TaskType == "map" {
			runMap(mapf, reply.TaskId, reply.FileName, reply.NReduce)
		} else if reply.TaskType == "reduce" {
			runReduce(reducef, reply.TaskId)
		} else if reply.TaskType == "wait" {
			time.Sleep(time.Second)
			continue
		} else {
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}
}

func CallForSendTask() SendTaskReply {

	args := SendTaskArgs{}
	reply := SendTaskReply{}

	if !call("Coordinator.SendTask", &args, &reply) {
		os.Exit(0)
	}

	return reply
}

func CallForDoneTask(taskType string, taskId int) DoneTaskReply {
	args := DoneTaskArgs{}
	args.TaskType = taskType
	args.TaskId = taskId
	reply := DoneTaskReply{}

	call("Coordinator.DoneTask", &args, &reply)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
