package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus struct {
	isDone    bool
	startTime time.Time
}

type Coordinator struct {
	toBeAssignedMapTaskId    int
	toBeAssignedReduceTaskId int
	mapTaskStatuses          map[int]*TaskStatus
	reduceTaskStatuses       map[int]*TaskStatus
	files                    []string
	nReduce                  int
	mu                       sync.Mutex
}

func (c *Coordinator) getTaskId(taskType string) int {
	taskId := -1
	if taskType == "map" {
		for i := 0; i < len(c.mapTaskStatuses); i++ {
			if !c.mapTaskStatuses[i].isDone && c.mapTaskStatuses[i].startTime.Before(time.Now().Add(-time.Second*10)) {
				taskId = i
				c.mapTaskStatuses[i].startTime = time.Now()
				break
			}
		}
		if taskId == -1 {
			taskId = c.toBeAssignedMapTaskId
			c.toBeAssignedMapTaskId++
		}
	} else {
		for i := 0; i < len(c.reduceTaskStatuses); i++ {
			if !c.reduceTaskStatuses[i].isDone && c.reduceTaskStatuses[i].startTime.Before(time.Now().Add(-time.Second*10)) {
				taskId = i
				c.reduceTaskStatuses[i].startTime = time.Now()
				break
			}
		}
		if taskId == -1 {
			taskId = c.toBeAssignedReduceTaskId
			c.toBeAssignedReduceTaskId++
		}
	}
	return taskId
}

func (c *Coordinator) isMapTasksCompleted() bool {
	for _, v := range c.mapTaskStatuses {
		if !v.isDone {
			return false
		}
	}
	return true
}

func (c *Coordinator) getTask() (string, int, string) {
	var taskType string
	var taskId int
	var fileName string

	c.mu.Lock()
	if c.toBeAssignedMapTaskId < len(c.files) {
		taskType = "map"
		taskId = c.getTaskId("map")
		fileName = c.files[taskId]
		c.mapTaskStatuses[taskId] = &TaskStatus{isDone: false, startTime: time.Now()}
	} else if c.toBeAssignedReduceTaskId < c.nReduce && c.isMapTasksCompleted() {
		taskType = "reduce"
		taskId = c.getTaskId("reduce")
		c.reduceTaskStatuses[taskId] = &TaskStatus{isDone: false, startTime: time.Now()}

	} else if c.toBeAssignedReduceTaskId < c.nReduce && !c.isMapTasksCompleted() {
		taskType = "wait"
	} else {
		taskType = "die"
	}
	c.mu.Unlock()
	return taskType, taskId, fileName
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) SendTask(args *SendTaskArgs, reply *SendTaskReply) error {
	reply.TaskType, reply.TaskId, reply.FileName = c.getTask()
	reply.NReduce = c.nReduce

	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {

	taskType := args.TaskType
	taskId := args.TaskId

	c.mu.Lock()
	if taskType == "map" {
		c.mapTaskStatuses[taskId].isDone = true
	} else if taskType == "reduce" {
		c.reduceTaskStatuses[taskId].isDone = true
	}
	c.mu.Unlock()

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	if c.toBeAssignedMapTaskId == len(c.files) && c.toBeAssignedReduceTaskId == c.nReduce {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.toBeAssignedMapTaskId = 0
	c.toBeAssignedReduceTaskId = 0
	c.files = files
	c.nReduce = nReduce
	c.mapTaskStatuses = make(map[int]*TaskStatus)
	c.reduceTaskStatuses = make(map[int]*TaskStatus)

	c.server()
	return &c
}
