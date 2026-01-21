package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	Init     TaskStatus = 6660
	Execute  TaskStatus = 6661
	Failed   TaskStatus = 6662
	Finished TaskStatus = 6663
)

type MapTask struct {
	mu         sync.Mutex
	id         int
	taskStatus TaskStatus
	workerID   string
	filename   string
	expireTime time.Time
}

type ReduceTask struct {
	mu           sync.Mutex
	id           int
	taskStatus   TaskStatus
	workerID     string
	filenameList []string
	expireTime   time.Time
}

type CoordinatorPhase int

const (
	MapPhase    CoordinatorPhase = 7770
	ReducePhase CoordinatorPhase = 7771
	FinishPhase CoordinatorPhase = 7772
)

type Coordinator struct {
	// Your definitions here
	mapTaskList    []*MapTask
	reduceTaskList []*ReduceTask
	nReduce        int

	mu    sync.Mutex
	phase CoordinatorPhase
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RootHandler(send *SendMessage, reply *ReplyMessage) error {
	switch send.MessageType {
	case ApplyForTask:
		return c.applyForTaskHandler(send, reply)
	case HaveFinishedMapTask:
		return c.haveFinishMapTaskHandler(send, reply)
	case HaveFinishedReduceTask:
		return c.haveFinishReduceTaskHandler(send, reply)
	case HaveFailedMapTask:
		return c.haveFailedMapTaskHandler(send, reply)
	case HaveFailedReduceTask:
		return c.haveFailedReduceTaskHandler(send, reply)
	default:
		return fmt.Errorf("[Coordinator.MapReduceHandler] wrong SendMessageType %v", send.MessageType)
	}
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == FinishPhase {
		time.Sleep(3 * time.Second)
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.phase = MapPhase
	c.mapTaskList = make([]*MapTask, len(files))
	c.reduceTaskList = make([]*ReduceTask, nReduce)
	c.nReduce = nReduce
	for i, file := range files {
		c.mapTaskList[i] = &MapTask{
			id:         i,
			taskStatus: Init,
			filename:   file,
			expireTime: time.Now(),
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskList[i] = &ReduceTask{
			id:           i,
			taskStatus:   Init,
			filenameList: []string{},
			expireTime:   time.Now(),
		}
	}
	c.server()
	fmt.Printf("start coordinator")
	return &c
}
