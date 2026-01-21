package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type SendMessageType int

const (
	ApplyForTask           SendMessageType = 9990
	HaveFinishedMapTask    SendMessageType = 9991
	HaveFinishedReduceTask SendMessageType = 9992
	HaveFailedMapTask      SendMessageType = 9993
	HaveFailedReduceTask   SendMessageType = 9994
)

type SendMessage struct {
	MessageType SendMessageType

	// HaveFinishedMapTask && HaveFinishedReduceTask
	// HaveFailedMapTask && HaveFailedReduceTask
	ID       int
	WorkerID string

	// HaveFinishedMapTask
	ReduceID2FileName map[int]string
}

// ***************************************************
type ReplyMessageType int

const (
	AssignMapTask    ReplyMessageType = 8880
	AssignReduceTask ReplyMessageType = 8881
	PleaseWait       ReplyMessageType = 8882
	PleaseQuit       ReplyMessageType = 8883
)

type ReplyMessage struct {
	MessageType ReplyMessageType

	// AssignMapTask && AssignReduceTask
	ID       int
	WorkerID string

	// AssignMapTask
	Filename string
	NReduce  int

	// AssignReduceTask
	FilenameList []string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
