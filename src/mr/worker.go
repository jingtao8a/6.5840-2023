package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	retry := 0
	for true {
		send := SendMessage{}
		reply := ReplyMessage{}
		send.MessageType = ApplyForTask
		var err error
		var reduceID2FileName map[int]string
		ok := call("Coordinator.RootHandler", &send, &reply)
		if !ok {
			fmt.Println("[Worker] call failed")
			retry++
			if retry >= 5 {
				break
			}
			continue
		}
		retry = 0
		switch reply.MessageType {
		case AssignMapTask:
			err, reduceID2FileName = doMapTask(&reply, mapf)
			if err != nil {
				failMapTask(&reply)
			} else {
				finishMapTask(&reply, reduceID2FileName)
			}
		case AssignReduceTask:
			err = doReduceTask(&reply, reducef)
			if err != nil {
				failReduceTask(&reply)
			} else {
				finishReduceTask(&reply)
			}
		case PleaseWait:
			fmt.Printf("wait \n")
			time.Sleep(time.Second)
		case PleaseQuit:
			return
		default:
			fmt.Println("[Worker] wrong reply.Message %v", reply.MessageType)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMapTask(reply *ReplyMessage, mapf func(string, string) []KeyValue) (error, map[int]string) {
	var err error
	var file *os.File
	file, err = os.Open(reply.Filename)
	if err != nil {
		fmt.Printf("[doMapTask] cannot open %v \n", reply.Filename)
		return fmt.Errorf("[doMapTask] cannot open %v", reply.Filename), nil
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("[doMapTask] cannot read %v \n", reply.Filename)
		return fmt.Errorf("[doMapTask] cannot read %v", reply.Filename), nil
	}
	err = file.Close()
	if err != nil {
		fmt.Printf("[doMapTask] file close failed %v \n", reply.Filename)
		return fmt.Errorf("[doMapTask] file close failed %v", reply.Filename), nil
	}
	kva := mapf(reply.Filename, string(content))

	reduceID2FileSocket := make(map[int]*os.File)
	reduceID2Encoders := make(map[int]*json.Encoder)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reply.NReduce
		if _, ok := reduceID2FileSocket[reduceID]; !ok {
			reduceID2FileSocket[reduceID], err = os.CreateTemp("./", fmt.Sprintf("tmp-%d-%d-*", reply.ID, reduceID))
			if err != nil {
				fmt.Printf("[doMapTask] create tempfile failed\n")
				return fmt.Errorf("[doMapTask] create tempfile failed"), nil
			}
		}
		if _, ok := reduceID2Encoders[reduceID]; !ok {
			reduceID2Encoders[reduceID] = json.NewEncoder(reduceID2FileSocket[reduceID])
		}
		err = reduceID2Encoders[reduceID].Encode(&kv)
		if err != nil {
			fmt.Printf("[doMapTask] json encode kv failed\n")
			return fmt.Errorf("[doMapTask] json encode kv failed"), nil
		}
	}
	reduceID2FileName := make(map[int]string)
	for reduceID, fileSocket := range reduceID2FileSocket {
		newName := fmt.Sprintf("mr-%d-%d", reply.ID, reduceID)
		err = os.Rename(fileSocket.Name(), newName)
		if err != nil {
			fmt.Printf("[doMapTask] rename temp file failed\n")
			return fmt.Errorf("[doMapTask] rename temp file failed"), nil
		}
		err = fileSocket.Close()
		if err != nil {
			fmt.Printf("[doMapTask] file close fail %v\n", fileSocket.Name())
			return fmt.Errorf("[doMapTask] file close fail %v", fileSocket.Name()), nil
		}
		reduceID2FileName[reduceID] = newName
	}
	return nil, reduceID2FileName
}

func doReduceTask(reply *ReplyMessage, reducef func(string, []string) string) error {
	var err error
	var file *os.File
	intermediate := []KeyValue{}
	for _, fileName := range reply.FilenameList {
		file, err = os.Open(fileName)
		if err != nil {
			fmt.Printf("[doReduceTask] cannot open %v\n", fileName)
			return fmt.Errorf("[doReduceTask] cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		err = file.Close()
		if err != nil {
			fmt.Printf("[doReduceTask] file close failed %v \n", reply.Filename)
			return fmt.Errorf("[doReduceTask] file close failed %v", reply.Filename)
		}
	}
	sort.Sort(ByKey(intermediate))

	var tempFileSocket *os.File
	tempFileSocket, err = os.CreateTemp("./", fmt.Sprintf("tmp-%d-*", reply.ID))
	if err != nil {
		fmt.Printf("[doReduceTask] create tempfile failed\n")
		return fmt.Errorf("[doReduceTask] create tempfile failed")
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(tempFileSocket, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			fmt.Printf("[doReduceTask] write tempFileSocket faile\n")
			return fmt.Errorf("[doReduceTask] write tempFileSocket faile")
		}
		i = j
	}
	newName := fmt.Sprintf("mr-out-%d", reply.ID)
	err = os.Rename(tempFileSocket.Name(), newName)
	if err != nil {
		fmt.Printf("[doReduceTask] rename temp file failed\n")
		return fmt.Errorf("[doReduceTask] rename temp file failed")
	}
	err = tempFileSocket.Close()
	if err != nil {
		fmt.Printf("[doReduceTask] file close failed %v \n", tempFileSocket.Name())
		return fmt.Errorf("[doReduceTask] file close failed %v", tempFileSocket.Name())
	}
	return nil
}

func failMapTask(reply *ReplyMessage) {
	send := SendMessage{}
	send.MessageType = HaveFailedMapTask
	send.ID = reply.ID
	send.WorkerID = reply.WorkerID
	ok := call("Coordinator.RootHandler", &send, nil)
	if !ok {
		fmt.Println("[failMapTask] call failed")
	}
}

func finishMapTask(reply *ReplyMessage, reduceID2FileName map[int]string) {
	send := SendMessage{}
	send.MessageType = HaveFinishedMapTask
	send.ID = reply.ID
	send.WorkerID = reply.WorkerID
	send.ReduceID2FileName = reduceID2FileName

	ok := call("Coordinator.RootHandler", &send, nil)
	if !ok {
		fmt.Println("[finishMapTask] call failed")
	}
}

func failReduceTask(reply *ReplyMessage) {
	send := SendMessage{}
	send.MessageType = HaveFailedReduceTask
	send.ID = reply.ID
	send.WorkerID = reply.WorkerID

	ok := call("Coordinator.RootHandler", &send, nil)
	if !ok {
		fmt.Println("[failReduceTask] call failed")
	}
}

func finishReduceTask(reply *ReplyMessage) {
	send := SendMessage{}
	send.MessageType = HaveFinishedReduceTask
	send.ID = reply.ID
	send.WorkerID = reply.WorkerID

	ok := call("Coordinator.RootHandler", &send, nil)
	if !ok {
		fmt.Println("[finishReduceTask] call failed")
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
