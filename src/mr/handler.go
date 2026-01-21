package mr

import (
	"fmt"
	"time"
)

func (c *Coordinator) applyForTaskHandler(send *SendMessage, reply *ReplyMessage) error {
	switch c.phase {
	case MapPhase:
		for _, task := range c.mapTaskList {
			task.mu.Lock()
			if task.taskStatus == Init || task.taskStatus == Failed ||
				task.taskStatus == Execute && time.Now().After(task.expireTime) {
				task.taskStatus = Execute
				task.workerID = GenerateSimpleUniqueID()
				task.expireTime = time.Now().Add(10 * time.Second)

				reply.MessageType = AssignMapTask
				reply.ID = task.id
				reply.WorkerID = task.workerID
				reply.Filename = task.filename
				reply.NReduce = c.nReduce
				task.mu.Unlock()
				return nil
			}
			task.mu.Unlock()
		}
		reply.MessageType = PleaseWait
		return nil
	case ReducePhase:
		for _, task := range c.reduceTaskList {
			task.mu.Lock()
			if task.taskStatus == Init || task.taskStatus == Failed ||
				task.taskStatus == Execute && time.Now().After(task.expireTime) {
				task.taskStatus = Execute
				task.workerID = GenerateSimpleUniqueID()
				task.expireTime = time.Now().Add(10 * time.Second)

				reply.MessageType = AssignReduceTask
				reply.ID = task.id
				reply.WorkerID = task.workerID
				reply.FilenameList = task.filenameList
				task.mu.Unlock()
				return nil
			}
			task.mu.Unlock()
		}
		reply.MessageType = PleaseWait
		return nil
	case FinishPhase:
		reply.MessageType = PleaseQuit
		return nil
	default:
		return fmt.Errorf("[applyForTaskHandler] wrong CoordinatorPhase %v", c.phase)
	}
	return nil
}

func (c *Coordinator) haveFinishMapTaskHandler(send *SendMessage, reply *ReplyMessage) error {
	if send.ID < 0 || send.ID > len(c.mapTaskList) {
		return fmt.Errorf("[Coordinator.haveFinishMapTaskHandler] wrong send.ID %v", send.ID)
	}
	c.mapTaskList[send.ID].mu.Lock()
	if c.mapTaskList[send.ID].workerID != send.WorkerID {
		c.mapTaskList[send.ID].mu.Unlock()
		return fmt.Errorf("[Coordinator.haveFinishMapTaskHandler] invalid HaveFinishedMapTask message, wrong workerID %s, correct %s", send.WorkerID, c.mapTaskList[send.ID].workerID)
	}

	if !(c.mapTaskList[send.ID].taskStatus == Execute && time.Now().Before(c.mapTaskList[send.ID].expireTime)) {
		c.mapTaskList[send.ID].mu.Unlock()
		return fmt.Errorf("[Coordinator.haveFinishMapTaskHandler] invalid HaveFinishedMapTask message")
	}
	c.mapTaskList[send.ID].taskStatus = Finished
	c.mapTaskList[send.ID].mu.Unlock()

	for reduceID, filename := range send.ReduceID2FileName {
		c.reduceTaskList[reduceID].mu.Lock()
		c.reduceTaskList[reduceID].filenameList = append(c.reduceTaskList[reduceID].filenameList, filename)
		c.reduceTaskList[reduceID].mu.Unlock()
	}

	for _, task := range c.mapTaskList {
		task.mu.Lock()
		if task.taskStatus != Finished {
			task.mu.Unlock()
			return nil
		}
		task.mu.Unlock()
	}

	c.mu.Lock()
	c.phase = ReducePhase
	c.mu.Lock()
	return nil
}

func (c *Coordinator) haveFinishReduceTaskHandler(send *SendMessage, reply *ReplyMessage) error {
	if send.ID < 0 || send.ID > len(c.reduceTaskList) {
		return fmt.Errorf("[Coordinator.haveFinishTaskHandler] wrong send.ID %v", send.ID)
	}
	c.reduceTaskList[send.ID].mu.Lock()
	if c.reduceTaskList[send.ID].workerID != send.WorkerID ||
		!(c.reduceTaskList[send.ID].taskStatus == Execute && time.Now().Before(c.reduceTaskList[send.ID].expireTime)) {
		return fmt.Errorf("[Coordinator.haveFinishTaskHandler] invalid HaveFinishedReduceTask message")
	}
	c.reduceTaskList[send.ID].taskStatus = Finished
	c.reduceTaskList[send.ID].mu.Unlock()

	for _, task := range c.reduceTaskList {
		task.mu.Lock()
		if task.taskStatus != Finished {
			task.mu.Unlock()
			return nil
		}
		task.mu.Unlock()
	}
	c.mu.Lock()
	c.phase = FinishPhase
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) haveFailedMapTaskHandler(send *SendMessage, reply *ReplyMessage) error {
	if send.ID < 0 || send.ID > len(c.mapTaskList) {
		return fmt.Errorf("[Coordinator.haveFailedMapTaskHandler] wrong send.ID %v", send.ID)
	}
	c.mapTaskList[send.ID].mu.Lock()
	if c.mapTaskList[send.ID].workerID != send.WorkerID ||
		!(c.mapTaskList[send.ID].taskStatus == Execute && time.Now().Before(c.mapTaskList[send.ID].expireTime)) {
		c.mapTaskList[send.ID].mu.Unlock()
		return fmt.Errorf("[Coordinator.haveFailedMapTaskHandler] invalid HaveFailedMapTaskHandler message")
	}
	c.mapTaskList[send.ID].taskStatus = Failed
	c.mapTaskList[send.ID].mu.Unlock()
	return nil
}

func (c *Coordinator) haveFailedReduceTaskHandler(send *SendMessage, reply *ReplyMessage) error {
	if send.ID < 0 || send.ID > len(c.mapTaskList) {
		return fmt.Errorf("[Coordinator.haveFailedReduceTaskHandler] wrong send.ID %v", send.ID)
	}
	c.reduceTaskList[send.ID].mu.Lock()
	if c.reduceTaskList[send.ID].workerID != send.WorkerID ||
		!(c.reduceTaskList[send.ID].taskStatus == Execute && time.Now().Before(c.reduceTaskList[send.ID].expireTime)) {
		return fmt.Errorf("[Coordinator.haveFailedReduceTaskHandler] invalid HaveFailedReduceTaskHandler message")
	}
	c.reduceTaskList[send.ID].taskStatus = Failed
	c.reduceTaskList[send.ID].mu.Unlock()
	return nil
}
