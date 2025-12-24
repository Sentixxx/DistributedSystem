package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
	PhaseDone
)

type Coordinator struct {
	mapTasks    []Task
	reduceTasks []Task
	phase       Phase
	workerId int
	taskChan chan TaskEvent
}

type TaskEventType int

const (
	TaskEventAssign TaskEventType = iota
	TaskEventHeartbeat
	TaskEventComplete
	TaskEventInit
)

type TaskEvent struct {
	TaskEventType TaskEventType
	TaskId        int
	TaskType      TaskType
	WorkerId      string
	Reply         interface{} // for complete events

	ok chan struct{}
}

type TaskStatus int

const (
	StatusIdle TaskStatus = iota
	StatusProcessing
	StatusDone
)

type Task struct {
	id               int
	status           TaskStatus
	heartbeatTime    time.Time
	assignedWorkerId string

	fileName string // for map tasks only
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
	return c.phase == PhaseDone
}

func (c *Coordinator) GetIdleTask(req *GetIdleTaskRequest, resp *GetIdleTaskReply) error {
	taskEvent := TaskEvent{
		TaskEventType: TaskEventAssign,
		WorkerId:      req.WorkerId,
		Reply:         &resp,
		ok:            make(chan struct{}),
	}
	c.taskChan <- taskEvent
	<-taskEvent.ok
	return nil
}

func (c *Coordinator) GetUid(req *GetUidRequest, resp *GetUidReply) error {
	taskEvent := TaskEvent {
		TaskEventType: TaskEventInit,
		Reply: &resp,
		ok: make(chan struct{}),
	}
	c.taskChan <- taskEvent
	<-taskEvent.ok

	return nil
}

// Heartbeat handles RPC request to update task heartbeat
func (c *Coordinator) Heartbeat(req *HeartbeatRequest, resp *HeartbeatReply) error {
	log.Printf("Coordinator: Heartbeat received from worker %s for task %d of type %s", req.WorkerId, req.TaskId, req.TaskType)
	defer log.Printf("Coordinator: Hearbeat finish from from worker %s for task %d of type %s", req.WorkerId, req.TaskId, req.TaskType)

	taskEvent := TaskEvent{
		TaskEventType: TaskEventHeartbeat,
		TaskId:        req.TaskId,
		TaskType:      req.TaskType,
		WorkerId:      req.WorkerId,
		Reply: &resp,
		ok:            make(chan struct{}),
	}
	c.taskChan <- taskEvent
	<-taskEvent.ok

	return nil
}


func (c *Coordinator) assignIdleTask(workerId string, resp *GetIdleTaskReply) (*GetIdleTaskReply, error) {
	if c.phase == PhaseMap {
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.status == StatusIdle {
				task.assignedWorkerId = workerId
				task.status = StatusProcessing
				task.heartbeatTime = time.Now()

				resp.TaskId = task.id
				resp.TaskType = TypeMap
				resp.FileName = task.fileName
				return resp, nil
			}
		}
	} else if c.phase == PhaseReduce {
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.status == StatusIdle {
				task.assignedWorkerId = workerId
				task.status = StatusProcessing
				task.heartbeatTime = time.Now()

				resp.TaskId = task.id
				resp.TaskType = TypeReduce
				return resp, nil
			}
		}
	}

	return resp, nil
}

func (c *Coordinator) checkTasks() {
	now := time.Now()
	timeoutS := 10 * time.Second
	done := true
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.status != StatusDone {
			done = false
		}
		if task.status == StatusProcessing && now.Sub(task.heartbeatTime) > timeoutS {
			task.status = StatusIdle
			task.assignedWorkerId = ""
		}
	}

	for i := range c.reduceTasks {
		task := &c.reduceTasks[i]
		if task.status != StatusDone {
			done = false
		}
		if task.status == StatusProcessing && now.Sub(task.heartbeatTime) > timeoutS {
			task.status = StatusIdle
			task.assignedWorkerId = ""
		}
	}

	if done {
		c.phase = PhaseDone
	}
}

func (c *Coordinator) completeTask(workerId string, taskId int, taskType TaskType, resp *FinishTaskReply) error {
	if taskType == TypeMap {
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.id == taskId {
				if workerId == task.assignedWorkerId {
					task.status = StatusDone
					resp.Reset = false
				} else {
					resp.Reset = true
				}
			}
		}
	} else if taskType == TypeReduce {
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.id == taskId {
				if workerId == task.assignedWorkerId {
					task.status = StatusDone
					resp.Reset = false
				} else {
					resp.Reset = true
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) eventLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkTasks()

		case event := <-c.taskChan:
			switch event.TaskEventType {
			case TaskEventAssign:
				resp := event.Reply.(*GetIdleTaskReply)
				c.assignIdleTask(event.WorkerId, resp)
				close(event.ok)
			case TaskEventHeartbeat:
				resp := event.Reply.(*HeartbeatReply)
				c.updateHeartbeat(event.WorkerId, event.TaskId, event.TaskType, resp)
				close(event.ok)
			case TaskEventComplete:
				resp := event.Reply.(*FinishTaskReply)
				c.completeTask(event.WorkerId, event.TaskId, event.TaskType, resp)
				close(event.ok)
			case TaskEventInit:
				resp := event.Reply.(*GetUidReply)
				resp.Uid = strconv.Itoa(c.workerId)
				c.workerId++
				close(event.ok)
			}
		}
	}
}



// Internal method to update heartbeat (runs in a single goroutine)
func (c *Coordinator) updateHeartbeat(workerId string, taskId int, taskType TaskType, resp *HeartbeatReply) (bool, error) {
	if taskType == TypeMap {
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.id == taskId {
				if task.assignedWorkerId == workerId {
					task.heartbeatTime = time.Now()
					return true, nil

				} else {
					// 当前任务被分配给了其他 worker
					return false, nil
				}
			}
		}
	} else if taskType == TypeReduce {
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.id == taskId {
				if task.assignedWorkerId == workerId {
					task.heartbeatTime = time.Now()
					return true, nil

				} else {
					// 当前任务被分配给了其他 worker
					return false, nil
				}
			}
		}
	} else if taskType == TypeWait || taskType == TypeDone {
		return true, nil
	}

	return false, errors.New("invalid task type")
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workerId: 0,
		mapTasks: make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		taskChan: make(chan TaskEvent),
	}

	// Init tasks
	for i, filename := range files {
		c.mapTasks[i] = Task{
			id:       i,
			status:   StatusIdle,
			fileName: filename,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			id:     i,
			status: StatusIdle,
		}
	}

	// Initialize operation channel and start handler
	// This handler will serialize all operations that access shared state
	go c.eventLoop()

	c.server()
	return &c
}
