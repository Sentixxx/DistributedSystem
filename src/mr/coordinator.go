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
	mapTasks []Task
	reduceTasks []Task
	phase Phase
}

type TaskStatus int
const (
	StatusIdle TaskStatus = iota
	StatusProcessing
	StatusDone
)


type Task struct {
	id int
	status TaskStatus
	heartbeatTime time.Time
	assignedWorkerId string

	fileName string // for map tasks only
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := true

	// Your code here.


	return ret
}

func (c *Coordinator) GetIdleTask(req *GetIdleTaskRequest, resp *GetIdleTaskReply) error {
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.status == StatusIdle {
			resp.TaskId = task.id
			resp.TaskType = TypeMap
			resp.FileName = task.fileName

			task.assignedWorkerId = req.WorkerId
			task.heartbeatTime = time.Now()
			task.status = StatusProcessing
			return nil
		}
	}

	for _, task := range c.reduceTasks {
		if task.status == StatusIdle {
			resp.TaskId = task.id
			resp.TaskType = TypeReduce

			task.assignedWorkerId = req.WorkerId
			task.heartbeatTime = time.Now()
			task.status = StatusProcessing
			return nil
		}
	}

	return errors.New("no idle task found")
}

func (c* Coordinator) Heartbeat(req *HeartbeatRequest, resp *HeartbeatReply) error {
	if req.TaskType == TypeMap {
		for _, task := range c.mapTasks {
			if task.id == req.TaskId && task.assignedWorkerId == req.WorkerId {
				task.heartbeatTime = time.Now()
				resp.Ok = true
				return nil
			}
		}
	} else if req.TaskType == TypeReduce {
		for _, task := range c.reduceTasks {
			if task.id == req.TaskId && task.assignedWorkerId == req.WorkerId {
				task.heartbeatTime = time.Now()
				resp.Ok = true
				return nil
			}
		}
	} else if req.TaskType == TypeWait || req.TaskType == TypeDone {
		resp.Ok = true
		return nil
	}

	resp.Ok = false
	return errors.New("invalid task type: " + strconv.Itoa(int(req.TaskType)))
}

func (c *Coordinator) scheduleTasks() {
	for {
		time.Sleep(1 * time.Second)

		for _, task := range c.mapTasks {
			if task.status == StatusIdle {
				task.status = StatusProcessing
				task.startTime = time.Now()
			}
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// Init tasks
	c.mapTasks = make([]Task, len(files))
	for i, filename := range files {
		c.mapTasks[i] = Task{
			id: i,
			status: StatusIdle,
			fileName: filename,
		}
	}

	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			id: i,
			status: StatusIdle, 
		}
	}

	go c.scheduleTasks()

	

	c.server()
	return &c
}
