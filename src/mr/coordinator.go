package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Phase int

const (
	PHASE_INIT Phase = iota
	PHASE_MAP
	PHASE_REDUCE
	PHASE_DONE
)

type Coordinator struct {
	phase         Phase
	mapTasks      []mapTask
	reduceTasks   []reduceTask
	heartBeatChan chan *heartBeatMsg
	reportChan    chan *reportMsg
}

type Status int

const (
	STATUS_INVALID Status = iota
	STATUS_TODO
	STATUS_DOING
	STATUS_DONE
)

type mapTask struct {
	id        int
	status    Status
	filename  string
	startTime time.Time
}

type reduceTask struct {
	id        int
	status    Status
	startTime time.Time
}

type heartBeatMsg struct {
	heartBeatResp *HeartBeatResp
	ok            chan struct{}
}

type reportMsg struct {
	reportReq *ReportReq
	ok        chan struct{}
}

func (c *Coordinator) HeartBeat(req *HeartBeatReq, resp *HeartBeatResp) error {
	log.Printf("HeartBeat|handle begin|req=%v", req)
	defer log.Printf("HeartBeat|handle end|req=%v|resp=%v", req, resp)

	heartBeatMsg := &heartBeatMsg{
		heartBeatResp: resp,
		ok:            make(chan struct{}),
	}
	c.heartBeatChan <- heartBeatMsg
	<-heartBeatMsg.ok
	return nil
}

func (c *Coordinator) Report(req *ReportReq, resp *ReportResp) error {
	log.Printf("Report|handle begin|req=%v", req)
	defer log.Printf("Report|handle end|req=%v|resp=%v", req, resp)

	reportMsg := &reportMsg{
		reportReq: req,
		ok:        make(chan struct{}),
	}
	c.reportChan <- reportMsg
	<-reportMsg.ok
	return nil
}

func (c *Coordinator) schedule() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case heartBeatMsg := <-c.heartBeatChan:
			c.handleHeartBeat(heartBeatMsg.heartBeatResp)
			heartBeatMsg.ok <- struct{}{}
		case reportMsg := <-c.reportChan:
			c.handleReport(reportMsg.reportReq)
			reportMsg.ok <- struct{}{}
		case <-ticker.C:
			c.checkTasks()
			if c.phase == PHASE_DONE {
				return
			}
		}
	}
}

func (c *Coordinator) handleHeartBeat(resp *HeartBeatResp) {
	log.Printf("handleHeartBeat|handle begin")
	defer log.Printf("handleHeartBeat|handle end")

	switch c.phase {
	case PHASE_MAP:
		c.getMapTask(resp)
	case PHASE_REDUCE:
		c.getReduceTask(resp)
	case PHASE_DONE:
		resp.Task = TASK_COMPLETE
	default:
		panic(fmt.Sprintf("handleHeartBeat|invalid phase %v", c.phase))
	}
}

func (c *Coordinator) getMapTask(resp *HeartBeatResp) {
	for i := range c.mapTasks {
		if c.mapTasks[i].status != STATUS_TODO {
			continue
		}
		c.mapTasks[i].status = STATUS_DOING
		c.mapTasks[i].startTime = time.Now()
		resp.Task = TASK_MAP
		resp.Id = i + 1
		resp.Filename = c.mapTasks[i].filename
		resp.NMap = len(c.mapTasks)
		resp.NReduce = len(c.reduceTasks)
		log.Printf("getMapTask|get task succ|id=%v|filename=%v", resp.Id, resp.Filename)
		return
	}
	resp.Task = TASK_WAIT
	log.Printf("getMapTask|get task fail|id=%v|filename=%v", resp.Id, resp.Filename)
}

func (c *Coordinator) getReduceTask(resp *HeartBeatResp) {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].status != STATUS_TODO {
			continue
		}
		c.reduceTasks[i].status = STATUS_DOING
		c.reduceTasks[i].startTime = time.Now()
		resp.Task = TASK_REDUCE
		resp.Id = c.reduceTasks[i].id
		resp.NMap = len(c.mapTasks)
		resp.NReduce = len(c.reduceTasks)
		log.Printf("getReduceTask|get task succ|id=%v", resp.Id)
		return
	}
	resp.Task = TASK_WAIT
	log.Printf("getReduceTask|get task fail|id=%v", resp.Id)
}

func (c *Coordinator) handleReport(req *ReportReq) {
	log.Printf("handleReport|handle begin|req=%v", req)
	defer log.Printf("handleReport|handle end|req=%v", req)

	switch req.Task {
	case TASK_MAP:
		c.setMapTaskDone(req)
	case TASK_REDUCE:
		c.setReduceTaskDone(req)
	default:
		log.Printf("handleReport|invalid task %v", req.Task)
	}
}

func (c *Coordinator) setMapTaskDone(req *ReportReq) {
	if req.Id < 1 || req.Id > len(c.mapTasks) {
		log.Printf("setMapTaskDone|invalid id|id=%v", req.Id)
		return
	}
	if c.mapTasks[req.Id-1].status != STATUS_DOING {
		log.Printf("setMapTaskDone|invalid status|id=%v|status=%v", req.Id, c.mapTasks[req.Id-1].status)
		return
	}
	c.mapTasks[req.Id-1].status = STATUS_DONE
	log.Printf("setMapTaskDone|set task done|id=%v", req.Id)

	for i := range c.mapTasks {
		if c.mapTasks[i].status != STATUS_DONE {
			return
		}
	}
	c.phase = PHASE_REDUCE
	log.Printf("setMapTaskDone|map phase done.")
}

func (c *Coordinator) setReduceTaskDone(req *ReportReq) {
	if req.Id < 1 || req.Id > len(c.reduceTasks) {
		log.Printf("setReduceTaskDone|invalid id|id=%v", req.Id)
		return
	}
	if c.reduceTasks[req.Id-1].status != STATUS_DOING {
		log.Printf("setReduceTaskDone|invalid status|id=%v|status=%v", req.Id, c.reduceTasks[req.Id-1].status)
		return
	}
	c.reduceTasks[req.Id-1].status = STATUS_DONE
	log.Printf("setReduceTaskDone|set task done|id=%v", req.Id)

	for i := range c.reduceTasks {
		if c.reduceTasks[i].status != STATUS_DONE {
			return
		}
	}
	c.phase = PHASE_DONE
	log.Printf("setReduceTaskDone|reduce phase done.")
}

func (c *Coordinator) checkTasks() {
	log.Printf("checkTasks|check begin")
	defer log.Printf("checkTasks|check end")

	var timeoutDuration time.Duration = 10 * time.Second
	now := time.Now()
	switch c.phase {
	case PHASE_MAP:
		for i := range c.mapTasks {
			if c.mapTasks[i].status != STATUS_DOING {
				continue
			}
			if now.Sub(c.mapTasks[i].startTime) > timeoutDuration {
				c.mapTasks[i].status = STATUS_TODO
				log.Printf("checkTasks|map task timeout|id=%v|filename=%v", c.mapTasks[i].id, c.mapTasks[i].filename)
			}
		}
	case PHASE_REDUCE:
		for i := range c.reduceTasks {
			if c.reduceTasks[i].status != STATUS_DOING {
				continue
			}
			if now.Sub(c.reduceTasks[i].startTime) > timeoutDuration {
				c.reduceTasks[i].status = STATUS_TODO
				log.Printf("checkTasks|reduce task timeout|id=%v", c.reduceTasks[i].id)
			}
		}
	case PHASE_DONE:
		// do nothing
	default:
		panic(fmt.Sprintf("checkTasks|invalid phase %v", c.phase))
	}
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	return c.phase == PHASE_DONE
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		phase:         PHASE_MAP,
		mapTasks:      make([]mapTask, len(files)),
		reduceTasks:   make([]reduceTask, nReduce),
		heartBeatChan: make(chan *heartBeatMsg),
		reportChan:    make(chan *reportMsg),
	}
	for i, filename := range files {
		c.mapTasks[i] = mapTask{
			id:       i + 1,
			status:   STATUS_TODO,
			filename: filename,
		}
	}
	for i := range c.reduceTasks {
		c.reduceTasks[i] = reduceTask{
			id:     i + 1,
			status: STATUS_TODO,
		}
	}
	go c.schedule()

	c.server()
	return c
}
