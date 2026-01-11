package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type Task int

const (
	TASK_INVALID Task = iota
	TASK_MAP
	TASK_REDUCE
	TASK_WAIT
	TASK_COMPLETE
)

type HeartBeatReq struct{}

type HeartBeatResp struct {
	Task     Task
	Id       int
	Filename string
	NReduce  int
	NMap     int
}

type ReportReq struct {
	Task Task
	Id   int
}

type ReportResp struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
