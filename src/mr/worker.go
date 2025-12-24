package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, resp *GetIdleTaskReply, workerId string) {
	filename := resp.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("worker %s | doMapTask | cannot open file %v", workerId, filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("worker %s | doMapTask | cannot read file %v", workerId, filename)
	}

	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, resp.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % resp.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	for i := 0; i < resp.NReduce; i++ {
		sort.Slice(intermediate[i], func(j, k int) bool {
			return intermediate[i][j].Key < intermediate[i][k].Key
		})
		filename := fmt.Sprintf("mr-%d-%d", resp.TaskId, i)
		tmpfile, err := os.Create(filename)
		if err != nil {
			log.Fatalf("worker %s | doMapTask | cannot create file %v", workerId, filename)
		}
		defer tmpfile.Close()

		encoding := json.NewEncoder(tmpfile)
		for _, kv := range intermediate[i] {
			err := encoding.Encode(kv)
			if err != nil {
				log.Fatalf("worker %s | doMapTask | cannot encode kv %v", workerId, kv)
			}
		}
	}

}

func doReduceTask(reducef func(string, []string) string) {

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var workerId string
	for {
		req := GetUidRequest{}
		reply := GetUidReply{}
		ok := call("Coordinator.GetUid", &req, &reply)
		if ok {
			workerId = reply.Uid
			break
		} else {
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}

	var taskId int = -1
	var taskType TaskType
	var reset bool = false
	var complete bool = false

	go func() {
		for {
			//stage 1: get idle task
			req := GetIdleTaskRequest{
				WorkerId: workerId,
			}
			reply := GetIdleTaskReply{}
			TrySend(func() bool {
				return call("Coordinator.GetIdleTask", &req, &reply)
			}, 100)
			taskId = reply.TaskId
			taskType = reply.TaskType
			if reset {
				continue
			}

			//stage 2: do task
			switch taskType {
			case TypeMap:
				doMapTask(mapf, &reply, workerId)
			case TypeReduce:
				doReduceTask(taskId, taskType, workerId)
			case TypeWait:
				time.Sleep(1 * time.Second)
				continue
			case TypeDone:
				complete = true
				break
			}

			//stage 3: finish task
			finishTaskReq := FinishTaskRequest{
				WorkerId: workerId,
				TaskId:   taskId,
				TaskType: taskType,
			}
			finishTaskReply := FinishTaskReply{}
			TrySend(func() bool {
				return call("Coordinator.FinishTask", &finishTaskReq, &finishTaskReply)
			}, 100)
			reset = finishTaskReply.Reset
			if reset {
				continue
			}
		}
	}()

	for {
		if taskId == -1 {
			continue
		}

		heartbeatReq := HeartbeatRequest{
			WorkerId: workerId,
			TaskId:   taskId,
			TaskType: taskType,
		}
		heartbeatReply := HeartbeatReply{}
		TrySend(func() bool {
			return call("Coordinator.Heartbeat", &heartbeatReq, &heartbeatReply)
		}, 100)
		reset = heartbeatReply.Reset

		if complete {
			break
		}

		time.Sleep(1 * time.Second)
	}

}

func TrySend(fn func() bool, waitTime time.Duration) error {
	for {
		ok := fn()
		if ok {
			return nil
		} else {
			time.Sleep(waitTime * time.Millisecond)
		}
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
