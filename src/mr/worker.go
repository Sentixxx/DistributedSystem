package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

		// Use atomic write: create temp file, write, then rename
		dir, _ := filepath.Split(filename)
		if dir == "" {
			dir = "."
		}
		tmpfile, err := os.CreateTemp(dir, "mr-tmp-*")
		if err != nil {
			log.Fatalf("worker %s | doMapTask | cannot create temp file %v", workerId, filename)
		}

		encoding := json.NewEncoder(tmpfile)
		for _, kv := range intermediate[i] {
			err := encoding.Encode(kv)
			if err != nil {
				tmpfile.Close()
				os.Remove(tmpfile.Name())
				log.Fatalf("worker %s | doMapTask | cannot encode kv %v", workerId, kv)
			}
		}

		// Close file before renaming to ensure data is flushed
		if err := tmpfile.Close(); err != nil {
			os.Remove(tmpfile.Name())
			log.Fatalf("worker %s | doMapTask | cannot close temp file %v", workerId, tmpfile.Name())
		}

		// Atomic rename: only visible after complete write
		if err := os.Rename(tmpfile.Name(), filename); err != nil {
			os.Remove(tmpfile.Name())
			log.Fatalf("worker %s | doMapTask | cannot rename file %v", workerId, filename)
		}
	}

}

func doReduceTask(reducef func(string, []string) string, resp *GetIdleTaskReply, workerId string) string {

	outputFilename := fmt.Sprintf("mr-out-%d", resp.TaskId)

	intermediate := make([]KeyValue, 0)
	for i := 0; i < resp.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, resp.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			// File might not exist if map task failed or not completed yet
			continue
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Group by key
	result := make(map[string][]string)
	for _, kv := range intermediate {
		result[kv.Key] = append(result[kv.Key], kv.Value)
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	filename := outputFilename

	dir, _ := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf("worker %s | doReduceTask | cannot create temp file %v", workerId, filename)
	}

	// Write output in sorted order
	for _, key := range keys {
		output := reducef(key, result[key])
		fmt.Fprintf(f, "%v %v\n", key, output)
	}

	// Close file before renaming
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		log.Fatalf("worker %s | doReduceTask | cannot close temp file %v", workerId, f.Name())
	}

	// Copy file permissions if target file exists
	if info, err := os.Stat(filename); err == nil {
		if err := os.Chmod(f.Name(), info.Mode()); err != nil {
			os.Remove(f.Name())
			log.Fatalf("worker %s | doReduceTask | cannot chmod file %v", workerId, filename)
		}
	}

	return f.Name()
}

type TaskContext struct {
	TaskId   int
	TaskType TaskType
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

	taskCh := make(chan TaskContext)
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

			task := TaskContext{
				TaskId:   reply.TaskId,
				TaskType: reply.TaskType,
			}

			var tmpFile string
			//stage 2: do task
			switch task.TaskType {
			case TypeMap:
				taskCh <- task
				doMapTask(mapf, &reply, workerId)
			case TypeReduce:
				taskCh <- task
				tmpFile = doReduceTask(reducef, &reply, workerId)
			case TypeWait:
				time.Sleep(1 * time.Second)
				continue
			case TypeDone:
				complete = true
				break
			}

			//stage 3: finish task (only for actual tasks, not TypeWait or TypeDone)
			if task.TaskType == TypeMap || task.TaskType == TypeReduce {
				finishTaskReq := FinishTaskRequest{
					WorkerId: workerId,
					TaskId:   task.TaskId,
					TaskType: task.TaskType,
				}
				finishTaskReply := FinishTaskReply{}
				TrySend(func() bool {
					return call("Coordinator.FinishTask", &finishTaskReq, &finishTaskReply)
				}, 100)
				reset = finishTaskReply.Reset
				if reset {
					if task.TaskType == TypeReduce {
						if err := os.Remove(tmpFile); err != nil {
							log.Fatalf("worker %s | doReduceTask | cannot remove file %v", workerId, tmpFile)
						}
					}
					continue
				}
				if task.TaskType == TypeReduce && reset == false {
					outputFilename := fmt.Sprintf("mr-out-%d", task.TaskId)
					if err := os.Rename(tmpFile, outputFilename); err != nil {
						os.Remove(tmpFile)
						log.Fatalf("worker %s | doReduceTask | cannot rename file %v", workerId, tmpFile)
					}
				}
			}
		}
	}()
	var currentTask TaskContext
	for {
		select {
		case task := <-taskCh:
			currentTask = task

		case <-time.After(1 * time.Second):
			if currentTask.TaskType != TypeWait {
				heartbeatReq := HeartbeatRequest{
					WorkerId: workerId,
					TaskId:   currentTask.TaskId,
					TaskType: currentTask.TaskType,
				}
				heartbeatReply := HeartbeatReply{}
				TrySend(func() bool {
					return call("Coordinator.Heartbeat", &heartbeatReq, &heartbeatReply)
				}, 100)
				reset = heartbeatReply.Reset

				if reset {
					currentTask.TaskType = TypeWait
				} else if complete {
					break
				}
			}
		}
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
