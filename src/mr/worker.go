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

// for sorting by key.
type ByKey []KeyValue

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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		resp := doHeartBeat()
		switch resp.Task {
		case TASK_MAP:
			mapWorker(resp, mapf)
		case TASK_REDUCE:
			reduceWorker(resp, reducef)
		case TASK_WAIT:
			time.Sleep(time.Second * 1)
		case TASK_COMPLETE:
			return
		default:
			panic(fmt.Sprintf("invalid Task %v", resp.Task))
		}
	}
}

func mapWorker(resp *HeartBeatResp, mapf func(string, string) []KeyValue) {
	log.Printf("mapWorker|map worker start|id=%v", resp.Id)
	defer log.Printf("mapWorker|map worker end|id=%v", resp.Id)

	file, err := os.Open(resp.Filename)
	if err != nil {
		log.Fatalf("mapWorker|open file fail|id=%v|filename=%v", resp.Id, resp.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("mapWorker|read file fail|id=%v|filename=%v", resp.Id, resp.Filename)
	}
	file.Close()

	result := mapf(resp.Filename, string(content))

	var encoders []*json.Encoder
	for i := 0; i < resp.NReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", resp.Id, i+1)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("mapWorker|create file fail|id=%v|filename=%v", resp.Id, filename)
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		encoders = append(encoders, enc)
	}

	for _, kv := range result {
		k := ihash(kv.Key) % resp.NReduce
		err := encoders[k].Encode(kv)
		if err != nil {
			log.Fatalf("mapWorker|write file fail|id=%v|kv=%v", resp.Id, kv)
		}
	}
	doReport(resp)
}

func reduceWorker(resp *HeartBeatResp, reducef func(string, []string) string) {
	log.Printf("reduceWorker|reduce worker start|id=%v", resp.Id)
	defer log.Printf("reduceWorker|reduce worker end|id=%v", resp.Id)

	filename := fmt.Sprintf("mr-out-%v", resp.Id)
	ofile, err := os.Create(filename)
	if err != nil {
		log.Fatalf("reduceWorker|create file fail|id=%v|filename=%v", resp.Id, filename)
	}
	defer ofile.Close()

	var kva []KeyValue
	for i := 0; i < resp.NMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i+1, resp.Id)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("reduceWorker|open file fail|id=%v|filename=%v", resp.Id, filename)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	doReport(resp)
}

func doHeartBeat() *HeartBeatResp {
	req := HeartBeatReq{}
	resp := HeartBeatResp{}
	ok := call("Coordinator.HeartBeat", &req, &resp)
	if ok {
		log.Printf("callHeartBeat|call succ|req=%v|resp=%v", req, resp)
	} else {
		log.Printf("callHeartBeat|call fail|req=%v|resp=%v", req, resp)
	}
	return &resp
}

func doReport(heartbeatResp *HeartBeatResp) *ReportResp {
	req := ReportReq{
		Task: heartbeatResp.Task,
		Id:   heartbeatResp.Id,
	}
	resp := ReportResp{}
	ok := call("Coordinator.Report", &req, &resp)
	if ok {
		log.Printf("callReport|call succ|req=%v|resp=%v", req, resp)
	} else {
		log.Printf("callReport|call fail|req=%v|resp=%v", req, resp)
	}
	return &resp
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
