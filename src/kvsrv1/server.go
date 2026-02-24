package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type DataEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]DataEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{data: make(map[string]DataEntry)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		DPrintf("Get|ErrNoKey|key=%s", args.Key)
		return
	}

	reply.Value = entry.Value
	reply.Version = entry.Version
	reply.Err = rpc.OK
	DPrintf("Get|OK|key=%s|value=%s|version=%d", args.Key, entry.Value, entry.Version)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// Args.Version is 0.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.data[args.Key]
	if !ok && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		DPrintf("Put|ErrNoKey|key=%s|version=%d", args.Key, args.Version)
		return
	}

	if ok && entry.Version != args.Version {
		reply.Err = rpc.ErrVersion
		DPrintf("Put|ErrVersion|key=%s|version=%d", args.Key, args.Version)
		return
	}

	entry.Value = args.Value
	entry.Version++
	kv.data[args.Key] = entry
	reply.Err = rpc.OK
	DPrintf("Put|OK|key=%s|value=%s|version=%d", args.Key, args.Value, args.Version)
}

// You can ignore for this lab
func (kv *KVServer) Kill() {
}

// You can ignore for this lab
func (kv *KVServer) Raft() *raft.Raft {
	return nil
}

// You can ignore all arguments; they are for replicated KVservers in lab 4
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *raft.Persister, maxraftstate int) tester.IKVServer {
	kv := MakeKVServer()
	return kv
}
