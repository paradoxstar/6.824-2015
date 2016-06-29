package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Type  string
	Key   string
	Value string
	Index int64

	Config   shardmaster.Config
	Reconfig GetShardReply
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.

	keyvalue map[string]string
	mark     map[int64]int

	minInst int
	config  shardmaster.Config
	log     map[int]map[int]map[string]string
	mukv    sync.Mutex
}

func (kv *ShardKV) WaitAgreement(seq int, value Op) Op {
	kv.px.Start(seq, value)
	to := 10 * time.Millisecond
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < time.Second {
			to += 10 * time.Millisecond
		}
	}
}

func (kv *ShardKV) Exec(op Op) {

	switch op.Type {

	case "Get":

	case "Put":
		kv.keyvalue[op.Key] = op.Value

	case "Append":
		_, ok := kv.keyvalue[op.Key]
		if ok {
			kv.keyvalue[op.Key] += op.Value
		} else {
			kv.keyvalue[op.Key] = op.Value
		}

	case "ReConfig":
		for key := range op.Reconfig.Keyvalue {
			kv.keyvalue[key] = op.Reconfig.Keyvalue[key]
		}
		kv.config = op.Config

	case "GetShard":

	}
	kv.mark[op.Index] = 1

}

func (kv *ShardKV) Apply(op Op) (Err, string) {
	DPrintf("Group %d, Peer-%v, Apply operation%v\n", kv.gid, kv.me, op)

	for true {
		_, ok := kv.mark[op.Index]
		if ok {
			if op.Type == "Get" {
				v, _ := kv.keyvalue[op.Key]
				return OK, v
			}
			return OK, ""
		}

		seq := kv.minInst + 1
		//If has been decided
		var val Op
		isdecided, value := kv.px.Status(seq)
		if isdecided == paxos.Decided {
			val = value.(Op)
		} else {
			val = kv.WaitAgreement(seq, op)
		}

		DPrintf("Group %d, Peer-%v, Op %v is argreed!\n", kv.gid, kv.me, val)
		kv.Exec(val)
		kv.px.Done(seq)
		kv.minInst++
		if val.Index == op.Index {
			if op.Type == "Get" {
				v, _ := kv.keyvalue[op.Key]
				return OK, v
			}
			return OK, ""

		}
	}
	return "", ""
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("----Get from Group%v, Peer-%v-- args--%v---kvcfg--%d\n", kv.gid, kv.me, args, kv.config.Num)

	if args.CurConfig != kv.config.Num {
		reply.Err = ErrWrongGroup
		return nil
	}

	err, val := kv.Apply(Op{Type: "Get", Key: args.Key, Index: args.Index})
	DPrintf("Get Done!\n")
	reply.Err = err
	reply.Value = val

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("----put to Group%v, Peer-%v-- args-%v---kvcfg--%d\n", kv.gid, kv.me, args, kv.config.Num)

	if args.CurConfig != kv.config.Num {
		reply.Err = ErrWrongGroup
		return nil
	}

	err, _ := kv.Apply(Op{Type: args.Op, Key: args.Key, Value: args.Value, Index: args.Index})
	DPrintf("Put Done!\n")
	reply.Err = err
	return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {

	DPrintf("----GetShard from Group%v, Peer-%v-- args--%v---kvcfg--%d\n%v\n", kv.gid, kv.me, args, kv.config.Num, kv.keyvalue)
	if kv.config.Num < args.CurConfig {
		reply.Err = ""
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	kv.Apply(Op{Type: "GetShard", Index: nrand()})

	DPrintf("----GetShard Done!!!!!!!!shard is %d!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", shard)

	reply.Keyvalue = map[string]string{}

	for key := range kv.keyvalue {
		DPrintf("loop: key %v, k2s: %d, shard: %d", key, key2shard(key), shard)
		if key2shard(key) == shard {
			reply.Keyvalue[key] = kv.keyvalue[key]
		}
	}
	reply.Err = OK
	DPrintf("----GetShard from Group%v, Peer-%v---KV%v", kv.gid, kv.me, reply.Keyvalue)

	return nil

}

func (kv *ShardKV) ReConfigure(newConfig shardmaster.Config) bool {

	reConfig := GetShardReply{OK, map[string]string{}}

	prevConfig := kv.config

	for i := 0; i < shardmaster.NShards; i++ {
		oldgid := prevConfig.Shards[i]
		if newConfig.Shards[i] == kv.gid && oldgid != kv.gid && prevConfig.Shards[i] > 0 {
			DPrintf("Group %d, Peer-%d reConfig\n", kv.gid, kv.me)
			DPrintf("----oldgid---%v----ncf.Shard[%d]----%v------kv.gid--%v-----", oldgid, i, newConfig.Shards[i], kv.gid)

			success := false
			for !success {
				time.Sleep(time.Millisecond * 10)
				args := &GetShardArgs{i, prevConfig.Num}
				var reply GetShardReply
				DPrintf("-----Search Group[%d] server:%v", oldgid, prevConfig.Groups[oldgid])
				for _, server := range prevConfig.Groups[oldgid] {
					DPrintf("-----Search Group[%d] server:%v", oldgid, server)
					ok := call(server, "ShardKV.GetShard", args, &reply)
					if ok && reply.Err == OK {
						DPrintf("ShardGet:%v, From server:%v", reply, server)
						for key, value := range reply.Keyvalue {
							reConfig.Keyvalue[key] = value
						}
						success = true
						break
					} else {
						DPrintf("-----Search Group[%d] server:%v FAIED", oldgid, server)
						continue
					}
				}
			}
		}
	}
	op := Op{Type: "ReConfig", Config: newConfig, Reconfig: reConfig, Index: int64(newConfig.Num)}
	kv.Apply(op)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := kv.sm.Query(-1)
	//DPrintf("Group %d, Peer-%v, get latest config %v\n", kv.gid, kv.me, newConfig)
	//old := kv.config.Num
	mark := 0
	for i := kv.config.Num + 1; i <= newConfig.Num; i++ {
		config := kv.sm.Query(i)
		ok := kv.ReConfigure(config)
		//DPrintf("Group %d, Peer-%v, update config----%v\n", kv.gid, kv.me, i, config)
		if !ok {
			i--
		}
		mark = 1
	}
	if mark == 1 {
		//DPrintf("Group %d, Peer-%v, update config from %d to %d\n", kv.gid, kv.me, old, kv.config.Num)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = shardmaster.Config{Num: -1}
	kv.keyvalue = make(map[string]string)
	kv.mark = make(map[int64]int)
	kv.minInst = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
