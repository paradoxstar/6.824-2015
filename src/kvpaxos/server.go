package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Index int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	keyvalue map[string]string
	mark     map[int64]int
	minIns   int
}

func (kv *KVPaxos) WaitAgreement(seq int, value Op) Op {
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

func (kv *KVPaxos) Exec(op Op) {

	if op.Type == "Put" {
		kv.keyvalue[op.Key] = op.Value
	} else if op.Type == "Append" {
		_, ok := kv.keyvalue[op.Key]
		if ok {
			kv.keyvalue[op.Key] += op.Value
		} else {
			kv.keyvalue[op.Key] = op.Value
		}
	}
	kv.mark[op.Index] = 1
}

func (kv *KVPaxos) Apply(op Op) (Err, string) {

	DPrintf("Peer-%v, Apply operation%v\n", kv.me, op)
	for true {
		//if repeated operation
		_, ok := kv.mark[op.Index]
		if ok {
			v, _ := kv.keyvalue[op.Key]
			return OK, v
		}

		seq := kv.minIns + 1

		//If has been decided
		var val Op
		isdecided, value := kv.px.Status(seq)
		if isdecided == paxos.Decided {
			val = value.(Op)
			kv.Exec(val)
			kv.px.Done(seq)
			kv.minIns++
			continue
		}

		//been not decided
		val = kv.WaitAgreement(seq, op)
		kv.Exec(val)
		kv.px.Done(seq)
		kv.minIns++

		if val == op {
			val, ok := kv.keyvalue[op.Key]
			if ok {
				return OK, val
			} else {
				return ErrNoKey, ""
			}
		}
	}

	return OK, ""
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	index := args.Index

	err, val := kv.Apply(Op{Key: key, Index: index})

	reply.Err = err
	reply.Value = val

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value := args.Value
	op := args.Op
	index := args.Index

	err, _ := kv.Apply(Op{Type: op, Key: key, Value: value, Index: index})

	reply.Err = err
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	kv.minIns = -1
	kv.keyvalue = make(map[string]string)
	kv.mark = make(map[int64]int)
	// Your initialization code here.

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
