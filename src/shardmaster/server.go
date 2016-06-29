package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	minInst   int
	curConfig int
}

//func nrand() int64 {
//	max := big.NewInt(int64(1) << 62)
//	bigx, _ := rand.Int(rand.Reader, max)
//	x := bigx.Int64()
//	return x
//}

type Op struct {
	// Your data here.
	Type   string
	GID    int64
	Server []string
	Shard  int
	Num    int
	//	Index  int64
}

func (sm *ShardMaster) WaitAgreement(seq int, value Op) Op {
	sm.px.Start(seq, value)
	to := 10 * time.Millisecond
	for {
		status, val := sm.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) GetNewConfig() Config {
	var newConfig Config
	newConfig.Num = sm.configs[sm.curConfig].Num + 1
	newConfig.Shards = [NShards]int64{}
	newConfig.Groups = map[int64][]string{}

	for gid, servers := range sm.configs[sm.curConfig].Groups {
		newConfig.Groups[gid] = servers
	}
	for shardind, gid := range sm.configs[sm.curConfig].Shards {
		newConfig.Shards[shardind] = gid
	}

	return newConfig
}

func (sm *ShardMaster) ReBanlance(newConfig *Config, gid int64, optype string) {

	counter := map[int64]int{}

	groupNum := len(newConfig.Groups)
	aver := NShards / groupNum
	alter := NShards % groupNum

	if optype == "Join" {

		for i := 0; i < NShards; i++ {
			groupid := newConfig.Shards[i]

			if counter[groupid] < aver {
				counter[groupid]++
			} else if counter[groupid] == aver && alter > 0 {
				alter--
				counter[groupid]++
			} else {
				newConfig.Shards[i] = 0
			}
		}

	} else if optype == "Leave" {

		for i := 0; i < NShards; i++ {
			groupid := newConfig.Shards[i]

			if groupid == gid {
				newConfig.Shards[i] = 0

			} else {
				if counter[groupid] < aver {
					counter[groupid]++
				} else if counter[groupid] == aver && alter > 0 {
					alter--
					counter[groupid]++
				} else {
					newConfig.Shards[i] = 0
				}
			}
		}
	}

	for i := 0; i < NShards; i++ {
		groupid := newConfig.Shards[i]

		if groupid != 0 {
			continue
		}

		for id, _ := range newConfig.Groups {
			if counter[id] < aver {
				newConfig.Shards[i] = id
				counter[id]++
				break
			} else if counter[id] == aver && alter > 0 {
				newConfig.Shards[i] = id
				counter[id]++
				alter--
				break
			}
		}
	}

}

func (sm *ShardMaster) JoinHandle(op Op) {
	newConfig := sm.GetNewConfig()
	_, exist := newConfig.Groups[op.GID]
	if !exist {
		newConfig.Groups[op.GID] = op.Server
		sm.ReBanlance(&newConfig, op.GID, "Join")
		sm.configs = append(sm.configs, newConfig)
		sm.curConfig++
	}
}

func (sm *ShardMaster) LeaveHandle(op Op) {
	newConfig := sm.GetNewConfig()
	_, exist := newConfig.Groups[op.GID]
	if exist {
		delete(newConfig.Groups, op.GID)
		sm.ReBanlance(&newConfig, op.GID, "Leave")
		sm.configs = append(sm.configs, newConfig)
		sm.curConfig++
	}
}

func (sm *ShardMaster) MoveHandle(op Op) {
	newConfig := sm.GetNewConfig()
	newConfig.Shards[op.Shard] = op.GID
	sm.configs = append(sm.configs, newConfig)
	sm.curConfig++
}

func (sm *ShardMaster) QueryHandle(op Op) Config {
	if op.Num == -1 || op.Num > sm.curConfig {
		return sm.configs[sm.curConfig]
	} else {
		return sm.configs[op.Num]
	}
}

func (sm *ShardMaster) Exec(op Op) Config {

	switch op.Type {
	case "Join":
		sm.JoinHandle(op)
	case "Leave":
		sm.LeaveHandle(op)
	case "Move":
		sm.MoveHandle(op)
	case "Query":
		return sm.QueryHandle(op)
	}
	return Config{}
}

func (sm *ShardMaster) Apply(op Op) Config {
	//	op.Index = nrand()

	for true {

		seq := sm.minInst + 1

		//If has been decided
		var val Op
		isdecided, value := sm.px.Status(seq)
		if isdecided == paxos.Decided {
			val = value.(Op)
		} else {
			val = sm.WaitAgreement(seq, op)
		}

		config := sm.Exec(val)
		sm.px.Done(seq)
		sm.minInst++
		if val.GID == op.GID && val.Num == op.Num && val.Shard == val.Shard && val.Type == op.Type {
			return config
		}
	}
	return Config{}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	//fmt.Printf("Peer %d begin handle join: %v\n", sm.me, args)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.Apply(Op{Type: "Join", GID: args.GID, Server: args.Servers})

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	//fmt.Printf("Peer %d begin handle leave: %v\n", sm.me, args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.Apply(Op{Type: "Leave", GID: args.GID})
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	//fmt.Printf("Peer %d begin handle move: %v\n", sm.me, args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.Apply(Op{Type: "Move", GID: args.GID, Shard: args.Shard})
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	//fmt.Printf("Peer %d begin handle query: %v\n", sm.me, args)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	reply.Config = sm.Apply(Op{Type: "Query", Num: args.Num})
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.curConfig = 0
	sm.minInst = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
