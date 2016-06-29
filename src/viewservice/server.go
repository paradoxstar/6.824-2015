package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	curView       View
	ackNum        uint
	backup_ackNum uint
	lastPingTime  map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	DPrintf("Pings from ("+args.Me+"viewnum: %d)\n", args.Viewnum)

	vs.lastPingTime[args.Me] = time.Now()

	if args.Me == vs.curView.Primary {
		DPrintf("Pings from Primary, update ackNum\n")
		DPrintf("ackNum is set from %d ", vs.ackNum)
		if vs.ackNum <= args.Viewnum {
			vs.ackNum = args.Viewnum
			DPrintf("to %d\n", vs.ackNum)
		} else {
			DPrintf("Primary restart immediately\n")
			tmp := vs.curView.Backup
			vs.curView.Backup = vs.curView.Primary
			vs.curView.Primary = tmp
			vs.curView.Viewnum++
		}
	}

	if args.Me == vs.curView.Backup {
		DPrintf("Pings from Backup, update back_ackNum\n")
		DPrintf("back_ackNum is set from %d ", vs.backup_ackNum)
		if vs.backup_ackNum <= args.Viewnum {
			vs.backup_ackNum = args.Viewnum
			DPrintf("to %d\n", vs.ackNum)
		} else {
			DPrintf("Backup restart immediately\n")
			vs.curView.Backup = ""
			vs.curView.Viewnum++
		}
	}

	if (vs.curView.Viewnum == vs.ackNum) && (vs.curView.Primary != args.Me) && (vs.curView.Backup != args.Me) {
		DPrintf("Pings from availiable server\n")
		if vs.curView.Primary == "" {
			vs.curView.Primary = args.Me
			vs.curView.Viewnum++
			DPrintf("Primary is null, set Primary, and curviewnum change to %d \n", vs.curView.Viewnum)
		} else if vs.curView.Backup == "" {
			vs.curView.Backup = args.Me
			vs.curView.Viewnum++
			DPrintf("Backup is null, set Backup, and curviewnum change to %d \n", vs.curView.Viewnum)
		}
	}

	reply.View = vs.curView
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.curView
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	if (vs.ackNum == vs.curView.Viewnum) && (vs.curView.Primary != "") && (time.Since(vs.lastPingTime[vs.curView.Primary]) > DeadPings*PingInterval) {
		DPrintf("Primary is overtime, considerred to be dead\n")
		vs.curView.Primary = vs.curView.Backup
		vs.curView.Backup = ""
		vs.curView.Viewnum++
		DPrintf("curviewnum change to %d \n", vs.curView.Viewnum)
	}
	if (vs.ackNum == vs.curView.Viewnum) && (vs.curView.Backup != "") && (time.Since(vs.lastPingTime[vs.curView.Backup]) > DeadPings*PingInterval) {
		DPrintf("Backup is overtime, considerred to be dead\n")
		vs.curView.Backup = ""
		vs.curView.Viewnum++
		DPrintf("curviewnum change to %d \n", vs.curView.Viewnum)
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me

	// Your vs.* initializations here.
	vs.curView.Backup = ""
	vs.curView.Primary = ""
	vs.curView.Viewnum = 0
	vs.backup_ackNum = 0
	vs.ackNum = 0
	vs.dead = 0
	vs.rpccount = 0
	vs.lastPingTime = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
