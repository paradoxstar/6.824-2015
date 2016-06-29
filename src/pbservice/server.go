package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view     viewservice.View
	keyvalue map[string]string
	mark     map[int64]int
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//exam primary
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	//primary must get after backup get
	var backupreply GetReply
	for pb.view.Backup != "" {
		ok := call(pb.view.Backup, "PBServer.BackupGet", args, &backupreply)
		if ok {
			break
		} else {
			//viewchange && repeat
			time.Sleep(viewservice.PingInterval)
			DPrintf("View change!\n")
			pb.viewChange()
			//primary overtime so known as dead by viewserver
			if pb.me != pb.view.Primary {
				reply.Err = ErrWrongServer
				return nil
			}
		}
	}

	//backup reply overtime so known as dead by viweserver
	if backupreply.Err == ErrWrongServer {
		reply.Err = ErrWrongServer
		return nil
	}

	//primary handle get
	_, ok := pb.keyvalue[args.Key]
	if ok {
		reply.Value = pb.keyvalue[args.Key]
	} else {
		reply.Value = ""
	}
	reply.Err = OK

	return nil
}

//handle backup get
func (pb *PBServer) BackupGet(args *GetArgs, reply *GetReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	//exam backup
	//only ensure backup is known by viewservice or not
	//not reply the value, which is the work of primary
	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
	} else {
		reply.Err = OK
	}

	return nil
}
  
// +++++++++++++++++
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//exam primary
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	//primary must get after backup get
	var backupreply PutAppendReply
	for pb.view.Backup != "" {
		ok := call(pb.view.Backup, "PBServer.BackupPutAppend", args, &backupreply)
		if ok {
			break
		} else {
			//viewchange && repeat
			time.Sleep(viewservice.PingInterval)
			pb.viewChange()
			DPrintf("View change\n")
			//primary overtime so known as dead by viewserver
			if pb.me != pb.view.Primary {
				reply.Err = ErrWrongServer
				return nil
			}
		}
	}

	//backup reply overtime so known as dead by viweserver
	if backupreply.Err == ErrWrongServer {
		reply.Err = ErrWrongServer
		return nil
	}

	//primary handle put&append
	_, ok := pb.mark[args.Index]
	if ok {
		reply.Err = OK
		return nil
	}
	if args.Operation == "Put" {
		pb.keyvalue[args.Key] = args.Value
	} else if args.Operation == "Append" {
		_, ok := pb.keyvalue[args.Key]
		if ok {
			pb.keyvalue[args.Key] += args.Value
		} else {
			pb.keyvalue[args.Key] = args.Value
		}
	} else {
		reply.Err = "NoOperation"
		return nil
	}

	pb.mark[args.Index] = 1
	reply.Err = OK

	return nil
}

//handle backup put & append
func (pb *PBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	//exam backup
	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	//backup handle put&&append
	_, ok := pb.mark[args.Index]
	if ok {
		reply.Err = OK
		return nil
	}
	if args.Operation == "Put" {
		pb.keyvalue[args.Key] = args.Value
	} else if args.Operation == "Append" {
		_, ok := pb.keyvalue[args.Key]
		if ok {
			pb.keyvalue[args.Key] += args.Value
		} else {
			pb.keyvalue[args.Key] = args.Value
		}
	} else {
		reply.Err = "NoOperation"
		return nil
	}

	pb.mark[args.Index] = 1
	reply.Err = OK

	return nil
}

//handle backup copy
func (pb *PBServer) BackupCopy(args *BackupCopyArgs, reply *BackupCopyReply) error {

	pb.mu.Lock()

	pb.keyvalue = args.KeyValue
	pb.mark = args.Mark
	reply.Err = OK

	pb.mu.Unlock()
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	pb.viewChange()
	pb.mu.Unlock()
}

func (pb *PBServer) viewChange() {

	// Your code here.
	view, error := pb.vs.Ping(pb.view.Viewnum)
	DPrintf("->this is "+pb.me+":viewnum=%d, "+"p="+pb.view.Primary+", b="+pb.view.Backup+"\n", pb.view.Viewnum)
	DPrintf("<-while curView: viewnum=%d, "+"p="+view.Primary+", b="+view.Backup+"\n", view.Viewnum)
	if error == nil {
		if view.Viewnum != pb.view.Viewnum {
			//only handle backup_change with the data copy from primary
			//to backup, since backup can be promoted to primary directly
			//this for loop will end onlyif copy succeed
			for pb.me == pb.view.Primary && view.Backup != pb.view.Backup && view.Backup != "" {
				var reply BackupCopyReply
				args := &BackupCopyArgs{pb.keyvalue, pb.mark}
				ok := call(view.Backup, "PBServer.BackupCopy", args, &reply)
				if ok {
					break
				} else {
					time.Sleep(viewservice.PingInterval)
				}
			}
			//view change!
			pb.view = view
			DPrintf("View changed for " + pb.me + " :primary=" + pb.view.Primary + " backup=" + pb.view.Backup + "\n")
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	pb.keyvalue = make(map[string]string)
	pb.mark = make(map[int64]int)

	pb.view.Viewnum = 0
	pb.view.Primary = ""
	pb.view.Backup = ""
	pb.dead = 0
	pb.unreliable = 0

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
