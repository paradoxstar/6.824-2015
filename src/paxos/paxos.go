package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

const (
	OK     = "OK"
	Reject = "Reject"
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	offset     int
	acceptor   map[int]*AcceptState
	prepareNum map[int]int
	dones      []int
}

type AcceptState struct {
	maxPrepareNum int
	acceptNum     int
	acceptValue   interface{}
	state         Fate
	decided       NumValuePair
}

type NumValuePair struct {
	Number int
	Value  interface{}
}

type PrepareArgs struct {
	InstanceId int
	PrepareNum int
}

type PrepareReply struct {
	State       string
	AcceptNum   int
	AcceptValue interface{}
	isDecided   bool
	maxPreNum   int
}

type AcceptArgs struct {
	InstanceId  int
	AcceptNum   int
	AcceptValue interface{}
}

type AcceptReply struct {
	State     string
	AcceptNum int
}

type DecisionArgs struct {
	Sender      int
	Done        int
	InstanceId  int
	Decidedinfo NumValuePair
}

type DecisionReply struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) PrepareHandle(args *PrepareArgs, reply *PrepareReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()

	instId := args.InstanceId
	prepNum := args.PrepareNum
	reply.isDecided = false
	reply.State = Reject

	//	log.Printf("Peer%d receives the prepare call, for instance%d, prepareNum%d\n", px.me, instId, prepNum)
	_, exist := px.acceptor[instId]
	if !exist {
		//		log.Printf("initial\n")
		px.acceptor[instId] = &AcceptState{maxPrepareNum: prepNum, acceptNum: -1, acceptValue: nil, state: Pending}
		reply.State = OK
		reply.AcceptNum = -1
		reply.AcceptValue = nil
		reply.maxPreNum = prepNum
	} else {
		//		log.Printf("update\n")
		if prepNum > px.acceptor[instId].maxPrepareNum {
			px.acceptor[instId].maxPrepareNum = prepNum
			reply.State = OK
			reply.AcceptNum = px.acceptor[instId].acceptNum
			reply.AcceptValue = px.acceptor[instId].acceptValue
			reply.maxPreNum = px.acceptor[instId].maxPrepareNum
		}
	}

	if px.acceptor[instId].state == Decided {
		reply.isDecided = true
	}
	return nil
}

func (px *Paxos) BroadcastPrepare(seq int, v interface{}) (bool, bool, []int, NumValuePair) {

	//Find proper prepareNum
	px.mu.Lock()
	_, exist := px.prepareNum[seq]
	if !exist {
		px.prepareNum[seq] = px.me
	} else {
		px.prepareNum[seq] += px.offset
	}

	prepNum := px.prepareNum[seq]
	px.mu.Unlock()

	//	log.Printf("Peer%d send prepare%d for instance%d\n", px.me, px.prepareNum[seq], seq)

	//send prepare and receive
	isDecided := false
	isMajority := false
	highestAccept := NumValuePair{Number: -1, Value: v}
	OKAnsPeer := make([]int, 0)
	maxPre := -1

	args := PrepareArgs{InstanceId: seq, PrepareNum: prepNum}

	for i := 0; i < len(px.peers); i++ {
		//log.Printf("Peer%d send prepare%d for instance%d to Peer%d\n", px.me, prepNum, seq, i)
		var reply PrepareReply
		if i != px.me {
			call(px.peers[i], "Paxos.PrepareHandle", &args, &reply)
		} else {
			px.PrepareHandle(&args, &reply)
		}
		if reply.State == OK {
			OKAnsPeer = append(OKAnsPeer, i)
			if reply.AcceptNum > highestAccept.Number {
				highestAccept.Number = reply.AcceptNum
				highestAccept.Value = reply.AcceptValue
			}
			if reply.maxPreNum > maxPre {
				maxPre = reply.maxPreNum
			}
		}
		if reply.isDecided {
			isDecided = true
		}
	}

	highestAccept.Number = prepNum
	//log.Printf("OKAnsPrepare:%d\n", len(OKAnsPeer))
	if len(OKAnsPeer) >= (len(px.peers)/2 + 1) {
		isMajority = true
	}

	//log.Printf("PrepareBroadcast returns:%v, %v, %v, %v", isDecided, isMajority, OKAnsPeer, highestAccept)
	return isDecided, isMajority, OKAnsPeer, highestAccept
}

func (px *Paxos) BroadcastAccept(seq int, AnswerPrepare []int, acceptInfo NumValuePair) bool {

	//log.Printf("Peer%d send accept%d for instance%d \n", px.me, acceptInfo.Number, seq)

	//send accept and receive
	OKAnsPeer := make([]int, 0)
	isMajority := false

	args := AcceptArgs{InstanceId: seq, AcceptNum: acceptInfo.Number, AcceptValue: acceptInfo.Value}
	for i := range AnswerPrepare {
		var reply AcceptReply
		if AnswerPrepare[i] != px.me {
			call(px.peers[AnswerPrepare[i]], "Paxos.AcceptHandle", &args, &reply)
		} else {
			px.AcceptHandle(&args, &reply)
		}
		if reply.State == OK {
			OKAnsPeer = append(OKAnsPeer, i)
		}
	}

	//log.Printf("OKAnsAccept:%d\n", len(OKAnsAccept))

	// majority
	if len(OKAnsPeer) >= (len(px.peers)/2 + 1) {
		isMajority = true
	}

	return isMajority
}

func (px *Paxos) AcceptHandle(args *AcceptArgs, reply *AcceptReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()

	instId := args.InstanceId
	acceptnumber := args.AcceptNum
	acceptvalue := args.AcceptValue

	//log.Printf("Peer%d receives the accept call, for instance%d, prepareNum%d\n", px.me, instId, acceptnumber)

	_, exist := px.acceptor[instId]

	if !exist {
		reply.State = Reject
	} else {
		if acceptnumber >= px.acceptor[instId].maxPrepareNum {
			px.acceptor[instId].acceptNum = acceptnumber
			px.acceptor[instId].acceptValue = acceptvalue
			px.acceptor[instId].maxPrepareNum = acceptnumber
			reply.State = OK
			reply.AcceptNum = acceptnumber
		} else {
			reply.State = Reject
		}
	}

	return nil
}

func (px *Paxos) BroadcastDecide(seq int, decidedInfo NumValuePair) {

	//log.Printf("Peer%d sends the decided call, for instance%d\n", px.me, seq)

	//send decided
	px.mu.Lock()
	done := px.dones[px.me]
	px.mu.Unlock()
	args := DecisionArgs{InstanceId: seq, Sender: px.me, Done: done}
	args.Decidedinfo.Number = decidedInfo.Number
	args.Decidedinfo.Value = decidedInfo.Value
	for i := 0; i < len(px.peers); i++ {
		var reply DecisionReply
		if i != px.me {
			call(px.peers[i], "Paxos.DecideHandle", &args, &reply)
		} else {
			px.DecideHandle(&args, &reply)
		}
	}

}

func (px *Paxos) DecideHandle(args *DecisionArgs, reply *DecisionReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()

	//log.Printf("Peer%d receives the decided call, for instance%d\n", px.me, args.InstanceId)

	_, exist := px.acceptor[args.InstanceId]

	if !exist {
		px.acceptor[args.InstanceId] = &AcceptState{maxPrepareNum: -1, acceptNum: -1, acceptValue: ""}
	}
	px.acceptor[args.InstanceId].maxPrepareNum = args.Decidedinfo.Number
	px.acceptor[args.InstanceId].acceptValue = args.Decidedinfo.Value
	px.acceptor[args.InstanceId].acceptNum = args.Decidedinfo.Number
	px.acceptor[args.InstanceId].decided = args.Decidedinfo
	px.acceptor[args.InstanceId].state = Decided
	px.dones[args.Sender] = args.Done

	return nil

}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	go func() {
		//forgotten
		if seq < px.Min() {
			//log.Printf("Peer%d submit instance%d, forgotten, return directly\n", px.me, seq)
			return
		}

		//log.Printf("Peer%d submit instance%d\n", px.me, seq)
		for {
			isDecided, isPreMajority, AnswerPrepare, acceptInfo := px.BroadcastPrepare(seq, v)

			if isDecided {
				break
			}
			if !isPreMajority {
				continue
			}

			isAccMajority := px.BroadcastAccept(seq, AnswerPrepare, acceptInfo)

			if !isAccMajority {
				continue
			}

			px.BroadcastDecide(seq, acceptInfo)

			break
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if px.dones[px.me] < seq {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.

	return len(px.acceptor) - 1
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.

	px.mu.Lock()
	defer px.mu.Unlock()

	min := px.dones[px.me]
	for k := range px.dones {
		if min > px.dones[k] {
			min = px.dones[k]
		}
	}

	for k, _ := range px.acceptor {
		if k <= min && px.acceptor[k].state == Decided {
			delete(px.acceptor, k)
			delete(px.prepareNum, k)
		}
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here

	//log.Printf("judge status of %d\n", seq)
	if seq < px.Min() {
		//	log.Printf("forgotten\n")
		return Forgotten, nil
	}
	//log.Printf("no forgotten\n")
	px.mu.Lock()
	defer px.mu.Unlock()

	_, exist := px.acceptor[seq]

	if exist {
		return px.acceptor[seq].state, px.acceptor[seq].decided.Value
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.dead = 0
	px.unreliable = 0
	px.offset = len(peers)
	px.acceptor = make(map[int]*AcceptState)
	px.prepareNum = make(map[int]int)
	px.dones = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		px.dones[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
