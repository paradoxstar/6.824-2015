package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"
import "time"
import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	me int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	index := nrand()
	servIndex := 0
	args := &GetArgs{key, index}
	var reply GetReply
	DPrintf("CK %d Parameter: "+key+"|"+"%d\n", ck.me, index)

	//send get_rpc until Operation is handled to one clerk successfully
	for {
		DPrintf("CK %d send Get to server%d\n", ck.me, servIndex)
		ok := call(ck.servers[servIndex], "KVPaxos.Get", args, &reply)
		DPrintf("ok = %d && reply.Err = %s for CK %d", ok, reply.Err, ck.me)
		if ok && reply.Err == OK {
			//done successfully
			DPrintf(reply.Value + "\n")
			return reply.Value
		}
		servIndex = (servIndex + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 100)
	}

	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	index := nrand()
	args := &PutAppendArgs{key, value, op, index}
	servIndex := 0
	DPrintf("CK %d Parameter: "+key+"|"+value+"|"+op+"|"+"%d\n", ck.me, index)
	var reply PutAppendReply

	//send put$append_rpc until can ensure the Operation is done successfully

	for {
		DPrintf("CK %d send PutAppend to server%d\n", ck.me, servIndex)
		ok := call(ck.servers[servIndex], "KVPaxos.PutAppend", args, &reply)
		DPrintf("ok = %d && reply.Err = %s for CK%d", ok, reply.Err, ck.me)
		if ok && reply.Err == OK {
			//done successfully
			return
		}
		servIndex = (servIndex + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 100)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
