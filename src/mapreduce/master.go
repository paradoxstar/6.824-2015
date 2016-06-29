package mapreduce

import "container/list"
import "fmt"
import "time"
import "sync"
import "runtime"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	args DoJobArgs
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	runtime.GOMAXPROCS(2) //running in multicore

	//concurrently handle worker registration
	go func() {
		for {
			DPrintf("waiting for worker registration\n")
			//waiting for worker register
			resworker := <-mr.registerChannel
			DPrintf("Master get Register: %s\n", resworker)
			//save worker info
			var w WorkerInfo
			w.address = resworker
			mr.Workers[resworker] = &w
			//send worker address to availWorkers channel
			mr.availWorkers <- resworker
			DPrintf("Master handle Register:%s Done\n", resworker)
		}
	}()

	//number of map & reduce done jobs
	mapdone := 0
	reducedone := 0

	//stack to store failed job processing which need to be redone
	//stacklock is to use for synchronization of stack operations
	stacklock := new(sync.Mutex)
	var redojob [100]int

	top := -1  // top of stack
	jobid := 0 // sequential job id for job list
	curid := 0 // current job id that should be processed next

	//handle map jobs
	for {
		// map jobs are all done
		if mapdone == mr.nMap {
			break
		}
		//handle map jobs that failed before, which stored in stack first
		//then for sequential job list and wait for the last jobs done
		if top >= 0 {
			stacklock.Lock()
			curid = redojob[top]
			top--
			stacklock.Unlock()
		} else if jobid < mr.nMap {
			curid = jobid
			jobid++
		} else {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		//set jobs arguments
		var args DoJobArgs
		args.File = mr.file
		args.JobNumber = curid
		args.NumOtherPhase = mr.nReduce
		args.Operation = Map
		DPrintf("job ready:\tjobid:%d\tjobtype:%s\tjobNumOtherPhase:%d\n", args.JobNumber, args.Operation, args.NumOtherPhase)

		//waiting for available workers, stored in avworker
		avworker := <-mr.availWorkers
		DPrintf("get channel availWorkers: %s\n", avworker)
		//concurrently send the job to avworker
		go func(worker string, args DoJobArgs) {
			var reply DoJobReply
			o := make(chan bool)
			//concurrently call the worker to handle the job
			go func() {
				ok := call(worker, "Worker.DoJob", args, &reply)
				if (ok != false) && (reply.OK == true) {
					mr.availWorkers <- worker
				}
				//handle successfully
				o <- ok
			}()
			// waiting for result of job handling
			select {
			case re := <-o:
				//successfully
				if re {
					mapdone++
				} else {
					//failed
					stacklock.Lock()
					top++
					redojob[top] = args.JobNumber
					stacklock.Unlock()
				}
			//time out
			case <-time.After(100 * time.Millisecond):
				stacklock.Lock()
				top++
				redojob[top] = args.JobNumber
				stacklock.Unlock()
			}
		}(avworker, args)
	}

	top = -1
	jobid = 0

	//handle reduce jobs
	for {
		// reduce jobs are all done
		if reducedone == mr.nReduce {
			break
		}
		//handle reduce jobs that failed before, which stored in stack first
		//then for sequential job list and wait for the last jobs done
		if top >= 0 {
			stacklock.Lock()
			curid = redojob[top]
			top--
			stacklock.Unlock()
		} else if jobid < mr.nReduce {
			curid = jobid
			jobid++
		} else {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		//set jobs arguments
		var args DoJobArgs
		args.File = mr.file
		args.JobNumber = curid
		args.NumOtherPhase = mr.nMap
		args.Operation = Reduce
		DPrintf("job ready:\tjobid:%d\tjobtype:%s\tjobNumOtherPhase:%d\n", args.JobNumber, args.Operation, args.NumOtherPhase)

		//waiting for available workers, stored in avworker
		avworker := <-mr.availWorkers
		DPrintf("get channel availWorkers: %s\n", avworker)
		//concurrently send the job to avworker
		go func(worker string, args DoJobArgs) {
			var reply DoJobReply
			o := make(chan bool)
			//concurrently call the worker to handle the job
			go func() {
				ok := call(worker, "Worker.DoJob", args, &reply)
				if (ok != false) && (reply.OK == true) {
					mr.availWorkers <- worker
				}
				o <- ok
				//handle successfully
			}()
			// waiting for result of job handling
			select {
			case re := <-o:
				//successfully
				if re {
					reducedone++
				} else {
					//failed
					stacklock.Lock()
					top++
					redojob[top] = args.JobNumber
					stacklock.Unlock()
				}
			//time out
			case <-time.After(100 * time.Millisecond):
				stacklock.Lock()
				top++
				redojob[top] = args.JobNumber
				stacklock.Unlock()
			}
		}(avworker, args)
	}

	//all jobs done
	return mr.KillWorkers()
}

func (mr *MapReduce) RunMaster_scheme2() *list.List {
	// Your code here

	runtime.GOMAXPROCS(2) //running in multicore
	//handle register
	//concurrently handle worker registration
	go func() {
		for {
			DPrintf("waiting for worker registration\n")
			//waiting for worker register
			resworker := <-mr.registerChannel
			DPrintf("Master get Register: %s\n", resworker)
			//save worker info
			var w WorkerInfo
			w.address = resworker
			mr.Workers[resworker] = &w
			//send worker address to availWorkers channel
			mr.availWorkers <- resworker
			DPrintf("Master handle Register:%s Done\n", resworker)
		}
	}()

	//channel to receive the job done signal
	done := make(chan int)

	//for every map job, allocate a gofun to handle the map job
	for i := 0; i < mr.nMap; i++ {
		//args for the i-th map job
		var args DoJobArgs
		args.File = mr.file
		args.JobNumber = i
		args.NumOtherPhase = mr.nReduce
		args.Operation = Map
		DPrintf("job ready:\tjobid:%d\tjobtype:%s\tjobNumOtherPhase:%d\n", args.JobNumber, args.Operation, args.NumOtherPhase)

		//concurrently handle the i-th map job
		go func(jobid int, jobargs DoJobArgs) {
			//handle circularly thr i-th map job till it is finished
			//if do == 1, need to do another cycle to handle i-th job;
			//if do == 0, i-th job done, loop ends
			do := 1
			for {
				//loop ends
				if do == 0 {
					break
				}
				//waiting for available worker, stored in worker
				worker := <-mr.availWorkers
				DPrintf("get channel availWorkers for job %d: %s\n", jobid, worker)
				var reply DoJobReply
				o := make(chan bool)
				//concurrently call the worker to handle the job
				go func() {
					ok := call(worker, "Worker.DoJob", jobargs, &reply)
					if (ok != false) && (reply.OK == true) {
						mr.availWorkers <- worker
					}
					//handle successfully
					o <- ok
				}()
				// waiting for result of job handling
				select {
				case re := <-o:
					if re {
						//successfully
						done <- 1
						do = 0
					} else {
						//failed
						do = 1
					}
				//time out
				case <-time.After(100 * time.Millisecond):
					do = 1
				}
			}
		}(i, args)
	}
	//map done
	for i := 0; i < mr.nMap; i++ {
		<-done
	}
	DPrintf("====================Map jobs done=================\n")
	//for every map job, allocate a gofun to handle the map job
	for i := 0; i < mr.nReduce; i++ {
		//args for the i-th map job
		var args DoJobArgs
		args.File = mr.file
		args.JobNumber = i
		args.NumOtherPhase = mr.nMap
		args.Operation = Reduce
		DPrintf("job ready:\tjobid:%d\tjobtype:%s\tjobNumOtherPhase:%d\n", args.JobNumber, args.Operation, args.NumOtherPhase)

		//concurrently handle the i-th map job
		go func(jobid int, jobargs DoJobArgs) {
			//handle circularly thr i-th map job till it is finished
			//if do == 1, need to do another cycle to handle i-th job;
			//if do == 0, i-th job done, loop ends
			do := 1
			for {
				//loop ends
				if do == 0 {
					break
				}
				//waiting for available worker, stored in worker
				worker := <-mr.availWorkers
				DPrintf("gaaet channel availWorkers for job %d: %s\n", jobid, worker)
				var reply DoJobReply
				o := make(chan bool)
				//concurrently call the worker to handle the job
				go func() {
					ok := call(worker, "Worker.DoJob", jobargs, &reply)
					if (ok != false) && (reply.OK == true) {
						mr.availWorkers <- worker
					}
					//handle successfully
					o <- ok
				}()
				// waiting for result of job handling
				select {
				case re := <-o:
					if re {
						//successfully
						done <- 1
						do = 0
					} else {
						//failed
						do = 1
					}
				//time out
				case <-time.After(100 * time.Millisecond):
					do = 1
				}
			}
		}(i, args)
	}

	//reduce done
	for i := 0; i < mr.nReduce; i++ {
		<-done
	}

	//all jobs done
	return mr.KillWorkers()
}
