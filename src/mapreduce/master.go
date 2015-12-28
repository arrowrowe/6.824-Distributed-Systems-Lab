package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
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

func (mr *MapReduce) Handle(selfSize int, otherSize int, operation JobType) {
	doneChannel := make(chan int, selfSize)
	// Begin all workers parallelly.
	for i := 0; i < selfSize; i++ {
		// See [Go statements](https://golang.org/ref/spec#Go_statements).
		go func (jobNumber int) {
			for {
				// See [Channels](https://gobyexample.com/channels).
				worker := <- mr.registerChannel
				if call(worker, "Worker.DoJob", &DoJobArgs{mr.file, operation, jobNumber, otherSize}, &DoJobReply{}) {
					mr.registerChannel <- worker
					doneChannel <- jobNumber
					return
				}
			}
		}(i)
	}
	// Wait for all workers.
	for i := 0; i < selfSize; i++ {
		fmt.Println(operation, "Job", <- doneChannel, "done")
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Handle(mr.nMap, mr.nReduce, Map)
	mr.Handle(mr.nReduce, mr.nMap, Reduce)
	return mr.KillWorkers()
}
