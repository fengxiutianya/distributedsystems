package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map or Reduce).
// the mapFiles argument holds the names of the files that are the inputs to the map phase, one per map task.
// nReduce is the number of reduce tasks.
// the registerChan argument yields a stream of registered workers; each item is the worker's RPC address,
// suitable for passing to call().
// registerChan will yield all existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string,
	mapFiles []string, nReduce int, phase jobPhase,
	registerChan chan string) {
	//下面是来进行判断是reduce map的类型，并着手做一些准备
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		//启动一个线程
		go func(taskNum int, n_other int, phase jobPhase) {
			defer wg.Done()
			for {
				worker := <-registerChan

				var args DoTaskArgs
				args.JobName = jobName
				args.File = mapFiles[taskNum]
				args.Phase = phase
				args.TaskNumber = taskNum
				args.NumOtherPhase = n_other

				//Use the call() function in mapreduce/common_rpc.go to send an RPC to a worker.
				//The first argument is the the worker's address, as read from registerChan.
				//The second argument should be "Worker.DoTask".
				//The third argument should be the DoTaskArgs structure,
				//and the last argument should be nil.
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					//传递worker到通道里面
					go func() {
						registerChan <- worker
					}()
					break
				}
			}
		}(i, n_other, phase)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
