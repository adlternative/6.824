package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

/* 工作者状态 */
type WorkerStatus int

const (
	Idle WorkerStatus = iota
	InProgress
	Completed
)

/* 任务类型 */
type TaskType int

const (
	Map TaskType = iota
	Reduce
	None
)

/* 任务状态 */
type TaskStatus int

const (
	UnHandle TaskStatus = iota
	Handling
	Handled
)

type Task struct {
	Reply  TaskReply
	Status TaskStatus /* 任务状态 */
	// Processtime /* 起始时间 */
	// Worker /* 关联的工作者 */
}

type Coordinator struct {
	NReduce int /* reduce 分块数量 */
	NMap    int /* map 任务总数 */

	// HandlingMapTasksSet map[int]Task /* 任务集合 */
	// HandledMapTasksSet  map[int]Task /* 任务集合 */
	// UnHandleMapTasksSet map[int]Task /* 任务集合 */

	UnHandleMapTasks    []Task       /* 没有处理的任务列表 */
	HandlingMapTasksSet map[int]Task /* 进行中的 map 任务集合 */
	// HandledMapTasksSet  map[int]Task /* 已结束的 map 任务集合 */

	UnHandleReduceTasks    []Task       /* 没有处理的任务列表 */
	HandlingReduceTasksSet map[int]Task /* 进行中的 reduce 任务集合 */

	MapIsDone    bool
	ReduceIsDone bool

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.MapIsDone && c.ReduceIsDone
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// if (isDone)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ReduceIsDone {
		/* reduce 都完成了那就结束吧 */
		reply.Type = None
	} else if c.MapIsDone {
		/* map 全部结束  */
		if len(c.UnHandleReduceTasks) != 0 {
			/*  还有一些 没有处理的 reduce 任务 */
			task := c.UnHandleReduceTasks[0]
			c.UnHandleReduceTasks = c.UnHandleReduceTasks[1:]
			*reply = task.Reply
		} else if len(c.HandlingReduceTasksSet) != 0 {
			/* 进行中的 */ reply.Type = None
		} else {
			reply.Type = None
		}
	} else {
		// c.MapIsDone == false

		if len(c.UnHandleMapTasks) != 0 {
			/* 仍然有 map 任务可做 */
			task := c.UnHandleMapTasks[0]
			c.UnHandleMapTasks = c.UnHandleMapTasks[1:]
			c.HandlingMapTasksSet[task.Reply.MapId] = task
			*reply = task.Reply
			log.Printf("Now the tasks lens: %d %d\n", len(c.UnHandleMapTasks), len(c.HandlingMapTasksSet))
		} else if len(c.HandlingMapTasksSet) != 0 {
			/* 进行中的 */ reply.Type = None
		} else {
			reply.Type = None
		}
	}
	return nil
}

func (c *Coordinator) MapFinal(args *MapFinalArgs, reply *MapFinalReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	/* 添加到已决任务列表中 */
	if task, ok := c.HandlingMapTasksSet[args.MapId]; ok {
		task.Reply.Type = Reduce

		c.UnHandleReduceTasks = append(c.UnHandleReduceTasks, task)
		delete(c.HandlingMapTasksSet, args.MapId)
	} else {
		log.Printf("It seems like we have handled this map task before.")
	}
	log.Printf("Now the tasks lens: %d %d\n", len(c.UnHandleMapTasks), len(c.HandlingMapTasksSet))

	reply.Ack = 0
	/* 说明 map 任务全部结束 之后我们就只分配 reduce 任务就好了 */
	if len(c.UnHandleMapTasks) == 0 && len(c.HandlingMapTasksSet) == 0 {
		c.MapIsDone = true
		c.setReduceTasks()
	}
	return nil
}

func (c *Coordinator) ReduceFinal(args *ReduceFinalArgs, reply *ReduceFinalReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	/* 添加到已决任务列表中 */
	if _, ok := c.HandlingReduceTasksSet[args.ReduceId]; ok {
		delete(c.HandlingReduceTasksSet, args.ReduceId)
	} else {
		log.Printf("It seems like we have handled this reduce task before.")
	}
	log.Printf("Now the reduce tasks lens: %d %d\n", len(c.UnHandleReduceTasks), len(c.HandlingReduceTasksSet))

	reply.Ack = 0
	/* 说明 map 任务全部结束 之后我们就只分配 reduce 任务就好了 */
	if len(c.UnHandleReduceTasks) == 0 && len(c.HandlingReduceTasksSet) == 0 {
		c.ReduceIsDone = true
	}
	return nil
}

func (c *Coordinator) setReduceTasks() {
	/* 向未处理的REDUCE 任务列表中添加新任务 */
	for i := 0; i != c.NReduce; i++ {
		c.UnHandleReduceTasks = append(c.UnHandleReduceTasks,
			Task{
				Reply: TaskReply{
					Type:     Reduce,
					ReduceId: i,
					NMap:     c.NMap,
				},
				Status: UnHandle,
			})
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		HandlingMapTasksSet:    make(map[int]Task),
		HandlingReduceTasksSet: make(map[int]Task),
		NReduce:                nReduce,
		NMap:                   len(files),
	}

	for i, file := range files {
		c.UnHandleMapTasks = append(c.UnHandleMapTasks,
			Task{
				Reply: TaskReply{
					Type:          Map,
					NReduce:       nReduce,
					InputFileName: file,
					MapId:         i,
				},
				Status: UnHandle,
			})
	}
	c.server()
	return &c
}
