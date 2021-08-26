package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	// "strconv"
	"sync"
	// "sync/atomic"
	"time"
)

/* 任务类型 */
type TaskType int

const (
	Map TaskType = iota
	Reduce
	Done
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
	// FinalChan chan string
	ctx     context.Context
	done_fn context.CancelFunc
}

type Coordinator struct {
	NReduce int /* reduce 分块数量 */
	NMap    int /* map 任务总数 */

	MapTasks []*Task /* 没有处理的任务列表 */

	ReduceTasks []*Task /* 没有处理的任务列表 */

	TaskChan chan *Task

	MapLeftCount    sync.WaitGroup
	ReduceLeftCount sync.WaitGroup
	// GetTaskWaiterCount int32

	done chan bool
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
func (c *Coordinator) Done() {
	<-c.done
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// atomic.AddInt32(&c.GetTaskWaiterCount, 1)
	task, ok := <-c.TaskChan
	// atomic.AddInt32(&c.GetTaskWaiterCount, -1)
	if !ok {
		task = &Task{
			Reply: TaskReply{
				Type: Done,
			},
		}
	} else {
		go func() {
			select {
			case <-task.ctx.Done():
				if task.Reply.Type == Map {
					c.MapLeftCount.Done()
				} else if task.Reply.Type == Reduce {
					c.ReduceLeftCount.Done()
				}
				break
			case <-time.After(10 * time.Second):
				log.Println("任务超时！")
				/* 重新分配任务 */
				c.TaskChan <- task
			}
		}()
	}
	*reply = task.Reply
	return nil
}

func (c *Coordinator) MapFinal(args *MapFinalArgs, reply *MapFinalReply) error {
	task := c.MapTasks[args.MapId]
	select {
	case <-task.ctx.Done():
		log.Printf(" [%d] Map 任务已经被别的工作者结束！", args.MapId)
		break
	default:
		log.Printf(" [%d] Map 任务结束！", args.MapId)
		task.done_fn()
	}
	reply.Ack = 0
	return nil
}

func (c *Coordinator) ReduceFinal(args *ReduceFinalArgs, reply *ReduceFinalReply) error {
	task := c.ReduceTasks[args.ReduceId]
	select {
	case <-task.ctx.Done():
		log.Printf(" [%d] Reduce 任务已经被别的工作者结束！\n", args.ReduceId)
		break
	default:
		log.Printf(" [%d] Reduce 任务结束！\n", args.ReduceId)
		task.done_fn()
	}
	reply.Ack = 0
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	/* 初始化一个协调者 */
	c := Coordinator{
		// HandlingMapTasksSet:    make(map[int]*Task),
		// HandlingReduceTasksSet: make(map[int]*Task),
		NReduce:  nReduce,
		NMap:     len(files),
		TaskChan: make(chan *Task),
		done:     make(chan bool),
	}
	/* 初始化 waitGroup 用来同步 */
	c.MapLeftCount.Add(c.NMap)
	c.ReduceLeftCount.Add(c.NReduce)

	go c.PutAllMapTasks(files, nReduce)
	go c.PutAllReduceTasks()
	go c.PutDoneTasks()
	c.server()
	return &c
}

func (c *Coordinator) PutAllMapTasks(files []string, nReduce int) {
	/* 初始化 Map 任务队列 */
	for i, file := range files {
		task := &Task{
			Reply: TaskReply{
				Type:          Map,
				NReduce:       nReduce,
				InputFileName: file,
				MapId:         i,
			},
			Status: UnHandle,
		}
		task.ctx, task.done_fn = context.WithCancel(context.Background())
		c.MapTasks = append(c.MapTasks, task)
	}
	for i := 0; i < len(c.MapTasks); i++ {
		c.TaskChan <- c.MapTasks[i]
	}
}

func (c *Coordinator) PutAllReduceTasks() {
	c.MapLeftCount.Wait()
	// /* 向未处理的REDUCE 任务列表中添加新任务 */
	for i := 0; i != c.NReduce; i++ {
		task := &Task{
			Reply: TaskReply{
				Type:     Reduce,
				ReduceId: i,
				NMap:     c.NMap,
			},
		}
		task.ctx, task.done_fn = context.WithCancel(context.Background())
		c.ReduceTasks = append(c.ReduceTasks, task)
	}
	for i := 0; i < len(c.ReduceTasks); i++ {
		c.TaskChan <- c.ReduceTasks[i]
	}
}

func (c *Coordinator) PutDoneTasks() {
	log.Printf("PutDoneTasks begin")
	c.ReduceLeftCount.Wait()
	log.Printf("All Reduce down")
	// for atomic.LoadInt32(&c.GetTaskWaiterCount) > 0 {
	// log.Printf("%d", atomic.LoadInt32(&c.GetTaskWaiterCount))
	close(c.TaskChan)
	// }
	log.Printf("Waht")

	c.done <- true
	log.Printf("PutDoneTasks end")

}
