package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskArgs struct {
}

type TaskReply struct {
	Type TaskType /* 任务类型 */

	NReduce       int    /* reduce 分块数量 */
	InputFileName string /* 当任务类型为Map时，该字段表示输入文件名 */
	MapId         int    /* 当任务类型为Map时，该字段表示Map的Id */

	ReduceId int /* 当任务类型为Reduce时，该字段表示Reduce的Id */
	NMap     int /* 当任务类型为Reduce时，该字段表示Map任务总数 */
}

type MapFinalArgs struct {
	MapId int
}

type MapFinalReply struct {
	Ack int /* 0 就可以 */
}

type ReduceFinalArgs struct {
	ReduceId int
}

type ReduceFinalReply struct {
	Ack int /* 0 就可以 */
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
