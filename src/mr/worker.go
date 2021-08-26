package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func HandlerMap(mapf func(string, string) []KeyValue, filename string, mapId int, nReduce int) {
	log.Printf("HandlerMap\n fileName:%s\n nReduce:%d\n", filename, nReduce)
	intermediate := make([][]KeyValue, nReduce)
	/* 读取输入文件内容 */
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	/* 执行用户的 map 函数 得到 键值对列表 */
	kva := mapf(filename, string(content))
	/* 对键值对列表进行分桶 */
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	// 创建中间目录
	intermediateDir := "./mr-intermediate"
	if _, err := os.Stat(intermediateDir); err != nil {
		err := os.MkdirAll(intermediateDir, 0711)
		if err != nil {
			log.Fatalf("Error creating directory %v", err)
		}
	}

	/* 将分桶后的结果写入文件 */
	for index, bucket := range intermediate {
		/* 变成 json */
		jsons, err := json.Marshal(bucket)
		if err != nil {
			log.Fatalf("json.Marshal: %v", err)
		}
		/* 创建临时文件 */
		tmpfile, err := ioutil.TempFile(intermediateDir, "mr-temp.*.json")
		if err != nil {
			log.Fatalf("ioutil.Tempfile: %v", err)
		}
		log.Printf("tempFile: %s\n", tmpfile.Name())
		/* 写入临时文件 */
		fmt.Fprintf(tmpfile, string(jsons))
		tmpfile.Close()
		/* 原子重命名 */
		os.Rename(tmpfile.Name(), fmt.Sprintf("%s/mr-%d-%d", intermediateDir, mapId, index))
	}

	FinalMapTaskRPC(mapId)
}

func HandlerReduce(reducef func(string, []string) string, reduceId int, nMap int) {
	log.Printf("HandlerReduce\n")
	intermediate := []KeyValue{}

	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("%s/mr-%d-%d", "./mr-intermediate", i, reduceId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()
		kva := []KeyValue{}
		json.Unmarshal([]byte(content), &kva)
		intermediate = append(intermediate, kva...)
		// reducef(string(content), kva)
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + fmt.Sprint(reduceId)
	// ofile, _ := os.Create(oname)
	// defer ofile.Close()

	/* 创建临时文件 */
	tmpfile, err := ioutil.TempFile("./", oname)
	if err != nil {
		log.Fatalf("ioutil.Tempfile: %v", err)
	}
	log.Printf("tempFile: %s\n", tmpfile.Name())

	i := 0
	for i < len(intermediate) {
		j := i + 1
		/* [i,j) 同 key */
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		/* key相同的那些项的value整合到一个 value 数组中 */
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		/* 调用 reduce 对 values 数组中的内容进行整合 -->output */
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpfile.Close()
	/* 原子重命名 */
	os.Rename(tmpfile.Name(), oname)

	FinalReduceTaskRPC(reduceId)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// Your worker implementation here.
		reply := GetTaskRPC()
		switch reply.Type {
		case Map:
			HandlerMap(mapf, reply.InputFileName, reply.MapId, reply.NReduce)
			// do map task
		case Reduce:
			HandlerReduce(reducef, reply.ReduceId, reply.NMap)
		// do reduce task
		case Done:
			os.Exit(0)
		}
	}
}

/* 从服务器获取任务 */
func GetTaskRPC() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		/* 服务器挂了 */
		os.Exit(0)
	}
	return reply
}

func FinalMapTaskRPC(mapId int) {
	args := MapFinalArgs{mapId}
	reply := MapFinalReply{}
	ok := call("Coordinator.MapFinal", &args, &reply)
	if !ok || reply.Ack != 0 {
		/* 服务器挂了 */
		os.Exit(0)
	}
	log.Printf("the server have acked this map task!")
}

func FinalReduceTaskRPC(reduceId int) {
	args := ReduceFinalArgs{reduceId}
	reply := ReduceFinalReply{}
	ok := call("Coordinator.ReduceFinal", &args, &reply)
	if !ok || reply.Ack != 0 {
		/* 服务器挂了 */
		os.Exit(0)
	}
	log.Printf("the server have acked this reduce task!")
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("call: %s\n", err)
	return false
}
