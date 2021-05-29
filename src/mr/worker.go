package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MapTask struct {
	id int
	reduceNum int
	// {reduce task number: [kvs]}
	intermediate map[int][]KeyValue
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	req := GetTaskRequest{}
	res := GetTaskResponse{}
	for call("Coordinator.GetTask", req, &res) {
		if res.Command == Exit {
			break
		} else if res.Command == Wait {
			time.Sleep(time.Second)
		} else {
			if doMap(res.Path, res.ID, res.ReduceNum, mapf) {
				notifyTaskFinish(res.ID)
			}
		}
	}
}

func intermediatePath(ID int, reduceTaskNum int) string {
	return fmt.Sprintf("mr-%d-%d", ID, reduceTaskNum)
}

func doMap(path string, ID int, reduceNum int, mapf func(string, string) []KeyValue) bool {
	var mapTask MapTask
	mapTask.id = ID
	mapTask.reduceNum = reduceNum
	mapTask.intermediate = make(map[int][]KeyValue)

	file, err := os.Open(path)
	if err != nil {
		log.Println("worker: cannot open ", path, err)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Println("worker: cannot read ", path, err)
		return false
	}
	file.Close()

	kva := mapf(path, string(content))
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % reduceNum
		mapTask.intermediate[reduceTaskNum] = append(mapTask.intermediate[reduceTaskNum], kv)
	}

	if !writeIntermediate(mapTask) {
		return false
	}
	return true
}

func writeIntermediate(mapTask MapTask) bool {
	for reduceTaskNum, kva := range mapTask.intermediate {
		targetPath := intermediatePath(mapTask.id, reduceTaskNum)
		tmpfile, err := ioutil.TempFile("", targetPath)
		if err != nil {
			log.Printf("worker: cannot create tmp file with prefix %v, err: %v\n", targetPath, err)
			return false
		}

		defer os.Remove(tmpfile.Name())
		
		enc := json.NewEncoder(tmpfile)
		for _, kv := range kva {
			err = enc.Encode(&kv)
			if err != nil {
				log.Printf("worder: cannot encode json to file %v, err: %v\n", tmpfile.Name(), err)
				return false
			}
		}

		os.Rename(tmpfile.Name(), targetPath)
	}

	return true
}

func notifyTaskFinish(ID int) {
	req := FinishTaskRequest{}
	req.ID = ID
	res := FinishTaskResponse{}
	call("Coordinator.FinishTask", req, &res)
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

	fmt.Println(err)
	return false
}
