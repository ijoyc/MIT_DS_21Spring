package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MapWorker struct {
	id int
	reduceNum int
	// {reduce task number: [kvs]}
	intermediate map[int][]KeyValue
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// !!! If server set nil or 0 to an field on res, that field keep its original value, rather than change to nil or 0.
	// So everytime we should use a new object as the receiver.
	for true {
		req := GetTaskRequest{}
		res := GetTaskResponse{}

		if !call("Coordinator.GetTask", req, &res) { 
			break 
		}

		if res.Command == Exit {
			break
		} else if res.Command == Wait {
			time.Sleep(time.Second)
		} else if res.Command == Map {
			paths, ok := doMap(res.Paths, res.ID, res.ReduceNum, mapf)
			if ok {
				notifyTaskFinish(res.ID, Maping, paths)
			}
		} else if res.Command == Reduce {
			if doReduce(res.Paths, res.ID, reducef) {
				notifyTaskFinish(res.ID, Reducing, res.Paths)
			}
		}
	}
}

// ================== Map ==================

func intermediatePath(ID int, reduceTaskNum int) string {
	return fmt.Sprintf("mr-%d-%d", ID, reduceTaskNum)
}

func doMap(paths []string, ID int, reduceNum int, mapf func(string, string) []KeyValue) ([]string, bool) {
	var mapWorker MapWorker
	mapWorker.id = ID
	mapWorker.reduceNum = reduceNum
	mapWorker.intermediate = make(map[int][]KeyValue)

	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			log.Println("worker: cannot open ", path, err)
			return nil, false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Println("worker: cannot read ", path, err)
			return nil, false
		}
		file.Close()

		kva := mapf(path, string(content))
		for _, kv := range kva {
			reduceTaskNum := ihash(kv.Key) % reduceNum
			mapWorker.intermediate[reduceTaskNum] = append(mapWorker.intermediate[reduceTaskNum], kv)
		}
	}

	return writeIntermediate(mapWorker)
}

func writeIntermediate(mapWorker MapWorker) ([]string, bool) {
	targetPaths := make([]string, mapWorker.reduceNum)

	for reduceTaskNum, kva := range mapWorker.intermediate {
		targetPath := intermediatePath(mapWorker.id, reduceTaskNum)
		tmpfile, err := ioutil.TempFile("", targetPath)
		if err != nil {
			log.Printf("worker: cannot create tmp file with prefix %v, err: %v\n", targetPath, err)
			return nil, false
		}

		defer os.Remove(tmpfile.Name())
		
		enc := json.NewEncoder(tmpfile)
		for _, kv := range kva {
			err = enc.Encode(&kv)
			if err != nil {
				log.Printf("worder: cannot encode json to file %v, err: %v\n", tmpfile.Name(), err)
				return nil, false
			}
		}

		os.Rename(tmpfile.Name(), targetPath)
		targetPaths[reduceTaskNum] = targetPath
	}

	return targetPaths, true
}

// ================== Reduce ==================

func doReduce(paths []string, reduceTaskNum int, reducef func(string, []string) string) bool {
	kva := readAll(paths)

	sort.Sort(ByKey(kva))

	return writeResult(kva, reduceTaskNum, reducef)
}

func readAll(paths []string) []KeyValue {
	var kva []KeyValue
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			log.Printf("worder: cannot open file %s for read: %v\n", path, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

func writeResult(kva []KeyValue, reduceTaskNum int, reducef func(string, []string) string) bool {
	targetPath := fmt.Sprintf("mr-out-%d", reduceTaskNum)
	file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		fmt.Printf("Cannot open file %s to write, err: %v\n", targetPath, err)
		return false
	}

	i := 0
	for i < len(kva) {
		j := i
		currentKey := kva[i].Key
		var values []string
		for j < len(kva) && kva[j].Key == currentKey {
			values = append(values, kva[j].Value)
			j++
		}
		output := reducef(currentKey, values)
		fmt.Fprintf(file, "%v %v\n", currentKey, output)
		i = j
	}

	file.Close()
	return true
}

// ================== Utils ==================

func notifyTaskFinish(ID int, kind Stage, paths []string) {
	req := FinishTaskRequest{}
	req.ID = ID
	req.Kind = kind
	req.Paths = paths
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
