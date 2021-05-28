package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskState int

const TimeOut = 10

const (
	Ready TaskState = iota
	Ongoing
	Finished
)

type Task struct {
	ID int
	Path string
	State TaskState

	timer *time.Timer
}

func NewTask(id int, path string) *Task {
	task := new(Task)
	task.ID = id
	task.Path = path
	task.State = Ready
	return task
}

type Coordinator struct {
	// Your definitions here.
	Tasks []*Task
	ReduceNum int

	pendingIndexes []int
	finishedCount int
	mutex sync.Mutex
}

func (c *Coordinator) finished() bool {
	return c.finishedCount == len(c.Tasks)
}

// ================== RPC handlers for the worker to call ==================

func (c *Coordinator) GetTask(req GetTaskRequest, res *GetTaskResponse) error {
	c.mutex.Lock()
	defer func() { c.mutex.Unlock() }()

	if c.finished() {
		log.Println("All tasks are finished, please exit.")
		res.Command = Exit
	} else if len(c.pendingIndexes) == 0 {
		log.Println("All tasks are ongoing, please wait for a while.")
		res.Command = Wait
	} else {
		index := c.pendingIndexes[0]
		task := c.Tasks[index]
		if task.State != Ready {
			log.Printf("Invalid task state: expected to be Ready, got %v\n", task.State)
		}

		log.Printf("Ask a new client to execute task %d, path = %v, reduceNum = %d\n", task.ID, task.Path, c.ReduceNum)

		c.pendingIndexes = c.pendingIndexes[1:]
		res.Command = Exec
		res.ID = task.ID
		res.Path = task.Path
		res.ReduceNum = c.ReduceNum

		task.State = Ongoing
		task.timer = time.AfterFunc(TimeOut * time.Second, func() {
			c.mutex.Lock()
			defer func() { c.mutex.Unlock() }()

			if task.State == Ongoing {
				log.Printf("Task %d timeout, will reset state to Ready\n", index)
				task.State = Ready
				c.pendingIndexes = append(c.pendingIndexes, index)
			}
		})
	}

	return nil
}

func (c *Coordinator) FinishTask(req FinishTaskRequest, res *FinishTaskResponse) error {
	c.mutex.Lock()
	defer func() { c.mutex.Unlock() }()

	task := c.Tasks[req.ID]
	if task.State != Ongoing {
		log.Printf("Invalid task state for id %d, expected to be Ongoing, got %v\n", task.ID, task.State)
	}
	task.State = Finished
	task.timer.Stop()
	c.finishedCount += 1

	log.Printf("Task %d finished, current finish count: %d\n", req.ID, c.finishedCount)

	res.Status = OK

	return nil
}

// ========================================================================

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
	return c.finished()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	var tasks []*Task
	var indexes []int
	for i, path := range files {
		tasks = append(tasks, NewTask(i, path))
		indexes = append(indexes, i)
	}
	c.Tasks = tasks
	c.pendingIndexes = indexes
	c.ReduceNum = nReduce

	c.server()
	return &c
}
