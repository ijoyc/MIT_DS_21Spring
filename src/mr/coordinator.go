package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

// ================== Constants ==================

const TimeOut = 10

// ================== Types ==================

type TaskState int

const (
	NotReady TaskState = iota
	Ready
	Ongoing
	Finished
)

type Task struct {
	ID int
	State TaskState
	Paths []string

	timer *time.Timer
}

type TaskGroup struct {
	tasks []*Task
	pendingIndexes []int
	finishedCount int
}

type Coordinator struct {
	// Your definitions here.
	maps *TaskGroup
	reduces *TaskGroup
	
	stage Stage
	reduceNum int
	mutex sync.RWMutex
}

// ================== Constructors ==================

func NewTask(id int, state TaskState, paths []string) *Task {
	task := new(Task)
	task.ID = id
	task.Paths = paths
	task.State = state
	return task
}

// ================== RPC handlers for the worker to call ==================

func (c *Coordinator) GetTask(req GetTaskRequest, res *GetTaskResponse) error {
	if c.finished() {
		log.Println("All tasks are finished, please exit.")
		res.Command = Exit
	} else if c.pending() {
		log.Println("All tasks are ongoing, please wait for a while.")
		res.Command = Wait
	} else {
		c.mutex.Lock()
		defer func() { c.mutex.Unlock() }()

		index := c.currentPendingIndexes()[0]
		task := c.currentTasks()[index]
		if task.State != Ready {
			log.Printf("Invalid %s task state: expected to be Ready, got %v\n", c.kindOfTask(), task.State)
		}

		log.Printf("Ask a new client to execute %s task %d, paths = %v, reduceNum = %d\n", c.kindOfTask(), task.ID, task.Paths, c.reduceNum)
		if c.stage == Maping {
			c.maps.pendingIndexes = c.maps.pendingIndexes[1:]
			res.Command = Map
		} else {
			c.reduces.pendingIndexes = c.reduces.pendingIndexes[1:]
			res.Command = Reduce
		}

		res.Paths = task.Paths
		// !!! If server set nil or 0 to an field on res, that field keep its original value, rather than change to nil or 0.
		// So everytime the client should use a new object as the response receiver.
		res.ID = task.ID
		res.ReduceNum = c.reduceNum

		task.State = Ongoing
		task.timer = time.AfterFunc(TimeOut * time.Second, func() {
			c.mutex.Lock()
			defer func() { c.mutex.Unlock() }()

			if task.State == Ongoing {
				log.Printf("task %d timeout, will reset state to Ready\n", index)
				task.State = Ready

				if c.stage == Maping {
					c.maps.pendingIndexes = append(c.maps.pendingIndexes, index)
				} else {
					c.reduces.pendingIndexes = append(c.reduces.pendingIndexes, index)
				}
			}
		})
	}

	return nil
}

func (c *Coordinator) FinishTask(req FinishTaskRequest, res *FinishTaskResponse) error {
	c.mutex.Lock()
	defer func() { c.mutex.Unlock() }()

	if c.stage != req.Kind {
		// TODO: error handling
		log.Printf("Receive outdated task: current stage = %d, kind of request = %d\n", c.stage, req.Kind)
		return nil
	}

	task := c.currentTasks()[req.ID]

	if task.State != Ongoing {
		log.Printf("Invalid %s task state for id %d, expected to be Ongoing, got %v\n", c.kindOfTask(), task.ID, task.State)
		return nil
	}
	task.State = Finished
	task.timer.Stop()

	c.taskDidFinish(req.ID, req.Paths)
	if c.stage == Maping && c.maps.finishedCount == len(c.maps.tasks) {
		c.stage = Reducing

		for _, reduceTask := range c.reduces.tasks {
			reduceTask.State = Ready
		}
	}

	res.Status = OK

	return nil
}

// ================== Utils ==================

func (c *Coordinator) finished() bool {
	c.mutex.RLock()
	defer func() { c.mutex.RUnlock() }()
	return c.stage == Reducing && c.reduces.finishedCount == len(c.reduces.tasks)
}

func (c *Coordinator) pending() bool {
	c.mutex.RLock()
	defer func() { c.mutex.RUnlock() }()
	return (c.stage == Maping && len(c.maps.pendingIndexes) == 0) || 
		(c.stage == Reducing && len(c.reduces.pendingIndexes) == 0)
}

// Following instance methods are thread safe and cannot be locked internally

func (c *Coordinator) kindOfTask() string {
	if c.stage == Maping {
		return "map"
	} else {
		return "reduce"
	}
}

func (c *Coordinator) currentTasks() []*Task {
	if c.stage == Maping {
		return c.maps.tasks
	} else {
		return c.reduces.tasks
	}
}

func (c *Coordinator) currentPendingIndexes() []int {
	if c.stage == Maping {
		return c.maps.pendingIndexes
	} else {
		return c.reduces.pendingIndexes
	}
}

func (c *Coordinator) taskDidFinish(id int, paths []string) {
	var finishedCount int
	if c.stage == Maping {
		c.maps.finishedCount += 1
		finishedCount = c.maps.finishedCount

		for i, reduceTask := range c.reduces.tasks {
			reduceTask.Paths = append(reduceTask.Paths, paths[i])
		}
	} else {
		c.reduces.finishedCount += 1
		finishedCount = c.reduces.finishedCount
	}
	log.Printf("%s task %d finished, current finish count: %d\n", c.kindOfTask(), id, finishedCount)
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
	return c.finished()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.maps = makeMapTasks(files, nReduce)
	c.reduces = makeReduceTasks(nReduce)
	c.stage = Maping
	c.reduceNum = nReduce

	c.server()
	return &c
}

func makeMapTasks(files []string, reduceNum int) *TaskGroup {
	var group TaskGroup
	var mapTasks []*Task
	var indexes []int
	for i, path := range files {
		mapTasks = append(mapTasks, NewTask(i, Ready, []string{ path }))
		indexes = append(indexes, i)
	}
	group.tasks = mapTasks
	group.pendingIndexes = indexes
	return &group
}

func makeReduceTasks(reduceNum int) *TaskGroup {
	var group TaskGroup
	var reduceTasks []*Task
	var indexes []int
	for i := 0; i < reduceNum; i++ {
		reduceTasks = append(reduceTasks, NewTask(i, NotReady, nil))
		indexes = append(indexes, i)
	}
	group.tasks = reduceTasks
	group.pendingIndexes = indexes
	return &group
}
