package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type Command int

const (
	Map Command = iota
	Reduce
	Wait
	Exit
)

type Status int

const (
	OK Status = iota
)

type Stage int

const (
	Maping Stage = iota
	Reducing
)

type GetTaskRequest struct {

}

type GetTaskResponse struct {
	Command Command
	ID int
	Paths []string
	ReduceNum int
}

type FinishTaskRequest struct {
	ID int
	Kind Stage
	Paths []string
}

type FinishTaskResponse struct {
	Status Status
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
