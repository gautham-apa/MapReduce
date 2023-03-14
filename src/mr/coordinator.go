package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Coordinator struct {
	inputFiles []string
	nReduce    int
}

type OperationType string

const (
	Map    OperationType = "map"
	Reduce OperationType = "reduce"
)

type MRTask struct {
	status   OperationStatus
	fileName string
}

type ActiveWorker struct {
	WorkerId                int
	Status                  OperationStatus
	OperationType           OperationType
	timeElapsed             int32
	FileNames               []string
	TaskId                  string
	NReduce                 int
	TaskCompletionFileNames []string
}

// map tasks
var filesToMap = []MRTask{}

// reduce tasks
var filesToReduce = []MRTask{}

// Active workers
var workers = []ActiveWorker{}

var workerId = 0

var nReduceCount = 1

var m sync.Mutex

func (c *Coordinator) OnOperationStatusChange(request *ActiveWorker, reply *ActiveWorker) error {
	m.Lock()
	defer m.Unlock()
	if request.Status == Idle {
		assignTask(request, reply)
	} else if request.Status == Completed {
		updateFileCompletion(request, reply)
	}
	copyWorkerInstance(request, reply)
	return nil
}

func copyWorkerInstance(from *ActiveWorker, to *ActiveWorker) {
	to.WorkerId = from.WorkerId
	to.Status = from.Status
	to.OperationType = from.OperationType
	to.FileNames = append([]string{}, from.FileNames...)
	to.TaskId = from.TaskId
	to.NReduce = from.NReduce
	to.TaskCompletionFileNames = append([]string{}, from.TaskCompletionFileNames...)
}

func updateFileCompletion(activeWorker *ActiveWorker, reply *ActiveWorker) {
	// Update all tasks on completion status
	updateFileStatus(activeWorker, Completed)
	// If map completion, add reply tasks to reduce queue
	if activeWorker.OperationType == Map {
		for _, filename := range activeWorker.TaskCompletionFileNames {
			filesToReduce = append(filesToReduce, MRTask{
				status:   Unprocessed,
				fileName: filename,
			})
		}
	}
	// Remove worker from active worker list
	for idx, worker := range workers {
		if worker.WorkerId == activeWorker.WorkerId {
			removeElementAtIndex(&workers, idx)
			break
		}
	}
	// Update status of worker
	activeWorker.Status = Idle
	activeWorker.TaskCompletionFileNames = []string{}
	activeWorker.FileNames = []string{}
	activeWorker.TaskId = ""
}

func assignTask(activeWorker *ActiveWorker, reply *ActiveWorker) {
	taskId, newMapTask := getUnmappedTask()
	// Add reduce file split count
	activeWorker.NReduce = nReduceCount
	if newMapTask != nil {
		workerId = workerId + 1
		activeWorker.WorkerId = workerId
		activeWorker.Status = Processing
		activeWorker.OperationType = Map
		activeWorker.timeElapsed = 0
		activeWorker.FileNames = []string{newMapTask.fileName}
		activeWorker.TaskId = strconv.Itoa(taskId)
		newWorker := ActiveWorker{}
		copyWorkerInstance(activeWorker, &newWorker)
		workers = append(workers, newWorker)
		return
	}
	// Do not start reduce tasks till all map tasks complete
	if !isAllMapTasksDone() {
		// Setting worker to idle state to wait for new tasks to be assigned
		activeWorker.Status = Idle
		return
	}

	taskNumber, newReduceTasks := getReduceTasks()
	if len(newReduceTasks) > 0 {
		fileNames := []string{}
		for _, task := range newReduceTasks {
			fileNames = append(fileNames, task.fileName)
		}
		workerId = workerId + 1
		activeWorker.WorkerId = workerId
		activeWorker.Status = Processing
		activeWorker.OperationType = Reduce
		activeWorker.timeElapsed = 0
		activeWorker.FileNames = fileNames
		activeWorker.TaskId = taskNumber
		workers = append(workers, *activeWorker)
		return
	} else {
		// Setting worker to idle state to wait for new tasks to be assigned
		activeWorker.Status = Idle
	}
}

func getUnmappedTask() (int, *MRTask) {
	for idx := range filesToMap {
		task := &filesToMap[idx]
		if task.status == Unprocessed {
			task.status = Processing
			return idx, task
		}
	}
	return -1, nil
}

func getReduceTasks() (string, []MRTask) {
	reduceTasks := []MRTask{}
	fileNumber := ""
	for _, task := range filesToReduce {
		if task.status == Unprocessed {
			fileNumber = strings.Split(task.fileName, "-")[2]
			break
		}
	}
	if fileNumber == "" {
		return "", reduceTasks
	}
	for idx := range filesToReduce {
		task := &filesToReduce[idx]
		if strings.Split(task.fileName, "-")[2] == fileNumber {
			task.status = Processing
			reduceTasks = append(reduceTasks, *task)
		}
	}
	return fileNumber, reduceTasks
}

func isAllMapTasksDone() bool {
	for _, task := range filesToMap {
		if task.status != Completed {
			return false
		}
	}
	return true
}

func isAllReduceTasksDone() bool {
	for _, task := range filesToReduce {
		if task.status != Completed {
			return false
		}
	}
	return true
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	updateWorkerTimeElapsed()
	return isAllMapTasksDone() && isAllReduceTasksDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, nReduce}
	for _, fileName := range files {
		filesToMap = append(filesToMap, MRTask{
			status:   Unprocessed,
			fileName: fileName,
		})
	}
	nReduceCount = nReduce
	c.server()
	return &c
}

func updateWorkerTimeElapsed() {
	for i := 0; i < len(workers); i++ {
		worker := &workers[i]
		worker.timeElapsed += 1
		if worker.timeElapsed > 10 {
			// worker crashed/unresponsive. Release tasks by changing state
			updateFileStatus(worker, Unprocessed)
			removeElementAtIndex(&workers, i)
			i--
		}
	}
}

func updateFileStatus(worker *ActiveWorker, status OperationStatus) {
	if worker.OperationType == Map {
		for _, fileName := range worker.FileNames {
			for idx := range filesToMap {
				task := &filesToMap[idx]
				if task.fileName == fileName {
					task.status = status
				}
			}
		}
	} else {
		for _, fileName := range worker.FileNames {
			for idx := range filesToReduce {
				task := &filesToReduce[idx]
				if task.fileName == fileName {
					task.status = status
				}
			}
		}
	}
}

func removeElementAtIndex(a *[]ActiveWorker, i int) {
	(*a)[i] = (*a)[len((*a))-1]
	(*a) = (*a)[:len((*a))-1]
}
