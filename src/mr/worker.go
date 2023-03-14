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
	"strconv"
	"time"
)

type OperationStatus string

const (
	Processing  OperationStatus = "processing"
	Completed   OperationStatus = "completed"
	Idle        OperationStatus = "idle"
	Unprocessed OperationStatus = "unprocessed"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// declare a request structure.
var req ActiveWorker = ActiveWorker{
	Status: Idle,
}

// declare a reply structure.
var reply1 ActiveWorker = ActiveWorker{}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		success := GetTaskFromCoordinator()
		if !success {
			time.Sleep(time.Second)
			log.Print("RPC failure")
			continue
		}
		copyReplyIntoRequest()

		if req.Status == Processing {
			// handle new request
			handleAssignedTask(mapf, reducef)
		}
		time.Sleep(time.Second)
	}
}

func copyReplyIntoRequest() {
	req = ActiveWorker{}
	req.WorkerId = reply1.WorkerId
	req.Status = reply1.Status
	req.OperationType = reply1.OperationType
	req.FileNames = append([]string{}, reply1.FileNames...)
	req.TaskId = reply1.TaskId
	req.NReduce = reply1.NReduce
	req.TaskCompletionFileNames = append([]string{}, reply1.TaskCompletionFileNames...)
}

func GetTaskFromCoordinator() bool {
	ok := call("Coordinator.OnOperationStatusChange", &req, &reply1)
	return ok
}

func handleAssignedTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	if req.OperationType == Map {
		handleMapOperation(mapf)
	} else {
		handleReduceOperation(reducef)
	}
	req.Status = Completed
}

func handleMapOperation(mapf func(string, string) []KeyValue) {
	filename := req.FileNames[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	// Run map routine on file contents
	kva := mapf(filename, string(content))
	file.Close()
	// Creating buckets for storing files according to their hashes
	buckets := make([][]KeyValue, req.NReduce)
	for i := range buckets {
		buckets[i] = make([]KeyValue, 0)
	}

	for _, pair := range kva {
		bucketId := ihash(pair.Key) % req.NReduce
		buckets[bucketId] = append(buckets[bucketId], pair)
	}
	req.TaskCompletionFileNames = []string{}

	// Splitting key-value pairs into multiple buckets and storing them in corresponding files
	for id, subBucket := range buckets {
		var filename string = "mr-" + req.TaskId + "-" + strconv.Itoa(id)
		req.TaskCompletionFileNames = append(req.TaskCompletionFileNames, filename)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(file)
		for _, pair := range subBucket {
			error := enc.Encode(&pair)
			if error != nil {
				log.Fatalf("Json encode write error " + error.Error())
			}
		}
		defer file.Close()
	}
}

func handleReduceOperation(reducef func(string, []string) string) {
	var intermediate = []KeyValue{}
	// Parse through all files and extract key value pairs
	for _, filename := range req.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	filename := "mr-out-" + req.TaskId
	path, _ := os.Getwd()
	tempfile, _ := ioutil.TempFile(path, "prefix")

	defer os.Remove(tempfile.Name())

	// call Reduce on each distinct key in intermediate[], and print the result to mr-out-Y.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempfile.Close()
	os.Rename(tempfile.Name(), path+"/"+filename+".txt")
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args *ActiveWorker, reply *ActiveWorker) bool {
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
