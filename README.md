# Google MapReduce
A simple implementation of Google's Map Reduce Paper by Jeffrey Dean and Sanjay Ghemawat using Golang.

#### Overview
Map and Reduce cluster operations inspired by Google Map Reduce Paper. In order to accelerate fie processing distributed over many machines, this mechanism was designed. Developers can build applications without the need to worry about scaling and running them on multiple machines.

#### Implementation
This repository is an implementation of a simpler version of Map Reduce running on multiple processes on a single machine. It consists of a central Master(coordinator) process which manages the job of assigning tasks and handling task failures. Multiple worker processes are created which run the Map and Reduce tasks assigned by the master. 

![MapReduceImplementation](https://user-images.githubusercontent.com/25281293/224888257-38c718a6-b6e9-413f-a145-ea75348a52fc.png)

The above image shows a simple representation of the system. There are n input files which have to be processed. The master has various data structures to keep track of these files and their progress. There are multiple Worker processes which request for a task from Master through a Remote Procedure Call(RPC), in this project it is an inter process call made through Unix sockets. The map operation splits each file into m buckets, where m is the number of output files to be generated as specified by the user.
The split is created using a hash function to evenly distribute the generated data(which is in the form of key-value pairs). Once all map tasks are completed, Master assigns reduce tasks to workers and the split intermediate files are reduces to m output files. 

This system can handle worker crashes and failures by constantly observing the time taken to process and dynamically re-assigns the failed tasks to other workers.

#### Usage
Clone the repository and cd into MapReduce/src/main directory on your terminal. Run the following command to create a fresh build of plugin wc.go 
```
go build -buildmode=plugin ../mrapps/crash.go
```
On one terminal window, run the coordinator process from the same directory using
```
go run mrcoordinator.go pg-*
```
Run the following child process on multiple terminal windows
```
go run mrworker.go wc.so
```
To run the test cases, you can run the following command
```
bash test-mr.sh
```

#### Acknowledgements
1. Google MapReduce paper which can be found in the link [here](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).
2. MIT for providing the base materials to build this project which can be found [here](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html).
