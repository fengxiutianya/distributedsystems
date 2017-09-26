package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
// Each intermediate file contains a prefix,the map task number,
// and the reduce task number

// Call ihash() (see below) on each key, mod nReduce, to pick r for a key/value pair.

// mapF() is the map function provided by the application. The first
// argument should be the input file name, though the map function
// typically ignores it. The second argument should be the entire
// input file contents. mapF() returns a slice containing the
// key/value pairs for reduce; see common.go for the definition of
// KeyValue.
//
// Look at Go's ioutil and os packages for functions to read
// and write files.
//
// Coming up with a scheme for how to format the key/value pairs on
// disk can be tricky, especially when taking into account that both
// keys and values could contain newlines, quotes, and any other
// character you can think of.
//
// One format often used for serializing data to a byte stream that the
// other end can correctly reconstruct is JSON. You are not required to
// use JSON, but as the output of the reduce tasks *must* be JSON,
// familiarizing yourself with it here may prove useful. You can write
// out a data structure as a JSON string to a file using the commented
// code below. The corresponding decoding functions can be found in
// common_reduce.go.
//
//   enc := json.NewEncoder(file)
//   for _, kv := ... {
//     err := enc.Encode(&kv)
//
// Remember to close the file after you have written all the values!
//
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string, //
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	//创建intermediate的文件
	var intermediate = make([]*os.File, nReduce)
	var err error
	for i := 0; i < nReduce; i++ {
		//得到中间为文件的名字
		intermediateName := reduceName(jobName, mapTaskNumber, i)
		intermediate[i], err = os.OpenFile(intermediateName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatal(err)
			return
		}
	}
	//读出给定文件的名字
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
	}
	KeyValue := mapF(inFile, string(contents))

	for key, value := range KeyValue {
		//Call ihash() (see below) on each key, mod nReduce, to pick r for a key/value pair.
		n := ihash(string(key)) % nReduce
		enc := json.NewEncoder(intermediate[n])
		enc.Encode(&value)
	}

	//关闭所有文件
	for i := 0; i < nReduce; i++ {
		if err = intermediate[i].Close(); err != nil {
			log.Fatal(err)
		}
	}

}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
