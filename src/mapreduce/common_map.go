package mapreduce

import (
	"encoding/json"
	"hash/fnv"
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
	//打开文件
	inputFile, err := os.Open(inFile)

	if err != nil {
		log.Fatal("doMap: open input file ", inFile, " error: ", err)
	}
	defer inputFile.Close()
	//获取文件的信息
	fileInfo, err := inputFile.Stat()
	if err != nil {
		log.Fatal("doMap: getstat input file ", inFile, " error: ", err)
	}
	//创建用于存储数据的切片
	data := make([]byte, fileInfo.Size())
	//读取文件
	_, err = inputFile.Read(data)
	if err != nil {
		log.Fatal("doMap: read input file ", inFile, " error: ", err)
	}
	//通过给定的函数来获取文件转换成的keyvalue
	keyValues := mapF(inFile, string(data))

	for i := 0; i < nReduce; i++ {

		fileName := reduceName(jobName, mapTaskNumber, i)
		reduceFile, err := os.Create(fileName)
		if err != nil {
			log.Fatal("doMap: create intermediate file ", fileName, " error: ", err)
		}
		defer reduceFile.Close()

		enc := json.NewEncoder(reduceFile)
		for _, kv := range keyValues {
			if ihash(kv.Key)%nReduce == i {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("doMap: encode error: ", err)
				}
			}
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
