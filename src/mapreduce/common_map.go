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
// Each intermediate file contains a prefix,the map task number,and the reduce task number
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

	file, err := os.Open(inFile)
	defer file.Close()

	if err != nil {
		log.Fatal("打开文件失败", err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal("获取文件信息失败", err)
	}
	contents := make([]byte, fileInfo.Size())
	//当没有新变量在左边，又和一个 _ 一起时，不需要加上 : 这个符号
	_, err = file.Read(contents)

	if err != nil {
		log.Fatal("获取文件内容失败", err)
	}
	keyvalues := mapF(inFile, string(contents))

	for i := 0; i < nReduce; i++ {

		intermidateName := reduceName(jobName, mapTaskNumber, i)
		reduceFile, err := os.Create(intermidateName)

		if err != nil {
			log.Fatal("创建文件失败", err)
		}
		defer reduceFile.Close()

		enc := json.NewEncoder(reduceFile)
		//这个地点要注意，[]keyvalues 是一个结构体类型的数组 key 是0 1 2 value 是struct
		// index,value = range 数组   key,value = range map 注意区别
		//ihash 是通过 KeyValue 结构体里面的key来做hash
		for _, key := range keyvalues {
			if ihash(key.Key)%nReduce == i {
				err := enc.Encode(&key)
				if err != nil {
					log.Fatal("写入文件失败", err)
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
