package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

//1358730414@qq.com

// doReduce manages one reduce task:
//1. it reads the intermediate key/value pairs (produced by the map phase) for this task,
//
//2. sorts the intermediate key/value pairs by key, calls the user-defined reduce function (reduceF) for each key,
//
//3. and writes the output to disk.
//
// You will need to write this function.
//
// You'll need to read one intermediate file from each map task;
// reduceName(jobName, m, reduceTaskNumber) yields the file name from map task m.

// Your doMap() encoded the key/value pairs in the intermediate
// files, so you will need to decode them. If you used JSON, you can
// read and decode by creating a decoder and repeatedly calling
// .Decode(&kv) on it until it returns an error.
//
// You may find the first example in the golang sort package
// documentation useful.
//
// reduceF() is the application's reduce function. You should
// call it once per distinct key, with a slice of all the values
// for that key. reduceF() returns the reduced value for that key.
//
// You should write the reduce output as JSON encoded KeyValue
// objects to the file named outFile. We require you to use JSON
// because that is what the merger than combines the output
// from all the reduce tasks expects. There is nothing special about
// JSON -- it is just the marshalling format we chose to use. Your
// output code will look something like this:
//
// enc := json.NewEncoder(file)
// for key := ... {
// 	enc.Encode(KeyValue{key, reduceF(...)})
// }
// file.Close()
//
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	reduceResult := make(map[string][]string)
	//1. it reads the intermediate key/value pairs (produced by the map phase) for this task,
	//
	// enc := json.NewEncoder(routFile)
	for i := 0; i < nMap; i++ {
		//intermediate file name
		intermediateName := reduceName(jobName, i, reduceTaskNumber)

		//打开文件
		tmpfile, err1 := os.OpenFile(intermediateName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err1 != nil {
			log.Fatal(err1)
			return
		}
		dec := json.NewDecoder(tmpfile)

		for dec.More() {
			var m map[string]string
			// decode an array value (Message)
			err := dec.Decode(&m)
			if err != nil {
				log.Fatal(err)
			}
			reduceResult[m["Key"]] = append(reduceResult[m["Key"]], m["Value"])

		}
		if err1 = tmpfile.Close(); err1 != nil {
			// log.Fatal(err1)
		}

	}
	var keys []string
	for key, _ := range reduceResult {
		keys = append(keys, key)
	}

	//2. sorts the intermediate key/value pairs by key, calls the user-defined reduce function (reduceF) for each key,
	//3. and writes the output to disk.
	sort.Strings(keys)

	//输出的文件
	routFile, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
		return
	}
	enc := json.NewEncoder(routFile)
	for _, val := range keys {
		enc.Encode(KeyValue{val, reduceF(val, reduceResult[val])})
	}
	//关闭输出文件
	if err = routFile.Close(); err != nil {
		log.Fatal(err)
	}

}
