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
//2. sorts the intermediate key/value pairs by key,
//3. calls the user-defined reduce function (reduceF) for each key,
//
//4. and writes the output to disk.
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
	keyValues := make(map[string][]string, 0)
	//把当前reduce操作对应的文件内容读取出来，并存储到keyvalues中
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("doReduce: open intermediate file ", fileName, " error: ", err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			//类似于iterator.next 会一直获取内容，知道出错就结束
			err := dec.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := keyValues[kv.Key]
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}

	var keys []string

	for k, _ := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mergeFile, err := os.Create(outFile)

	if err != nil {
		log.Fatal("doReduce: create merge file ", outFile, " error: ", err)
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for _, k := range keys {
		res := reduceF(k, keyValues[k])
		err := enc.Encode(&KeyValue{k, res})
		if err != nil {
			log.Fatal("doReduce: encode error: ", err)
		}
	}
}
