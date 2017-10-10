package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

	"../mapreduce"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// TODO: you have to write this function
	var res []mapreduce.KeyValue
	values := strings.FieldsFunc(contents,
		func(c rune) bool {
			return !unicode.IsLetter(c)
		})
	//此处注意理解mapreduce论文中wordcout 对于每一个单词的计数继承1
	for _, v := range values {
		res = append(res, mapreduce.KeyValue{v, "1"})
	}

	//下面是进行的优化，把每一个文件里面的相同的单词进行计数
	// tem := make(map[string]int)

	// for _, v := range values {
	// 	_, ok := tem[v]
	// 	if !ok {
	// 		tem[v] = 0
	// 	}
	// 	tem[v]++
	// }
	// for key, v := range tem {
	// 	res = append(res, mapreduce.KeyValue{key, strconv.Itoa(v)})
	// }
	return res
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	var sum int

	for _, v := range values {
		count, err := strconv.Atoi(v)
		if err != nil {
			log.Fatal("reduceF: strconv string to int error: ", err)
		}
		sum += count
	}
	return strconv.Itoa(sum)

}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
