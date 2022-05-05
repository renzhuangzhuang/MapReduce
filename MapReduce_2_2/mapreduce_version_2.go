package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//var lock sync.Mutex

func read_txt(file *os.File, map_reduce_channel chan map[string]int, file_start int, bs []byte, wg *sync.WaitGroup) { // , map_reduce_channel chan map[string]int
	file.Seek(int64(file_start), io.SeekStart)
	_, _ = file.Read(bs)
	map_reduce_channel <- Map_deal(string(bs))
	defer wg.Done()

}

// 生成 单词：数值 形式的键值对 作为map功能
func Map_deal(input string) (ans map[string]int) {
	ans = make(map[string]int)
	ss := strings.Fields(input)
	for _, v := range ss {
		word := strings.ToLower(v)
		for len(word) > 0 && (word[0] < 'a' || word[0] > 'z') {
			word = word[1:]
		}
		for len(word) > 0 && (word[len(word)-1] < 'a' || word[len(word)-1] > 'z') {
			word = word[:len(word)-1]
		}
		ans[word]++
	}
	return
	/*
		message = make(map[string]int)
		line1 := strings.Fields(line)
		for _, vl := range line1 {
			message[vl]++
		}
		return
	*/

}

// 用于计数 单词频次
func redece_deal(message map[string]int, message1 map[string]int, wg1 *sync.WaitGroup) {
	// 这个地方需要加互斥锁 不加锁报错
	for k, v := range message {

		message1[k] += v

		//message1[k] += v
	}
	defer wg1.Done()

}

func main() {
	var wg sync.WaitGroup
	var wg1 sync.WaitGroup
	start := time.Now()
	// 文件划分大小
	file_size := 500 * 1024 //定义字节数
	txt_name := "C:\\Users\\njupttest\\Desktop\\1.txt"
	fi, err := os.Stat(txt_name) //使用fi.size得到文件大小
	if err != nil {
		fmt.Println(err)
	}
	file_num := math.Ceil(float64(fi.Size()) / float64(file_size)) // 得到文件的分块数

	//fmt.Println(fi.Size()) 得到字节数
	inputFile, err := os.OpenFile(txt_name, os.O_RDWR, os.ModePerm)
	if err != nil {
		fmt.Println("read file error =", err)
		return
	}
	defer inputFile.Close()

	map_reduce := make([]chan map[string]int, int(file_num))
	//保存reduce结果
	result_out := make([]map[string]int, int(file_num))
	for i := 0; i < int(file_num); i++ {
		// 计算每次偏移时的字节
		value1 := i * file_size
		bs := make([]byte, int(fi.Size())-(int(file_num)-1)*file_size)
		if i != int(file_num)-1 {
			bs = make([]byte, file_size)
		}
		map_reduce[i] = make(chan map[string]int, 10000)
		//result_out[i] = make(map[string]int)
		wg.Add(1)
		go read_txt(inputFile, map_reduce[i], value1, bs, &wg)

	}
	//原来放在上面循环时候 只能开辟少量的redece_deal 换成下面形式可以赋多个goroutine
	for i := 1; i < int(file_num); i++ {
		result_out[i] = make(map[string]int)
		for j := 1; j < 2000; j++ {
			wg1.Add(1)
			go redece_deal(<-map_reduce[i], result_out[i], &wg1)
			if len(map_reduce[i]) == 0 {
				close(map_reduce[i])
				break
			}

		}

	}
	wg.Wait()
	wg1.Wait()
	cost := time.Since(start)
	fmt.Printf("cost_time = [%s]", cost)

	result_all := make(map[string]int)
	for i := range result_out {
		for k, v := range result_out[i] {
			result_all[k] += v
		}
	}

	result, _ := os.Create("result.txt")
	defer result.Close()
	// 参考华子的写法
	sortmap := []string{}
	for k := range result_all {
		sortmap = append(sortmap, k)
	}
	sort.Strings(sortmap)
	for _, v := range sortmap {
		result.WriteString(v + ":" + strconv.Itoa(result_all[v]) + "\n")
	}

}
