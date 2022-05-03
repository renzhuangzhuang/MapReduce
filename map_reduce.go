package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var lock sync.Mutex

func main() {

	txt_name := "C:\\Users\\njupttest\\Desktop\\1.txt"
	inputFile, err := os.Open(txt_name)
	if err != nil {
		fmt.Println("read file error =", err)
		return
	}
	defer inputFile.Close()
	// 一行一行读取文件，句柄形式
	file := bufio.NewReader(inputFile)
	//开辟 用于map到reduce 的通道
	map_reduce_channel := make(chan map[string]int, 100)
	//保存reduce结果
	result_out := make(map[string]int)

	// 定义这个flag 就是保证 map_reduce_channel 通道 在文件还没读完，而导致的reduce退出
	flag_value := false
	flag := &flag_value
	// 协程 读取文件
	go read_txt(file, map_reduce_channel, flag)
	// 循环处理 计算问题
	for {
		go redece_deal(<-map_reduce_channel, result_out)
		// 通道里面值全部取完 退出
		if len(map_reduce_channel) == 0 && *flag {
			break
		}

	}
	result, _ := os.Create("result.txt")
	defer result.Close()
	// 参考华子的写法
	sortmap := []string{}
	// 按单词排序 后面再整合
	for k := range result_out {
		sortmap = append(sortmap, k)
	}
	sort.Strings(sortmap)
	for _, v := range sortmap {
		result.WriteString(v + ":" + strconv.Itoa(result_out[v]) + "\n")
	}

}

func read_txt(file *bufio.Reader, map_reduce_channel chan map[string]int, flag *bool) { // , map_reduce_channel chan map[string]int
	for {
		line, err := file.ReadString('\n')
		//fmt.Println(Map_deal(line)) 这边读出来 都是正常的 map格式

		map_reduce_channel <- Map_deal(line)

		//fmt.Println(Map_deal(line))
		if err != nil && err != io.EOF {
			fmt.Println(err)
		}
		if err == io.EOF {
			close(map_reduce_channel)
			*flag = true
			break
		}
	}
}

// 生成 单词：数值 形式的键值对 作为map功能
func Map_deal(line string) (message map[string]int) {
	message = make(map[string]int)
	line1 := strings.Fields(line)
	for _, vl := range line1 {
		message[vl]++
	}
	return message

}

// 用于计数 单词频次
func redece_deal(message map[string]int, message1 map[string]int) {
	// 这个地方需要加互斥锁 不加锁报错

	lock.Lock()
	for k, v := range message {
		message1[k] += v
	}
	lock.Unlock()

}
