package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"
)

func main2() {
	started := time.Now()

	workerNum := runtime.NumCPU()
	runtime.GOMAXPROCS(workerNum)

	file, err := os.Open("etc/measurement.txt")
	if err != nil {
		panic("Error opening input file")
	}
	defer file.Close()

	// 1mb of buffer
	buff := make([]byte, 1024*1024)
	//scanner := bufio.NewScanner(file)
	reader := bufio.NewReader(file)

	result := make(map[string]*Station)
	stations := make([]string, 0)
	pointer := 0

	for {
		n, err := reader.Read(buff[pointer:])
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err.Error())
			panic("Error reading chunk")
		}
		chunk := buff[:pointer+n]
		lastLineIdx := bytes.LastIndexByte(chunk, '\n')
		if lastLineIdx < 0 {
			fmt.Println("lastline", lastLineIdx, n)
			break
		}

		remaining := buff[lastLineIdx+1:]
		chunk = chunk[:lastLineIdx+1]
	}

}

func worker(chunkChan chan<- []byte) {

}
