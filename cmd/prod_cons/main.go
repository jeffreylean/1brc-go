package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Station struct {
	station string
	min     int32
	max     int32
	sum     int64
	count   int
}

var stations = make([]string, 0)

func main() {
	started := time.Now()
	file, err := os.Open("etc/measurement.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Leverage maximum CPU
	workerNum := runtime.NumCPU()
	runtime.GOMAXPROCS(workerNum)

	var chunk []byte
	var wg sync.WaitGroup
	resultMap := new(sync.Map)
	ptr := 0
	buff := make([]byte, 1024*1024)
	chunkChan := make(chan []byte, 100000)
	reader := bufio.NewReader(file)

	for i := 0; i < workerNum; i++ {
		go worker(chunkChan, resultMap, &wg)
	}

	for {
		chunk, ptr, err = getChunk(reader, &buff, ptr)
		if err != nil {
			panic(err)
		}
		if len(chunk) == 0 {
			close(chunkChan)
			break
		}
		wg.Add(1)
		chunkChan <- chunk
	}
	wg.Wait()

	sort.Strings(stations)
	print("{")
	for _, s := range stations {
		if r, ok := resultMap.Load(s); ok {
			r := r.(*Station)
			fmt.Printf("%s:%.1f/%.1f/%.1f;\n", s, float64(r.min)/10, float64(r.sum)/float64(10)/float64(r.count), float64(r.max)/10)
		}
	}
	print("}\n")
	fmt.Printf("Time taken: %0.6f\n", time.Since(started).Seconds())
	fmt.Println(len(stations))
}

func worker(chunkChan <-chan []byte, resultMap *sync.Map, wg *sync.WaitGroup) {

	for chunk := range chunkChan {
		processChunk(chunk, resultMap)
		wg.Done()
	}
}

// Get chunk of data from reader
func getChunk(reader *bufio.Reader, buff *[]byte, ptr int) ([]byte, int, error) {
	buf := (*buff)
	n, err := reader.Read(buf[ptr:])
	if err != nil {
		if err == io.EOF {
			return []byte{}, 0, nil
		}
		return nil, 0, err
	}
	// Find the index of the last line, and call it a chunk
	lastLineIdx := bytes.LastIndexByte(buf[:ptr+n], '\n')
	if lastLineIdx < 0 {
		fmt.Println("lastline", lastLineIdx, n)
		return []byte{}, 0, nil
	}
	chunk := make([]byte, len(buf[:lastLineIdx+1]))
	copy(chunk, buf[:lastLineIdx+1])

	// Copy remaining of the incomplete bytes after the last line
	remaining := buf[lastLineIdx+1:]
	ptr = copy(buf, remaining)
	return chunk, ptr, nil
}

func processChunk(chunk []byte, resultMap *sync.Map) {
	for {
		// Split the bytes
		station, after, found := bytes.Cut(chunk, []byte(";"))
		if !found {
			break
		}
		stationString := string(station)
		index := 0
		isNegative := false

		// Check the sign of the decimal
		if after[index] == '-' {
			isNegative = true
			index++
		}

		temp := int32(0)
		for after[index] >= '0' && after[index] <= '9' {
			temp = temp*10 + int32(after[index]-'0')
			index++
		}
		// At this point the index shoud be `.`
		if after[index] != '.' {
			fmt.Println(string(after[index]))
			panic("Invalid input format")
		}
		// Skip `.`
		index++
		// Get the last digit
		temp = temp*10 + int32(after[index]-'0')
		if isNegative {
			temp = -temp
		}
		// Skip the `\n`
		index += 2
		// Update chunk
		chunk = after[index:]

		if st, exist := resultMap.Load(stationString); exist {
			st := st.(*Station)

			st.min = min(st.min, temp)
			st.max = max(st.max, temp)
			st.sum += int64(temp)
			st.count++
		} else {
			stationString := string(station)
			resultMap.Store(stationString, &Station{station: stationString, min: temp, max: temp, sum: int64(temp), count: 1})
			stations = append(stations, stationString)
		}
	}
}
