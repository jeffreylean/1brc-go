package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)

type Station struct {
	station string
	min     int32
	max     int32
	sum     int64
	count   int
}

func main() {
	started := time.Now()
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

			if st, exist := result[stationString]; exist {
				st.min = min(st.min, temp)
				st.max = max(st.max, temp)
				st.sum += int64(temp)
				st.count++
			} else {
				stationString := string(station)
				result[stationString] = &Station{station: stationString, min: temp, max: temp, sum: int64(temp), count: 1}
				stations = append(stations, stationString)
			}
		}
		pointer = copy(buff, remaining)
	}

	// Print the result
	sort.Strings(stations)
	print("{")
	for _, s := range stations {
		r := result[s]
		fmt.Printf("%s:%.1f/%.1f/%.1f;\n", s, float64(r.min)/10, float64(r.sum)/float64(10)/float64(r.count), float64(r.max)/10)
	}
	print("}\n")

	fmt.Printf("Time taken: %0.6f", time.Since(started).Seconds())
}
