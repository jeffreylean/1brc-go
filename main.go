package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)

type Station struct {
	station string
	min     float64
	max     float64
	sum     float64
	count   int
}

func main() {
	started := time.Now()
	file, err := os.Open("etc/measurement.txt")
	if err != nil {
		panic("Error opening input file")
	}
	defer file.Close()

	buff := make([]byte, 100)
	//scanner := bufio.NewScanner(file)
	reader := bufio.NewReader(file)

	result := make(map[string]Station)
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
			break
		}

		remaining := buff[lastLineIdx+1:]
		chunk = chunk[:lastLineIdx+1]

		for {
			// Split the bytes
			splits := bytes.Split(chunk, []byte("\n"))
			for i := 0; i < len(splits); i++ {
				station, temp, found := bytes.Cut(splits[i], []byte(";"))
				if !found {
					break
				}
				stationString := string(station)
				tempFloat, _ := strconv.ParseFloat(string(temp), 64)

				if st, exist := result[stationString]; exist {
					st.min = min(st.min, tempFloat)
					st.max = max(st.max, tempFloat)
					st.sum += tempFloat
					st.count++
				} else {
					stationString := string(station)
					result[stationString] = Station{station: stationString, min: tempFloat, max: tempFloat, sum: tempFloat, count: 1}
					stations = append(stations, stationString)
				}

			}
			pointer = copy(buff, remaining)
		}

	}

	//for scanner.Scan() {
	//	line := scanner.Text()
	//	splits := strings.Split(line, ";")

	//	station := splits[0]
	//	temp, _ := strconv.ParseFloat(splits[1], 64)

	//	if st, exist := result[station]; exist {
	//		st.min = min(st.min, temp)
	//		st.max = max(st.max, temp)
	//		st.sum += temp
	//		st.count++
	//	} else {
	//		result[station] = Station{station: station, min: temp, max: temp, sum: temp, count: 1}
	//		stations = append(stations, station)
	//	}
	//}

	// Print the result
	sort.Strings(stations)
	print("{")
	for _, s := range stations {
		r := result[s]
		fmt.Printf("%s:%.1f/%.1f/%.1f;\n", s, r.min, r.sum/float64(r.count), r.max)
	}
	print("}\n")

	fmt.Printf("Time taken: %0.6f", time.Since(started).Seconds())
}
