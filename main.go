package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
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

	scanner := bufio.NewScanner(file)

	result := make(map[string]Station)
	stations := make([]string, 0)

	for scanner.Scan() {
		line := scanner.Text()
		splits := strings.Split(line, ";")

		station := splits[0]
		temp, _ := strconv.ParseFloat(splits[1], 64)

		if st, exist := result[station]; exist {
			st.min = min(st.min, temp)
			st.max = max(st.max, temp)
			st.sum += temp
			st.count++
		} else {
			result[station] = Station{station: station, min: temp, max: temp, sum: temp, count: 1}
			stations = append(stations, station)
		}
	}

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
