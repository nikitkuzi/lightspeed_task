package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"strings"
	"sync"
	"time"
)

// constants for Hyper Log Log
var (
	b            = 15
	m            = int64(1 << b)
	alphaMM      = (0.7213 / (1 + 1.079/float64(m))) * float64(m) * float64(m)
	registers    = make([]int, m)
	processCount = 10
)

// hash function (SHA1)
func hash(item string) int64 {
	h := sha1.New()
	h.Write([]byte(item))
	hashValue := h.Sum(nil)
	return int64(hashValue[0])<<56 | int64(hashValue[1])<<48 | int64(hashValue[2])<<40 |
		int64(hashValue[3])<<32 | int64(hashValue[4])<<24 | int64(hashValue[5])<<16 |
		int64(hashValue[6])<<8 | int64(hashValue[7])
}

func rho(x int64) int {
	return bits.Len64(uint64(x) ^ (uint64(x) - 1))
}

// add function that updates the register based on the hash value
func add(item string) {
	hashValue := hash(item)
	registerIndex := hashValue & (m - 1) // Choose a register based on the first b bits
	registers[registerIndex] = max(registers[registerIndex], rho(hashValue>>b))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// calculate the sum of all registers in a given range
func countSum(start, end int) float64 {
	var sum float64
	for i := start; i < end; i++ {
		sum += math.Pow(0.5, float64(registers[i]))
	}
	return sum
}

// counts the number of registers with a value of zero
func countZeros(start, end int) int {
	var count int
	for i := start; i < end; i++ {
		if registers[i] == 0 {
			count++
		}
	}
	return count
}

// estimate the cardinality using HyperLogLog
func estimate() float64 {
	chunksLen := len(registers) / processCount
	chunks := make([][]int, processCount)
	for i := 0; i < processCount-1; i++ {
		chunks[i] = []int{i * chunksLen, (i + 1) * chunksLen}
	}
	chunks[processCount-1] = []int{(processCount - 1) * chunksLen, len(registers)}

	var wg sync.WaitGroup
	partialSumCh := make(chan float64, processCount)
	partialZerosCh := make(chan int, processCount)

	wg.Add(processCount)

	// Process each chunk to calculate partial sums and zeros
	for _, chunk := range chunks {
		go func(chunk []int) {
			defer wg.Done()
			partialSum := countSum(chunk[0], chunk[1])
			partialSumCh <- partialSum
			partialZeros := countZeros(chunk[0], chunk[1])
			partialZerosCh <- partialZeros
		}(chunk)
	}

	wg.Wait()
	close(partialSumCh)
	close(partialZerosCh)

	var partialSum float64
	for ps := range partialSumCh {
		partialSum += ps
	}

	Z := 1.0 / partialSum
	E := alphaMM * Z

	// if E <= 2.5 * m, use the raw estimate (adjusted)
	if E <= 2.5*float64(m) {
		var V int
		for v := range partialZerosCh {
			V += v
		}
		if V > 0 {
			E = float64(m) * math.Log(float64(m)/float64(V))
		}
	}

	// if the estimate is very large, apply a different correction
	if E > 1/30.0 {
		E = -(math.Pow(2, 32)) * math.Log(1-E/math.Pow(2, 32))
	}

	return E
}

// go through each pair of [start, end]
// adjust end to the closest start of the line
// shift current end and next start
func findNewLine(chunks [][]int64, fileWithIP string) {
	file, err := os.Open(fileWithIP)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	for i := 0; i < len(chunks)-1; i++ {
		file.Seek(int64(chunks[i][1]), io.SeekStart)
		line, _ := reader.ReadString('\n')
		reader.Discard(reader.Buffered())
		shift := int64(len(line))
		chunks[i][1] += shift
		chunks[i+1][0] += shift
	}
}

// create pairs of chunks depending on the filesize
func splitFileToChunks(fileWithIP string, processCount int) [][]int64 {
	fileInfo, err := os.Stat(fileWithIP)
	if err != nil {
		panic(err)
	}
	filesize := fileInfo.Size()

	chunksLen := filesize / int64(processCount)
	chunks := make([][]int64, processCount)
	for i := 0; i < processCount-1; i++ {
		chunks[i] = []int64{int64(i) * chunksLen, int64(i+1) * chunksLen}
	}
	chunks[processCount-1] = []int64{int64(processCount-1) * chunksLen, filesize}
	findNewLine(chunks, fileWithIP)
	return chunks
}

// read each IP
// add it to the Hyper Log Log
func readChunk(fileWithIPName string, chunk []int64) {
	var start int64 = chunk[0]
	var end int64 = chunk[1]
	ipFile, _ := os.Open(fileWithIPName)
	defer ipFile.Close()
	ipFile.Seek(int64(chunk[0]), io.SeekStart)
	reader := bufio.NewReader(ipFile)
	for start < end {
		ip, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		start += int64(len(ip))
		ip = strings.TrimSpace(ip)
		add(ip)
	}
}

func main() {
	// fileWithIPName := "ip_addresses"
	fileWithIPName := "test_data2.txt"

	chunks := splitFileToChunks(fileWithIPName, processCount)

	startTime := time.Now()

	var wg sync.WaitGroup
	wg.Add(processCount)
	for _, chunk := range chunks {
		go func(chunk []int64) {
			defer wg.Done()
			readChunk(fileWithIPName, chunk)
		}(chunk)
	}

	wg.Wait()

	// Estimate unique IPs using HyperLogLog
	uniqueIPs := estimate()
	endTime := time.Now()

	fmt.Printf("done reading file and counting, took: %v\n", endTime.Sub(startTime))
	fmt.Printf("Estimated unique IPs: %.0f\n", uniqueIPs)
}
