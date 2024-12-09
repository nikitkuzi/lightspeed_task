package main

import (
	"bufio"
	"fmt"
	"io"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
		var line string
		line, _ = reader.ReadString('\n')
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
// convert it into int
// set the corrsponding bit to 1
func fillBitmap(fileWithIPName string, bitMapFileName string, chunk []int64) { //  wg *sync.WaitGroup
	var start int64 = chunk[0]
	var end int64 = chunk[1]
	ipFile, _ := os.Open(fileWithIPName)
	defer ipFile.Close()
	bitmapFile, _ := os.OpenFile(bitMapFileName, os.O_RDWR, 0644)
	defer bitmapFile.Close()

	ipFile.Seek(int64(chunk[0]), io.SeekStart)
	reader := bufio.NewReader(ipFile)
	for start < end {
		ip, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		start += int64(len(ip))
		ip = strings.TrimSpace(ip)
		parts := strings.Split(string(ip), ".")
		var octets [4]int
		for i := 0; i < 4; i++ {
			octets[i], err = strconv.Atoi(parts[i])
		}
		convertedIP := uint32(octets[0]<<24 | octets[1]<<16 | octets[2]<<8 | octets[3])
		bytePos := int64(convertedIP / 8)
		bitOffset := convertedIP % 8

		oneByte := make([]byte, 1)
		_, err = bitmapFile.ReadAt(oneByte, bytePos)

		oneByte[0] |= (1 << (7 - bitOffset))
		_, err = bitmapFile.WriteAt(oneByte, bytePos)

	}

}

func countUniqueIPs(bitmapFile string, chunk []int64) uint32 {
	startPos := chunk[0] / 8
	endPos := chunk[1] / 8

	file, _ := os.Open(bitmapFile)

	file.Seek(startPos, io.SeekStart)

	var sum uint32
	reader := bufio.NewReader(file)

	for startPos < endPos {
		oneByte, _ := reader.ReadByte()
		startPos += 1
		sum += uint32(bits.OnesCount8(oneByte))
	}
	return sum
}

// create pairs to go through each of 2^32 nums
// to check later for set bits
func splitRangeToChunks(processCount int) [][]int64 {
	chunks := make([][]int64, processCount)
	maxLen := int64(1 << 32)
	chunksLen := maxLen / int64(processCount)
	for i := 0; i < processCount-1; i++ {
		chunks[i] = []int64{int64(i) * chunksLen, int64(i+1) * chunksLen}
	}
	chunks[processCount-1] = []int64{int64(processCount-1) * chunksLen, maxLen + 1}

	return chunks
}

func main() {
	// config
	// fileWithIP := "ip_addresses"
	fileWithIP := "test_data3.txt"
	bitmapFileName := "bitmap.dat"
	processCount := 10

	bitmapFile, _ := os.Create(bitmapFileName)
	defer bitmapFile.Close()
	bitmapFile.Truncate((1 << 29))

	chunks := splitFileToChunks(fileWithIP, processCount)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(processCount)
	for i := 0; i < processCount; i++ {
		go func(i int) {
			defer wg.Done()
			fillBitmap(fileWithIP, bitmapFileName, chunks[i])
		}(i)
	}

	wg.Wait()
	end := time.Now()
	fmt.Println("done reading file and filling bitmap, took: ", end.Sub(start))

	rangeChunks := splitRangeToChunks(processCount)
	var result uint32
	wg.Add(processCount)
	for i := 0; i < processCount; i++ {
		go func(i int) {
			defer wg.Done()
			result += countUniqueIPs(bitmapFileName, rangeChunks[i])

		}(i)
	}
	wg.Wait()
	fmt.Println(result)

}
