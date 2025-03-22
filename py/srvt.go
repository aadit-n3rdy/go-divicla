package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:5678")
	if err != nil {
		panic(err)
	}
	conn.Write([]byte("1 2 3\n"))
	tensor := []float32{1, 2, 3, 4, 5, 6}
	buf := make([]byte, 0)
	for _, val := range tensor {
		buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(val))
	}
	fmt.Printf("%v %x\n", len(buf), buf)
	conn.Write(buf)
	conn.Close()
}
