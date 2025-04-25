package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

const (
	SOCKET_PATH   = "/tmp/unix_socket_benchmark"
	PACKET_SIZE   = 1 * 3 * 16 * 112 * 112 * 4
	NUM_ITERATIONS = 1000
)

func server() {
	os.Remove(SOCKET_PATH)
	ln, err := net.Listen("unix", SOCKET_PATH)
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer ln.Close()
	defer os.Remove(SOCKET_PATH)

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		return
	}
	defer conn.Close()

	buffer := make([]byte, PACKET_SIZE)
	for {
		totalRead := 0
		for totalRead < PACKET_SIZE {
			n, err := conn.Read(buffer[totalRead:])
			if err != nil || n == 0 {
				return
			}
			totalRead += n
		}
		conn.Write(buffer[:totalRead]) // Echo back
	}
}

func client() {
	time.Sleep(1 * time.Second) // Give server time to start
	conn, err := net.Dial("unix", SOCKET_PATH)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer conn.Close()

	data := make([]byte, PACKET_SIZE)
	response := make([]byte, PACKET_SIZE)

	start := time.Now()
	for i := 0; i < NUM_ITERATIONS; i++ {
		_, err := conn.Write(data)
		if err != nil {
			fmt.Println("Error sending data:", err)
			return
		}

		totalRead := 0
		for totalRead < PACKET_SIZE {
			n, err := conn.Read(response[totalRead:])
			if err != nil {
				fmt.Println("Error receiving data:", err)
				return
			}
			totalRead += n
		}
	}
	end := time.Now()

	elapsed := end.Sub(start).Seconds()
	latency := (elapsed / float64(NUM_ITERATIONS)) * 1e6 // Convert to microseconds
	throughput := (float64(PACKET_SIZE*NUM_ITERATIONS) / elapsed) / (1024 * 1024) // MB/s

	fmt.Printf("Average Latency: %.2f µs\n", latency)
	fmt.Printf("Throughput: %.2f MB/s\n", throughput)
}

func main() {
	go server()
	time.Sleep(500 * time.Millisecond) // Give server some time to start
	client()
}

