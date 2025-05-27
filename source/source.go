package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"time"

	ot "github.com/aadit-n3rdy/go-divicla/orchestrator/types"
	"github.com/aadit-n3rdy/go-divicla/types"
)

func min(a float32, b float32) float32 {
	if a < b {
		return a
	} else {
		return b
	}
}

type Stream struct {
	conn       net.Conn
	writeBuf   *bufio.Writer
	TotalUnits float32
}

type Source struct {
	inpTensor   types.Tensor
	srcID       string
	orcAddr     string
	srcAddr     string
	deficit     float32
	totalUnits  float32
	streams     map[string]Stream
	orc         *rpc.Client
	deficitChan chan float32
}

func (src *Source) Init(tensorShape []int, srcID string, orcAddr string, srcAddr string) {
	sz := 1
	for _, val := range tensorShape {
		sz *= val
	}
	src.inpTensor.Sizes = tensorShape
	count := 1
	for _, val := range tensorShape {
		count *= val
	}
	src.inpTensor.Buffer = make([]float32, count)
	for i := 0; i < count; i++ {
		src.inpTensor.Buffer[i] = rand.Float32()
	}

	src.streams = make(map[string]Stream)
	src.deficit = 1.0
	src.srcID = srcID
	src.orcAddr = orcAddr
	src.srcAddr = srcAddr
	src.deficitChan = make(chan float32, 5)

	src.updateDeficit()
}

func (src *Source) updateDeficit() {
	src.deficitChan <- src.calcDeficit()
}

func (src *Source) runDeficitUpdate() {
	orc, err := rpc.Dial("tcp", src.orcAddr)
	if err != nil {
		fmt.Println("Error connecting to orchestrator: ", err)
		return
	}
	defer orc.Close()
	tmp := 0
	for {
		deficit := <-src.deficitChan
		deficitReq := ot.SetDeficitReq{
			SrcID:   src.srcID,
			Deficit: deficit,
		}
		err = orc.Call("Orchestrator.SetSourceDeficit", &deficitReq, &tmp)
		if err != nil {
			fmt.Println("Error setting deficit: ", err)
			return
		}
	}
}

func (src *Source) calcDeficit() float32 {
	deficit := float32(1)
	for _, val := range src.streams {
		deficit -= val.TotalUnits
	}
	return deficit
}

func (src *Source) Run() {
	fmt.Println("Connecting to orc")
	orc, err := rpc.Dial("tcp", src.orcAddr)
	if err != nil {
		panic(err)
	}
	regReq := ot.RegSourceReq{
		SrcID: src.srcID,
		Addr:  src.srcAddr,
	}

	defer orc.Close()

	fmt.Println("Connected to orc")

	tmp := 0

	err = orc.Call("Orchestrator.RegisterSource", &regReq, &tmp)
	if err != nil {
		fmt.Println("Error registering source: ", err)
		return
	}
	fmt.Println("Registered source with orchestrator")

	src.updateDeficit()

	lastTime := time.Now()
	var curTime time.Time

	for {
		fmt.Println("Ready to send data")

		// pick a random client
		deficit := src.calcDeficit()
		choice := rand.Float32() * (1.0 - deficit)
		fmt.Println("Deficit:", deficit)
		fmt.Println("Choice:", choice)
		cur := float32(0.0)

		var stream Stream
		found := false
		var idx string

		for i, val := range src.streams {
			cur += val.TotalUnits
			if cur > choice {
				stream = val
				found = true
				idx = i
				break
			}
		}
		if !found {
			fmt.Println("No connected compute nodes, with length ", len(src.streams))
		} else {
			// send data

			fmt.Println("Found node")

			task := types.ComputeTask{
				ID: types.ComputeID{
					SourceID:  src.srcID,
					Timestamp: time.Now(),
				},
				Data: src.inpTensor,
			}
			fmt.Println("Sending task")
			stream.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err = task.ToWriter(stream.writeBuf)
			if err != nil {
				// src.deficit += stream.TotalUnits
				delete(src.streams, idx)
				src.updateDeficit()
			}
			fmt.Println("Task sent to compute node")
		}
		curTime = time.Now()

		timeDiff := curTime.Sub(lastTime).Milliseconds()
		sleepDuration := time.Duration(1000000 * (500 - timeDiff)) // 2ms*1000
		fmt.Println("Sleeping for", sleepDuration)
		time.Sleep(sleepDuration)

		lastTime = time.Now()
	}
}

func (src *Source) RegisterCommitments(req *map[string]float32, res *float32) error {
	// req: map from compute addr to commitment
	tbd := make([]string, 0)
	for k, _ := range src.streams {
		_, ok := (*req)[k]
		if !ok {
			tbd = append(tbd, k)
		}
	}

	// remove all keys from src.streams which are in tbd
	for _, k := range tbd {
		src.streams[k].conn.Close()
		src.streams[k].writeBuf.Flush()
		delete(src.streams, k)
	}

	for k, v := range src.streams {
		v.TotalUnits = 0
		src.streams[k] = v
	}
	for comp, newComm := range *req {
		oldval, ok := src.streams[comp]
		if ok {
			oldval.TotalUnits = newComm
			src.streams[comp] = oldval
		} else {
			conn, err := net.Dial("tcp", comp)
			if err != nil {
				fmt.Println("Error connecting to compute node @ ", comp, ": ", err)
				return err
			}
			wr := bufio.NewWriter(conn)
			units := newComm
			s := Stream{
				writeBuf:   wr,
				conn:       conn,
				TotalUnits: units,
			}
			src.streams[comp] = s
		}
	}
	return nil
}

func main() {
	fmt.Println("Hello world")
	source := Source{}
	var sourceID string
	sourceID, ok := os.LookupEnv("SOURCE_ID")
	if !ok {
		sourceID = "source"
	}
	sourceAddr, ok := os.LookupEnv("SOURCE_ADDR")
	if !ok {
		panic("SOURCE_ADDR not set")
	}
	orcAddr := os.Getenv("ORC_ADDR")
	source.Init([]int{1, 3, 16, 112, 112}, sourceID, orcAddr, sourceAddr)

	rpc.Register(&source)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", sourceAddr)
	if err != nil {
		panic(err)
	}

	fmt.Println("Going to start")

	go source.runDeficitUpdate()
	go source.Run()

	rpc.Accept(listener)
}
