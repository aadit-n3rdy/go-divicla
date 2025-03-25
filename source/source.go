package main

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"time"

	ot "github.com/aadit-n3rdy/go-divicla/orchestrator/types"
	st "github.com/aadit-n3rdy/go-divicla/source/types"
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
	Client     *rpc.Client
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
}

func (src *Source) updateDeficit() {
	src.deficitChan <- src.deficit
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
		<-src.deficitChan
		deficitReq := ot.SetDeficitReq{
			SrcID:   src.srcID,
			Deficit: src.deficit,
		}
		err = orc.Call("Orchestrator.SetSourceDeficit", &deficitReq, &tmp)
		if err != nil {
			fmt.Println("Error setting deficit: ", err)
			return
		}
	}
}

func (src *Source) Run() {
	orc, err := rpc.Dial("tcp", src.orcAddr)
	if err != nil {
		panic(err)
	}
	regReq := ot.RegSourceReq{
		SrcID: src.srcID,
		Addr:  src.srcAddr,
	}

	defer orc.Close()

	tmp := 0

	err = orc.Call("Orchestrator.RegisterSource", &regReq, &tmp)
	if err != nil {
		fmt.Println("Error registering source: ", err)
		return
	}
	fmt.Println("Registered source with orchestrator")

	src.deficit = 1.0
	src.updateDeficit()

	lastTime := time.Now()
	var curTime time.Time

	for {
		fmt.Println("Ready to send data")

		// pick a random client
		choice := rand.Float32() * (1.0 - src.deficit)
		fmt.Println("Deficit:", src.deficit)
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
			fmt.Println("No connected compute nodes")
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
				src.deficit += stream.TotalUnits
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

func (src *Source) RegisterStream(req *st.StreamReq, res *float32) error {
	val, ok := src.streams[req.Addr]
	fmt.Println("Registering stream for ", req.Addr)
	if !ok {
		//client, err := rpc.Dial("tcp", req.Addr)
		//if err != nil {
		//	fmt.Println("Error connecting to compute node @ ", req.Addr, ": ", err)
		//	return err
		//}
		conn, err := net.Dial("tcp", req.Addr)
		if err != nil {
			fmt.Println("Error connecting to compute node @ ", req.Addr, ": ", err)
			return err
		}
		wr := bufio.NewWriter(conn)
		accepted := min(req.Units, src.deficit)
		accepted = min(accepted, 0.6-val.TotalUnits)
		if accepted > 0 {
			src.deficit -= accepted
			s := Stream{
				// Client:     client,
				writeBuf:   wr,
				conn:       conn,
				TotalUnits: accepted,
			}
			src.streams[req.Addr] = s
			src.updateDeficit()
			*res = accepted
			fmt.Println("Accepted ", accepted, " units")
		}
		return nil
	}

	accepted := min(req.Units, src.deficit)
	accepted = min(accepted, 0.6-val.TotalUnits)
	if accepted <= 0 {
		fmt.Println("Denied by 0.6 rule")
		*res = 0
		return nil
	}
	val.TotalUnits += accepted
	src.deficit -= accepted
	src.updateDeficit()
	val.TotalUnits = accepted
	src.streams[req.Addr] = val
	*res = accepted
	fmt.Println("Increased to ", val.TotalUnits, " units")
	return nil
}

func (src *Source) ReduceStream(req *st.StreamReq, res *float32) error {
	val, ok := src.streams[req.Addr]
	if !ok {
		return errors.New("unknown compute node " + req.Addr)
	}
	if val.TotalUnits <= req.Units {
		// remove stream
		delete(src.streams, req.Addr)
		val.Client.Close()
		src.deficit -= val.TotalUnits
		*res = val.TotalUnits
		return nil
	}

	reduced := min(val.TotalUnits, req.Units)
	val.TotalUnits -= reduced
	src.deficit += reduced
	src.streams[req.Addr] = val
	*res = reduced

	return nil
}

func main() {
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

	go source.runDeficitUpdate()
	go source.Run()

	rpc.Accept(listener)
}
