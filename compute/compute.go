package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"
	"unsafe"

	ot "github.com/aadit-n3rdy/go-divicla/orchestrator/types"
	st "github.com/aadit-n3rdy/go-divicla/source/types"
	"github.com/aadit-n3rdy/go-divicla/types"
)

type SourceDetails struct {
	Addr       string
	Client     *rpc.Client
	Commitment float32
}

type Compute struct {
	orcClient   *rpc.Client
	sources     map[string]SourceDetails
	candleConn  net.Conn
	srvClient   *rpc.Client
	taskChannel chan types.ComputeTask
	computeAddr string
	committed   float32
}

func (c *Compute) Init(computeAddr string, orcAddr string, candlePort string, srvAddr string) error {
	var err error
	c.orcClient, err = rpc.Dial("tcp", orcAddr)
	if err != nil {
		return err
	}
	c.sources = make(map[string]SourceDetails)
	c.computeAddr = computeAddr
	c.taskChannel = make(chan types.ComputeTask, 10)

	for {
		c.candleConn, err = net.Dial("tcp", "127.0.0.1:"+candlePort)
		if err == nil {
			break
		}
		fmt.Println("Error connecting to candle:", err)
		time.Sleep(5 * time.Second)
	}

	fmt.Println("Connected to candle")

	c.srvClient, err = rpc.Dial("tcp", srvAddr)
	if err != nil {
		c.orcClient.Close()
		c.candleConn.Close()
		return err
	}

	fmt.Println("Connected to server")

	return nil
}

func tensorToTCP(te *types.Tensor, conn net.Conn) {
	sizeStr := []byte("")
	for _, v := range te.Sizes {
		sizeStr = fmt.Appendf(sizeStr, "%v ", v)
	}
	sizeStr = append(sizeStr, '\n')
	conn.Write(sizeStr)
}

func tensorToWriter(te *types.Tensor, wr *bufio.Writer) {
	for _, val := range te.Sizes {
		wr.WriteString(fmt.Sprint(val) + " ")
	}
	wr.WriteString("\n")
	bytePointer := unsafe.Pointer(&te.Buffer[0])
	byteBuffer := unsafe.Slice((*byte)(bytePointer), len(te.Buffer)*4)
	_, err := wr.Write(byteBuffer)
	if err != nil {
		fmt.Println("Error writing tensor buffer to writer: ", err)
	}
	wr.Flush()
}

func (c *Compute) RunController() {
	c.committed = 0.0
	for {
		if c.committed < 1.0 {
			var tmp int
			var node ot.OrcNode
			err := c.orcClient.Call("Orchestrator.GetMaximumSourceDeficit", &tmp, &node)
			if err != nil {
				fmt.Println("Error fetching source: ", err)
				return
			}
			fmt.Println("Fetched source: ", node.ID, "@", node.Addr, " with deficit ", node.Deficit)
			// register node with source
			if node.Deficit > 0 {
				sourceClient, err := rpc.Dial("tcp", node.Addr)
				if err != nil {
					fmt.Println("Could not connect to ", node.ID, ":", err)
					c.orcClient.Close()
				}
				fmt.Println("Connected to source ", node.ID)

				accepted := float32(0)
				err = sourceClient.Call("Source.RegisterStream", &st.StreamReq{
					Addr:  c.computeAddr,
					Units: 1.0,
				}, &accepted)
				if err != nil {
					fmt.Println("Error registering stream: ", err)
				} else if accepted > 0 {
					sd, ok := c.sources[node.ID]
					if ok {
						// already exists
						sourceClient.Close()
						sd.Commitment += accepted
						c.sources[node.ID] = sd
						fmt.Println("Increased commitment with source")
					} else {
						c.sources[node.ID] = SourceDetails{Addr: node.Addr, Client: sourceClient, Commitment: accepted}
						c.committed += accepted
						fmt.Println("Registered with source")
					}
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *Compute) Run() {

	wr := bufio.NewWriter(c.candleConn)
	rd := bufio.NewReader(c.candleConn)

	for {
		task, ok := <-c.taskChannel
		if !ok {
			fmt.Println("Job channel closed")
			break
		}
		fmt.Println("Send task ", task.ID, " to candle")
		tensorToWriter(&task.Data, wr)

		fmt.Println("Getting result for ", task.ID, " from candle")

		resultLine, err := rd.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading result from candle: ", err)
		}
		var val1, val2 float32
		fmt.Sscanf(resultLine, "%f %f\n", &val1, &val2)

		// TODO: work from here
		// val1 := math.Float32frombits(binary.LittleEndian.Uint32(resultBuf[:4]))
		// val2 := math.Float32frombits(binary.LittleEndian.Uint32(resultBuf[4:]))
		result := types.ComputeResult{
			ID:     task.ID,
			Result: types.Tensor{Sizes: []int{1, 2}, Buffer: []float32{val1, val2}},
		}
		tmp := 0
		fmt.Println("Got result for ", task.ID, " from candle")
		c.srvClient.Call("Server.RegisterValue", &result, &tmp)
		fmt.Println("Sent result for ", task.ID, " to server")
	}
	fmt.Println("Exiting compute runner")
}

func (c *Compute) removeSource(source string) {
	v := c.sources[source]
	c.committed -= v.Commitment
	v.Client.Close()
	delete(c.sources, source)
}

func (c *Compute) ConnHandler(conn net.Conn) {
	defer conn.Close()
	rdr := bufio.NewReader(conn)
	var dropped = 0
	srcID := ""
	for {
		task := types.ComputeTask{}
		err := task.FromReader(rdr)
		if err != nil {
			fmt.Println("Error recving task: ", err)
			if srcID != "" {
				c.removeSource(srcID)
			}
			return
		}
		srcID = task.ID.SourceID
		fmt.Println("Got a task!")
		if len(c.taskChannel) == cap(c.taskChannel) {
			fmt.Println("Channel is full, dropping task from ", task.ID)
			dropped += 1
			if dropped >= 5 {
				fmt.Println("Too many dropped, breaking")
				if srcID != "" {
					c.removeSource(srcID)
				}
				break
			}
			continue
		}
		dropped = 0
		c.taskChannel <- task
		fmt.Println("Pushed to channel")
	}
}

func (c *Compute) Listener(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
			break
		}
		fmt.Println("New connection from ", conn.RemoteAddr())
		go c.ConnHandler(conn)
	}
}

func main() {
	orcAddr, ok := os.LookupEnv("ORC_ADDR")
	if !ok {
		panic("ORC_ADDR env var missing")
	}
	port, ok := os.LookupEnv("COMPUTE_PORT")
	if !ok {
		panic("COMPUTE_PORT env var missing")
	}
	candlePort, ok := os.LookupEnv("CANDLE_PORT")
	if !ok {
		panic("CANDLE_PORT env var missing")
	}
	srvAddr, ok := os.LookupEnv("SERVER_ADDR")
	if !ok {
		panic("SERVER_ADDR env var missing")
	}
	computeAddr, ok := os.LookupEnv("COMPUTE_ADDR")
	if !ok {
		panic("COMPUTE_ADDR env var missing")
	}
	var compute Compute
	compute.Init(computeAddr, orcAddr, candlePort, srvAddr)

	//rpcSrv := rpc.NewServer()
	//rpc.Register(&compute)
	//rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	go compute.Listener(listener)
	//go func() {
	//	fmt.Println("Serving http")
	//	http.Serve(listener, nil)
	//	fmt.Println("Serve exited for some reason")
	//}()

	go compute.RunController()

	compute.Run()
}
