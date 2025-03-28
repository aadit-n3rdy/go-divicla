package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"time"
	"unsafe"

	"bitbucket.org/bertimus9/systemstat"

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
	sources     map[string]*SourceDetails
	candleConn  net.Conn
	srvClient   *rpc.Client
	taskChannel chan types.ComputeTask
	computeAddr string
	committed   float32

	oldCPU systemstat.CPUSample
}

func (c *Compute) Init(computeAddr string, orcAddr string, candlePort string, srvAddr string) error {
	var err error
	c.orcClient, err = rpc.Dial("tcp", orcAddr)
	if err != nil {
		return err
	}
	c.sources = make(map[string]*SourceDetails)
	c.computeAddr = computeAddr
	c.taskChannel = make(chan types.ComputeTask, 10)

	sockpath := "/tmp/divicla_compute_" + fmt.Sprint(rand.Int()%500)
	listener, err := net.Listen("unix", sockpath)
	if err != nil {
		fmt.Println("Error creating socket ", sockpath, ":", err)
		return err
	}

	cmd := exec.Command("python3", "./py/processor.py", sockpath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		fmt.Println("Error running process:", err)
		return err
	}

	c.candleConn, err = listener.Accept()
	if err != nil {
		fmt.Println("Error accepting conn:", err)
		return err
	}

	fmt.Println("Connected to candle")

	c.srvClient, err = rpc.Dial("tcp", srvAddr)
	if err != nil {
		c.orcClient.Close()
		c.candleConn.Close()
		return err
	}

	fmt.Println("Connected to server")
	c.oldCPU = systemstat.GetCPUSample()

	return nil
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

func (c *Compute) getLoad() float32 {
	curCPU := systemstat.GetCPUSample()
	usage := systemstat.GetCPUAverage(c.oldCPU, curCPU)
	c.oldCPU = curCPU
	return 1.0 - (float32(usage.IdlePct)+float32(usage.IowaitPct))/100
}

func (c *Compute) RunController() {
	c.committed = 0.0
	for {
		time.Sleep(2 * time.Second)
		usage := c.getLoad()
		fmt.Println("CURRENT LOAD: ", usage)
		if usage < 0.9 {
			var tmp int
			var node ot.OrcNode
			err := c.orcClient.Call("Orchestrator.GetMaximumSourceDeficit", &tmp, &node)
			if err != nil {
				fmt.Println("Error fetching source: ", err)
				continue
			}
			// fmt.Println("Fetched source: ", node.ID, "@", node.Addr, " with deficit ", node.Deficit)
			// register node with source
			if node.Deficit > 0 {
				sourceClient, err := rpc.Dial("tcp", node.Addr)
				if err != nil {
					fmt.Println("Could not connect to ", node.ID, ":", err)
					continue
				}

				accepted := float32(0)
				err = sourceClient.Call("Source.RegisterStream", &st.StreamReq{
					Addr:  c.computeAddr,
					Units: 0.1,
				}, &accepted)
				if err != nil {
					fmt.Println("Error registering stream: ", err)
				} else if accepted > 0 {
					sd, ok := c.sources[node.ID]
					if ok {
						// already exists
						// sourceClient.Close()
						sd.Commitment += accepted
						c.committed += accepted
						fmt.Println("Increased commitment with source")
					} else {
						c.sources[node.ID] = &SourceDetails{Addr: node.Addr, Client: sourceClient, Commitment: accepted}
						c.committed += accepted
						fmt.Println("Registered with source")
					}
				}
				log.Println("Accepted, new commitment:", c.committed)
			}
		} else {
			log.Println("CPU usage too high:", usage, ", reducing commitment")
			removeList := make([]string, 0)
			for k, v := range c.sources {
				redVal := v.Commitment / 2
				if v.Commitment-redVal < 0.05 {
					redVal = v.Commitment
				}
				err := v.Client.Call("Source.ReduceStream", &st.StreamReq{
					Addr:  c.computeAddr,
					Units: redVal,
				}, &redVal)
				if err != nil {
					fmt.Println("Error reducing stream: ", err)
					removeList = append(removeList, k)
				}
				if redVal < v.Commitment {
					// reduce commitment only if it won't be handled by removeSource
					v.Commitment -= redVal
				} else {
					removeList = append(removeList, k)
				}
				fmt.Println("Reducing stream ", k, " to ", v.Commitment-redVal)
			}
			for _, k := range removeList {
				c.removeSource(k)
			}
			log.Println("Reduced, new commitment:", c.committed)
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
			log.Println("Job channel closed")
			break
		}
		tensorToWriter(&task.Data, wr)

		resultLine, err := rd.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading result from candle: ", err)
		}
		var val1, val2 float32
		fmt.Sscanf(resultLine, "%f %f\n", &val1, &val2)

		result := types.ComputeResult{
			ID:     task.ID,
			Result: types.Tensor{Sizes: []int{1, 2}, Buffer: []float32{val1, val2}},
		}
		tmp := 0
		fmt.Println("Calculated for", task.ID)
		c.srvClient.Call("Server.RegisterValue", &result, &tmp)
	}
	fmt.Println("Exiting compute runner")
}

func (c *Compute) removeSource(source string) {
	v, ok := c.sources[source]
	if !ok {
		return
	}
	c.committed -= v.Commitment
	v.Client.Close()
	delete(c.sources, source)
}

func (c *Compute) ConnHandler(conn net.Conn) {
	defer conn.Close()
	rdr := bufio.NewReader(conn)
	dropped := 0
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
		log.Print("New connection from ", conn.RemoteAddr())
		go c.ConnHandler(conn)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	logFile, err := os.Create("compute.log")
	if err != nil {
		fmt.Println("Error creating logfile")
		return
	}
	defer logFile.Close()
	log.SetOutput(logFile)
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

	// rpcSrv := rpc.NewServer()
	// rpc.Register(&compute)
	// rpc.HandleHTTP()

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
