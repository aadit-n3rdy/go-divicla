package main

import (
	"bufio"
	"fmt"
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

type ComputeThread struct {
	candleConn  net.Conn
	computeChan chan types.ComputeTask
	srvClient   *rpc.Client
}

type Compute struct {
	orcClient       *rpc.Client
	sources         map[string]*SourceDetails
	srvClient       *rpc.Client
	computeAddr     string
	committed       float32
	computeChannels []chan types.ComputeTask
	lastChan        int32
	threads         []ComputeThread

	oldCPU systemstat.CPUSample
}

func (c *ComputeThread) Init(taskChan chan types.ComputeTask, srvAddr string) error {
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

	c.srvClient, err = rpc.Dial("tcp", srvAddr)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return err
	}

	fmt.Println("Connected to candle")
	return nil
}

func (c *Compute) Init(computeAddr string, orcAddr string, candlePort string, srvAddr string, nThreads int32) error {
	var err error
	c.orcClient, err = rpc.Dial("tcp", orcAddr)
	if err != nil {
		return err
	}
	c.sources = make(map[string]*SourceDetails)
	c.computeAddr = computeAddr

	c.srvClient, err = rpc.Dial("tcp", srvAddr)
	if err != nil {
		c.orcClient.Close()
		return err
	}

	fmt.Println("Connected to server")
	c.oldCPU = systemstat.GetCPUSample()

	c.lastChan = 0
	c.computeChannels = make([]chan types.ComputeTask, nThreads)
	c.threads = make([]ComputeThread, nThreads)
	for i := int32(0); i < nThreads; i++ {
		c.computeChannels[i] = make(chan types.ComputeTask, 2)
		c.threads[i] = ComputeThread{}
		c.threads[i].Init(c.computeChannels[i], srvAddr)
		go c.threads[i].RunCompute()
	}

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
		time.Sleep(5 * time.Second)
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
					Units: 1.0,
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
			}
		} else {
			removeList := make([]string, 0)
			for k, v := range c.sources {
				redVal := v.Commitment / 2
				if v.Commitment-redVal < 0.05 {
					redVal = v.Commitment
				}
				tmp := 0
				err := v.Client.Call("Source.ReduceStream", &st.StreamReq{
					Addr:  c.computeAddr,
					Units: redVal,
				}, &tmp)
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
			}
			for _, k := range removeList {
				c.removeSource(k)
			}
		}
	}
}

func (c *ComputeThread) RunCompute() {
	wr := bufio.NewWriter(c.candleConn)
	rd := bufio.NewReader(c.candleConn)

	for {
		task, ok := <-c.computeChan
		if !ok {
			fmt.Println("Job channel closed")
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

func (c *Compute) schedTask(task types.ComputeTask) bool {
	c.lastChan = (c.lastChan + 1) % int32(len(c.computeChannels))
	if len(c.computeChannels[c.lastChan]) == cap(c.computeChannels[c.lastChan]) {
		return false
	}
	c.computeChannels[c.lastChan] <- task
	return true
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
		ok := c.schedTask(task)
		if !ok {
			dropped++
			fmt.Println("Channel is full, dropping task from ", task.ID)
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
		// c.taskChannel <- task
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
	rand.Seed(time.Now().UnixNano())
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
	nThreadsString, ok := os.LookupEnv("N_THREADS")
	var nThreads int32 = 1
	if ok {
		fmt.Sscanf(nThreadsString, "%d", &nThreads)
	}
	var compute Compute
	compute.Init(computeAddr, orcAddr, candlePort, srvAddr, nThreads)

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

	compute.RunController()

	// compute.Run()
}
