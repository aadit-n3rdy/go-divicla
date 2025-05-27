package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"

	ot "github.com/aadit-n3rdy/go-divicla/orchestrator/types"
)

type Orchestrator struct {
	Nodes    map[string]ot.OrcNode
	Computes map[string]ot.CompNode // Map address to CPU util
	NodeList []string
	CompList []string
}

func (orc *Orchestrator) Init() {
	orc.Nodes = make(map[string]ot.OrcNode)
	orc.Computes = make(map[string]ot.CompNode)
	orc.NodeList = make([]string, 0)
	orc.CompList = make([]string, 0)
}

func (orc *Orchestrator) RegisterSource(req *ot.RegSourceReq, res *int) error {
	orc.Nodes[req.SrcID] = ot.OrcNode{ID: req.SrcID, Deficit: 0, Addr: req.Addr}
	orc.NodeList = append(orc.NodeList, req.SrcID)
	*res = 0
	fmt.Println("Registered source ", req.SrcID, "@", req.Addr)
	return nil
}

func (orc *Orchestrator) RegisterCompute(req *ot.RegCompReq, res *int) error {
	orc.Computes[req.Addr] = ot.CompNode{Util: 0.0, Capacity: req.Capacity, Commitment: 0.0}
	orc.CompList = append(orc.CompList, req.Addr)
	*res = 0
	return nil
}

func (orc *Orchestrator) SetComputeUtil(req *ot.SetUtilReq, res *int) error {
	cur := orc.Computes[req.ComputeID]
	cur.Util = req.Util
	orc.Computes[req.ComputeID] = cur
	*res = 0
	return nil
}

func (orc *Orchestrator) SetSourceDeficit(req *ot.SetDeficitReq, res *int) error {
	val, ok := orc.Nodes[req.SrcID]
	if !ok {
		return errors.New("unknown source")
	}
	fmt.Println("Set source deficit for ", req.SrcID, " to ", req.Deficit)
	val.Deficit = req.Deficit
	orc.Nodes[req.SrcID] = val
	return nil
}

func (orc *Orchestrator) GetMaximumSourceDeficit(_ *int, res *ot.OrcNode) error {
	if len(orc.Nodes) == 0 {
		fmt.Println("Queried source with no sources registered")
		return errors.New("no sources registered")
	}
	resultID := ""
	var maxDeficit float32 = 0.0
	for ID, val := range orc.Nodes {
		if maxDeficit < val.Deficit {
			resultID = ID
			maxDeficit = val.Deficit
		}
	}
	*res = orc.Nodes[resultID]
	fmt.Println("Sent maximum deficit source: ", res.ID, "@", res.Addr, " with deficit ", res.Deficit)
	return nil
}

func (orc *Orchestrator) runController() {
	for {
		// construct problem
		// solve using GA
		// distribute results
		time.Sleep(2000 * time.Millisecond)

		eaOrc = orc
		best, err := RunEA()

		if err != nil {
			fmt.Println("Error running EA: ", err)
			continue
		}

		for i := 0; i < len(orc.NodeList); i++ {
			nodeID := orc.NodeList[i]
			// Send commitments to each node
			node, ok := orc.Nodes[nodeID]
			if !ok {
				fmt.Println("Node not found: ", nodeID)
				continue
			}
			for _, k := range orc.CompList {
				v := orc.Computes[k]
				v.Commitment = 0
				orc.Computes[k] = v
			}
			commitments := make(map[string]float32, len(orc.CompList))
			for j := 0; j < len(orc.CompList); j++ {
				commitments[orc.CompList[j]] = float32(best[i*len(orc.CompList)+j])
				cobj := orc.Computes[orc.CompList[j]]
				cobj.Commitment = commitments[orc.CompList[j]]
				orc.Computes[orc.CompList[j]] = cobj
			}
			conn, err := rpc.Dial("tcp", node.Addr)
			if err != nil {
				fmt.Println("Error connecting to node ", nodeID, ": ", err)
				continue
			}
			var res float32
			err = conn.Call("Source.SetCommitments", &commitments, &res)
			if err != nil {
				fmt.Println("Error setting commitments for node ", nodeID, ": ", err)
				continue
			}
			conn.Close()
		}
	}
}

func main() {
	orcPort, ok := os.LookupEnv("ORC_PORT")
	if !ok {
		panic("ORC_PORT env var missing")
	}

	orc := Orchestrator{}
	orc.Init()

	go orc.runController()

	rpc.Register(&orc)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+orcPort)
	if err != nil {
		panic(err)
	}
	rpc.Accept(listener)
}
