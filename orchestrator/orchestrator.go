package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"

	ot "github.com/aadit-n3rdy/go-divicla/orchestrator/types"
)

type Orchestrator struct {
	Nodes map[string]ot.OrcNode
}

func (orc *Orchestrator) Init() {
	orc.Nodes = make(map[string]ot.OrcNode)
}

func (orc *Orchestrator) RegisterSource(req *ot.RegSourceReq, res *int) error {
	orc.Nodes[req.SrcID] = ot.OrcNode{ID: req.SrcID, Deficit: 0, Addr: req.Addr}
	fmt.Println("Registered source ", req.SrcID, "@", req.Addr)
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

func main() {
	orcPort, ok := os.LookupEnv("ORC_PORT")
	if !ok {
		panic("ORC_PORT env var missing")
	}

	orc := Orchestrator{}
	orc.Init()

	rpc.Register(&orc)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+orcPort)
	if err != nil {
		panic(err)
	}
	rpc.Accept(listener)
}
