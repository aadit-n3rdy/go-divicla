package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"

	"github.com/aadit-n3rdy/go-divicla/types"
)

type Server struct{}

func (s *Server) RegisterValue(req *types.ComputeResult, _ *int) error {
	fmt.Println("Received result for ", req.ID.SourceID, " at time", req.ID.Timestamp, " with value ", req.Result.Buffer)
	return nil
}

func main() {
	server := new(Server)
	rpc.Register(server)
	rpc.HandleHTTP()

	serverPort, ok := os.LookupEnv("SERVER_PORT")
	if !ok {
		panic("Missing SERVER_PORT")
	}

	listener, err := net.Listen("tcp", ":"+serverPort)
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on port ", serverPort)

	rpc.Accept(listener)
}
