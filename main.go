// kstor project main.go
package main

import (
	"log"

	pb "github.com/dream002/kstor/kstor_pb"
	cmd "github.com/dream002/kstor/kstorcmd"

	"google.golang.org/grpc"
)

const (
	address = "localhost:50051"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKstorClient(conn)

	cmd.Command(c)

}
