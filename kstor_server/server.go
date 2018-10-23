package main

import (
	"fmt"
	"log"
	"net"

	pb "kstor/kstor"
	bt "kstor/kstor_db"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

// SayHello implements helloworld.GreeterServer
func (s *server) KstorCommand(ctx context.Context, in *pb.KstorRequest) (*pb.KstorReply, error) {
	switch in.Cmd {
	case "createbucket":
		if err := bt.CreateBucket(in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "create bucket fail"}, err
		} else {
			return &pb.KstorReply{Info: "create bucket sucess"}, nil
		}
	case "deletebucket":
		if err := bt.DeleteBucket(in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "delete bucket fail"}, err
		} else {
			return &pb.KstorReply{Info: "delete bucket sucess"}, nil
		}
	case "setkey":
		if err := bt.SetKeyValue(in.Key, in.Value, in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "set key/value fail"}, err
		} else {
			return &pb.KstorReply{Info: "set key/value sucess"}, nil
		}
	case "getkey":
		if v, err := bt.GetKeyValue(in.Key, in.Bucketname); err != nil || v == "" {
			return &pb.KstorReply{Info: "get the value fail"}, err
		} else {
			return &pb.KstorReply{Info: "get the value " + v}, nil
		}
	case "getkeywithprefix":
		if v, err := bt.GetKeyValueWithP(in.Key, in.Bucketname); err != nil || v == "" {
			return &pb.KstorReply{Info: "get the k/v pairs fail"}, err
		} else {
			return &pb.KstorReply{Info: "get the k/v pairs: " + v}, nil
		}
	case "deletekey":
		if err := bt.DeleteKeyValue(in.Key, in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "delete key/value fail"}, err
		} else {
			return &pb.KstorReply{Info: "delete key/value sucess"}, nil
		}
	case "buckupdatabase":
		fmt.Println("buckupdatabase")
	case "restordatabase":
		fmt.Println("restordatabase")
	default:
		fmt.Println("error")
	}
	return &pb.KstorReply{Info: "get the message"}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKstorServer(s, &server{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
