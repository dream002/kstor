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

//服务端根据请求返回对应的响应
func (s *server) KstorCommand(ctx context.Context, in *pb.KstorRequest) (*pb.KstorReply, error) {

	switch in.Cmd {
	//创建bucket
	case "createbucket":
		if err := bt.CreateBucket(in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "create bucket fail"}, err
		} else {
			return &pb.KstorReply{Info: "create bucket sucess"}, nil
		}
	//删除bucket
	case "deletebucket":
		if err := bt.DeleteBucket(in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "delete bucket fail"}, err
		} else {
			return &pb.KstorReply{Info: "delete bucket sucess"}, nil
		}
	//添加k/v
	case "setkey":
		if err := bt.SetKeyValue(in.Key, in.Value, in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "set key/value fail"}, err
		} else {
			return &pb.KstorReply{Info: "set key/value sucess"}, nil
		}
	//获得key对应value
	case "getkey":
		if v, err := bt.GetKeyValue(in.Key, in.Bucketname); err != nil || v == "" {
			return &pb.KstorReply{Info: "get the value fail"}, err
		} else {
			return &pb.KstorReply{Info: "get the value " + v}, nil
		}
	//获得以key开头的k/v组
	case "getkeywithprefix":
		if v, err := bt.GetKeyValueWithP(in.Key, in.Bucketname); err != nil || v == "" {
			return &pb.KstorReply{Info: "get the k/v pairs fail"}, err
		} else {
			return &pb.KstorReply{Info: "get the k/v pairs: " + v}, nil
		}
	//删除key
	case "deletekey":
		if err := bt.DeleteKeyValue(in.Key, in.Bucketname); err != nil {
			return &pb.KstorReply{Info: "delete key/value fail"}, err
		} else {
			return &pb.KstorReply{Info: "delete key/value sucess"}, nil
		}
	//备份DB
	case "backupdatabase":
		if err := bt.BackupDatabase(in.Path); err != nil {
			return &pb.KstorReply{Info: "backup database fail"}, err
		} else {
			return &pb.KstorReply{Info: "backup database sucess"}, nil
		}
	//恢复DB
	case "restordatabase":
		if err := bt.RestorDatabase(); err != nil {
			return &pb.KstorReply{Info: "restor database fail"}, err
		} else {
			return &pb.KstorReply{Info: "restor database sucess"}, nil
		}
	default:
		fmt.Println("error")
	}
	return &pb.KstorReply{Info: "get the message"}, nil
}

func main() {

	//监听端口port
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	//创建GRPC服务并注册
	s := grpc.NewServer()
	pb.RegisterKstorServer(s, &server{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
