package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	bt "kstor/kstor_db"
	pb "kstor/kstor_pb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

var testpath string = "/home/zhangjiahua/codes/src/kstor/kstor_db/mybackup.db"
var defaultpath string = "/home/zhangjiahua/codes/src/kstor/kstor_db/my.db"

//var testpath string = "../kstor_db/my.db"

var (
	createbucketfail = &pb.Status{
		Code: 1110,
	}
	createbucketsucess = &pb.Status{
		Code: 1111,
	}
	deletebucketfail = &pb.Status{
		Code: 1120,
	}
	deletebucketsucess = &pb.Status{
		Code: 1121,
	}
	setkeyvaluefail = &pb.Status{
		Code: 1210,
	}
	setkeyvaluesucess = &pb.Status{
		Code: 1211,
	}
	getkeyvaluefail = &pb.Status{
		Code: 1220,
	}
	getkeyvaluesucess = &pb.Status{
		Code: 1221,
	}
	getkeyvaluesfail = &pb.Status{
		Code: 1230,
	}
	getkeyvaluessucess = &pb.Status{
		Code: 1231,
	}
	deletekeyvaluefail = &pb.Status{
		Code: 1240,
	}
	deletekeyvaluesucess = &pb.Status{
		Code: 1241,
	}
	backupdatabasefail = &pb.Status{
		Code: 1300,
	}
	backupdatabasesucess = &pb.Status{
		Code: 1301,
	}
	restordatabasefail = &pb.Status{
		Code: 1400,
	}
	restordatabasesucess = &pb.Status{
		Code: 1401,
	}
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

//服务端根据请求返回对应的响应
func (s *server) KstorCommand(ctx context.Context, in *pb.KstorRequest) (*pb.KstorReply, error) {

	switch in.Cmd {
	//创建bucket
	case "createbucket":
		if err := bt.CreateBucket(in.Bucketname); err != nil {
			return &pb.KstorReply{Status: createbucketfail, Info: "create bucket fail"}, err
		} else {
			return &pb.KstorReply{Status: createbucketsucess, Info: "create bucket sucess"}, nil
		}
	//删除bucket
	case "deletebucket":
		if err := bt.DeleteBucket(in.Bucketname); err != nil {
			return &pb.KstorReply{Status: deletebucketfail, Info: "delete bucket fail"}, err
		} else {
			return &pb.KstorReply{Status: deletebucketsucess, Info: "delete bucket sucess"}, nil
		}
	//添加k/v
	case "setkey":
		if err := bt.SetKeyValue(in.Key, in.Value, in.Bucketname); err != nil {
			return &pb.KstorReply{Status: setkeyvaluefail, Info: "set key/value fail"}, err
		} else {
			return &pb.KstorReply{Status: setkeyvaluesucess, Info: "set key/value sucess"}, nil
		}
	//获得key对应value
	case "getkey":
		if v, err := bt.GetKeyValue(in.Key, in.Bucketname); err != nil || v == "" {
			return &pb.KstorReply{Status: getkeyvaluefail, Info: "get the value fail"}, err
		} else {
			return &pb.KstorReply{Status: getkeyvaluesucess, Info: "get the value " + v}, nil
		}
	//获得以key开头的k/v组
	case "getkeywithprefix":
		if v, err := bt.GetKeyValueWithP(in.Key, in.Bucketname); err != nil || v == "" {
			return &pb.KstorReply{Status: getkeyvaluesfail, Info: "get the k/v pairs fail"}, err
		} else {
			return &pb.KstorReply{Status: getkeyvaluessucess, Info: "get the k/v pairs: " + v}, nil
		}
	//删除key
	case "deletekey":
		if err := bt.DeleteKeyValue(in.Key, in.Bucketname); err != nil {
			return &pb.KstorReply{Status: deletekeyvaluefail, Info: "delete key/value fail"}, err
		} else {
			return &pb.KstorReply{Status: deletekeyvaluesucess, Info: "delete key/value sucess"}, nil
		}
	//备份DB
	case "backupdatabase":

		if err := bt.BackupDatabase(in.Path); err != nil {
			return &pb.KstorReply{Status: backupdatabasefail, Info: "backup database fail"}, err
		} else {
			return &pb.KstorReply{Status: backupdatabasesucess, Info: "backup database sucess"}, nil
		}
	//恢复DB
	case "restordatabase":
		if err := bt.RestorDatabase(); err != nil {
			return &pb.KstorReply{Status: restordatabasefail, Info: "restor database fail"}, err
		} else {
			return &pb.KstorReply{Status: restordatabasesucess, Info: "restor database sucess"}, nil
		}
	default:
		fmt.Println("error")
	}
	return &pb.KstorReply{Info: "get the message"}, nil
}

func (s *server) KstorBackup(req *pb.BackupRequest, stream pb.Kstor_KstorBackupServer) error {

	//copy source file
	bt.BackupDatabase(testpath)

	size := req.Size
	buffer := make([]byte, size)

	f, err := os.Open(testpath)
	defer f.Close()
	if err != nil {
		log.Fatalf("open file fail: %v", err)
		return err
	}

	for {

		n, err := f.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		resp := &pb.BackupReply{BackupFile: buffer[:n]}

		err = stream.Send(resp)
		if err != nil {
			log.Fatal("send buffer error")
			return err
		}

		if n < int(size) {
			break
		}
	}

	//move source file
	os.Remove(testpath)
	return nil
}

func (s *server) KstorRestor(stream pb.Kstor_KstorRestorServer) error {

	f, err := os.Create(defaultpath)
	defer f.Close()
	if err != nil {
		log.Fatal("create file fail")
		return err
	}

	for {

		rsq, err := stream.Recv()
		if err == io.EOF {
			//stream.SendAndClose(&pb.RestorReply{Status: restordatabasesucess, Info: "restor database sucess"})
			break
		} else if err != nil {
			return err
		}

		_, err = f.Write(rsq.RestorFile)
		if err != nil {
			return err
		}

		if len(rsq.RestorFile) < 1024*1024 {
			stream.SendAndClose(&pb.RestorReply{Status: restordatabasesucess, Info: "restor database sucess"})
			break
		}

	}

	return nil
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
