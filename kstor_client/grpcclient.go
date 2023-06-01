package client

import (
	//"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/dream002/kstor/kstor_pb"

	"golang.org/x/net/context"
)

var backuppath string = "/home/zhangjiahua/codes/src/kstor/kstor_backup/my.db"
var dbname string = "my.db"

func BuckupDB1(c pb.KstorClient, databasepath string) {

	r, err := buckupdb(c, databasepath)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func buckupdb(c pb.KstorClient, databasepath string) (*pb.KstorReply, error) {

	//设置连接超时
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	//使用grpc向服务端发送请求并获得响应r
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "backupdatabase", Path: databasepath})
	return r, err
}

func RestorDB1(c pb.KstorClient) {

	r, err := restordb(c)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func restordb(c pb.KstorClient) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "restordatabase"})
	return r, err
}

func DeleteBucket(c pb.KstorClient, bucketname string) {

	r, err := deletebucket(c, bucketname)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func deletebucket(c pb.KstorClient, bucketname string) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "deletebucket", Bucketname: bucketname})
	return r, err
}

func SetKV(c pb.KstorClient, thekey string, thevalue string, bucketname string) {

	r, err := setkv(c, thekey, thevalue, bucketname)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func setkv(c pb.KstorClient, thekey string, thevalue string, bucketname string) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "setkey", Bucketname: bucketname, Key: thekey, Value: thevalue})
	return r, err
}

func GetKV(c pb.KstorClient, thekey string, bucketname string) {

	r, err := getkv(c, thekey, bucketname)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func getkv(c pb.KstorClient, thekey string, bucketname string) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "getkey", Bucketname: bucketname, Key: thekey})
	return r, err

}

func GetKVwithP(c pb.KstorClient, thekey string, bucketname string) {

	r, err := getkvwithp(c, thekey, bucketname)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func getkvwithp(c pb.KstorClient, thekey string, bucketname string) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "getkeywithprefix", Bucketname: bucketname, Key: thekey})
	return r, err

}

func DeleteKV(c pb.KstorClient, thekey string, bucketname string) {

	r, err := deletekv(c, thekey, bucketname)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func deletekv(c pb.KstorClient, thekey string, bucketname string) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "deletekey", Bucketname: bucketname, Key: thekey})
	return r, err

}

func CreateBucket(c pb.KstorClient, name string) {

	r, err := createbucket(c, name)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("StatusCode: %d, Info: %s", r.Status.Code, r.Info)
}

func createbucket(c pb.KstorClient, name string) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "createbucket", Bucketname: name})
	return r, err

}

func RestorDB(c pb.KstorClient) {

	reply, err := putfile(c)
	if err != nil {
		log.Printf("StatusCode: %d, Info: %s", reply.Status.Code, reply.Info)
	} else {
		log.Printf("StatusCode: %d, Info: %s", reply.Status.Code, reply.Info)
	}

}

func putfile(client pb.KstorClient) (pb.RestorReply, error) {

	buffer := make([]byte, 1024*1024)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.KstorRestor(ctx)
	if err != nil {
		return pb.RestorReply{}, err
	}

	f, err := os.Open(backuppath)
	defer f.Close()
	if err != nil {
		return pb.RestorReply{}, err
	}

	for {
		n, err := f.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			return pb.RestorReply{}, err
		}

		req := &pb.RestorRequest{RestorFile: buffer[:n]}

		err = stream.Send(req)
		if err != nil {
			return pb.RestorReply{}, err
		}

		if n < 1024*1024 {
			break
		}
	}
	reply, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		log.Fatalf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
	//log.Printf("StatusCode: %d, Info: %s", reply.Status.Code, reply.Info)
	return *reply, err
}

func BuckupDB(c pb.KstorClient, databasepath string) {

	_, err := getfile(c, databasepath)
	if err != nil {
		//log.Printf("StatusCode: %d, Info: %s", resp.Status.Code, reply.Info)
		log.Printf("StatusCode: 1300, Info: backup fail: %v", err)
	} else {
		//log.Printf("StatusCode: %d, Info: backup sucess", resp.Status.Code)
		log.Printf("StatusCode: 1301, Info: backup sucess")
	}

}

func getfile(client pb.KstorClient, path string) (*pb.BackupReply, error) {

	backuppath = path + dbname

	var size int32 = 1024 * 1024

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.KstorBackup(ctx, &pb.BackupRequest{Size: size})
	if err != nil {
		return &pb.BackupReply{}, err
	}

	f, err := os.Create(backuppath)
	defer f.Close()
	if err != nil {
		return &pb.BackupReply{}, err
	}

	for {

		resp, err := stream.Recv()
		if err != nil && err != io.EOF {
			return &pb.BackupReply{}, err
		}

		if err == io.EOF {
			break
		}

		_, err = f.Write(resp.BackupFile)

		if len(resp.BackupFile) < int(size) {
			return resp, nil
		}

		if err != nil {
			return &pb.BackupReply{}, err
		}

	}
	return &pb.BackupReply{}, nil
}
