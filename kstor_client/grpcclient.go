package client

import (
	"log"
	"time"

	pb "kstor/kstor"

	"golang.org/x/net/context"
)

func BuckupDB(c pb.KstorClient, databasepath string) {

	r, err := buckupdb(c, databasepath)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func buckupdb(c pb.KstorClient, databasepath string) (*pb.KstorReply, error) {

	//设置连接超时
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	//使用grpc向服务端发送请求并获得响应r
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "backupdatabase", Path: databasepath})
	return r, err
}

func RestorDB(c pb.KstorClient) {

	r, err := restordb(c)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
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
	log.Printf("Greeting: %s", r.Info)
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
	log.Printf("Greeting: %s", r.Info)
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
	log.Printf("Greeting: %s", r.Info)
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
	log.Printf("Greeting: %s", r.Info)
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
	log.Printf("Greeting: %s", r.Info)
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
	log.Printf("Greeting: %s", r.Info)
}

func createbucket(c pb.KstorClient, name string) (*pb.KstorReply, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "createbucket", Bucketname: name})
	return r, err

}
