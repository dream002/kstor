package client

import (
	"log"
	"time"

	pb "kstor/kstor"

	"golang.org/x/net/context"
)

func BuckupDB(c pb.KstorClient, databasepath string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "buckupdatabase", Path: databasepath})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func RestorDB(c pb.KstorClient, databasepath string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "restordatabase", Path: databasepath})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func DeleteBucket(c pb.KstorClient, bucketname string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "deletebucket", Bucketname: bucketname})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func SetKV(c pb.KstorClient, thekey string, thevalue string, bucketname string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "setkey", Bucketname: bucketname, Key: thekey, Value: thevalue})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func GetKV(c pb.KstorClient, thekey string, bucketname string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "getkey", Bucketname: bucketname, Key: thekey})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func GetKVwithP(c pb.KstorClient, thekey string, bucketname string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "getkeywithprefix", Bucketname: bucketname, Key: thekey})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func DeleteKV(c pb.KstorClient, thekey string, bucketname string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "deletekey", Bucketname: bucketname, Key: thekey})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}

func CreateBucket(c pb.KstorClient, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.KstorCommand(ctx, &pb.KstorRequest{Cmd: "createbucket", Bucketname: name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Info)
}
