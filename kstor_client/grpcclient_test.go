package client

import (
	"log"
	"testing"

	pb "kstor/kstor_pb"
	//cmd "kstor/kstorcmd"
	"strconv"

	"google.golang.org/grpc"
)

/*func Test_buckupdb(t *testing.T) {

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKstorClient(conn)

	for i := 1; i < 5; i++ {
		s := "C:/mycode/" + strconv.Itoa(i) + "/"
		if r, err := buckupdb(c, s); r.Info == "backup database sucess" && err == nil {
			t.Log("pass")
		} else {
			t.Error("error")
		}
	}

}*/

func Test_setkv(t *testing.T) {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKstorClient(conn)

	for i := 1; i < 1000; i++ {
		k := "test" + strconv.Itoa(i)
		v := strconv.Itoa(i + 20)
		if r, err := setkv(c, k, v, "mybucket"); r.Info == "set key/value sucess" && err == nil {
			t.Log("pass")
		} else {
			t.Error("error")
		}
	}

}

/*func Test_deletekv(t *testing.T) {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKstorClient(conn)

	for i := 1; i < 1000; i++ {
		k := "test" + strconv.Itoa(i)
		//v := strconv.Itoa(i + 20)
		if r, err := deletekv(c, k, "mybucket"); r.Info == "delete key/value sucess" && err == nil {
			t.Log("pass")
		} else {
			t.Error("error")
		}
	}
}*/
