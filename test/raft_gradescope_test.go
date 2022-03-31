package SurfTest

import (
	"fmt"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestRaftNewLeaderPushesUpdates(t *testing.T) {
	t.Logf("leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	t.Logf("okkk")
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	t.Logf("okkk")
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	t.Logf("okkk")
	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	defer worker1.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	// test.Clients[3].Crash(test.Context, &emptypb.Empty{})

	//client1 syncs
	t.Logf("client sync")
	SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// err = worker1.UpdateFile(file1, "update text")
	// if err != nil {
	// 	t.FailNow()
	// }
	// SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	// test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// stat0, _ := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	// fmt.Println(stat0)

	// test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	// test.Clients[3].Restore(test.Context, &emptypb.Empty{})
	// test.Clients[4].SetLeader(test.Context, &emptypb.Empty{})
	// test.Clients[4].SendHeartbeat(test.Context, &emptypb.Empty{})

	// err = worker1.UpdateFile(file1, "update text")
	// if err != nil {
	// 	t.FailNow()
	// }
	// //client1 syncs
	// err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	// if err != nil {
	// 	t.Fatalf("Sync failed")
	// }
	// test.Clients[4].SendHeartbeat(test.Context, &emptypb.Empty{})

	// test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	stat0, _ := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	stat1, _ := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
	stat2, _ := test.Clients[2].GetInternalState(test.Context, &emptypb.Empty{})
	// stat3, _ := test.Clients[3].GetInternalState(test.Context, &emptypb.Empty{})
	// stat4, _ := test.Clients[4].GetInternalState(test.Context, &emptypb.Empty{})

	fmt.Println(0, stat0)
	fmt.Println(1, stat1)
	fmt.Println(2, stat2)
	// fmt.Println(3, stat3)
	// fmt.Println(4, stat4)
}
