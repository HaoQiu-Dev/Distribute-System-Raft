package surfstore

import (
	context "context"
	"fmt"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn) //block

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn) //block

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	successInfor, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = successInfor.Flag
	// if *succ {
	// 	log.Print("successfully PutBlock")
	// } else {
	// 	log.Print("fail to PutBlock")
	// }
	// close the connection
	return conn.Close()

}

//
func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn) //block

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashesInes := &BlockHashes{Hashes: blockHashesIn}
	existBlocks, err := c.HasBlocks(ctx, blockHashesInes)
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = existBlocks.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) checkLeader() (*string, error) {

	var leader *string
	for _, ip := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(ip, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		internalState, err := c.GetInternalState(ctx, &emptypb.Empty{}) //******* useful

		if err != nil {
			conn.Close()
			continue
		}
		if internalState.IsLeader {
			leader = &ip
			conn.Close()
			return leader, nil
		}
		// close the connection
		conn.Close()
	}
	return leader, fmt.Errorf("no leader")
}

//
func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")
	// connect to the server
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	ip, err := surfClient.checkLeader()
	if err != nil {
		return err
	}
	leader := *ip
	//
	log.Println("call success")
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn) //return a metaStoreClient
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{}) //******* useful
	if err != nil {
		conn.Close()
		return err
	}

	*serverFileInfoMap = fileInfoMap.FileInfoMap //give value

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// panic("todo")
	// connect to the server
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	ip, err := surfClient.checkLeader()
	if err != nil {
		return err
	}
	leader := *ip
	log.Println("call success")
	// conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())

	conn, err := grpc.Dial(leader, grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn) //return a metaStoreClient
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	lsVersion, err := c.UpdateFile(ctx, fileMetaData) //******* useful
	if err != nil {
		conn.Close()
		return err
	}

	*latestVersion = lsVersion.Version //give value

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// panic("todo")
	// connect to the server
	ip, err := surfClient.checkLeader()
	if err != nil {
		return err
	}
	leader := *ip
	log.Println("call success")
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn) //return a metaStoreClient
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{}) //******* useful
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddr = addr.Addr //give value

	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
