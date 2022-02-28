package surfstore

import (
	context "context"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	Lock sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	//return the block with index
	bs.Lock.Lock()
	defer bs.Lock.Unlock()
	hashIndex := blockHash.Hash
	return bs.BlockMap[hashIndex], nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	bs.Lock.Lock()
	defer bs.Lock.Unlock()
	hashIndex := GetBlockHashString(block.BlockData)
	bs.BlockMap[hashIndex] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	bs.Lock.Lock()
	defer bs.Lock.Unlock()
	hashIndexList := blockHashesIn.Hashes
	var existBlocks BlockHashes
	for _, hashIdx := range hashIndexList {
		if _, ok := bs.BlockMap[hashIdx]; ok {
			existBlocks.Hashes = append(existBlocks.Hashes, hashIdx)
		}
	}
	return &existBlocks, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
