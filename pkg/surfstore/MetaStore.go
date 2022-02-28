package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
	Lock sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	m.Lock.Lock()
	defer m.Lock.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	// metaFMD := m.FileMetaMap[fileMetaData.Filename]
	//new data
	m.Lock.Lock()
	defer m.Lock.Unlock()
	if metaFMD, ok := m.FileMetaMap[fileMetaData.Filename]; !ok {
		if fileMetaData.Version == 1 {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		} else {
			return &Version{Version: -1}, nil
		}

	} else {
		if fileMetaData.Version > metaFMD.Version {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		} else {
			return &Version{Version: -1}, nil
		}
	}
	return &Version{Version: fileMetaData.Version + 1}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	m.Lock.Lock()
	defer m.Lock.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
