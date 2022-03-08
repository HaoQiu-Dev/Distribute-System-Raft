package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool

	lastApplied int64

	//Sever Info
	ip       string
	ipList   []string
	serverId int64

	//Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	rpcClient []raftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	// return nil, nil
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	// return nil, nil
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// panic("todo")

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)

	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)

	go s.attemptCommit() //attempt relicate to other severs

	success := <-committed //commite
	if success {
		return s.metaStore.UpdateFile(ctx, filemeta)
	} else {
		return nil, fmt.Errorf("update fail") //errors.New("update failed")
	}
	// return nil, nil
}

//attempt relicate to other severs //s is the leader s.attempt -> replicate
func (s *RaftSurfstore) attemptCommit() {
	targetIdx := s.commitIndex + 1
	commitchan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		//skip severself
		if int64(idx) == s.serverId {
			continue
		}
		go s.replicEntry(int64(idx), targetIdx, commitchan)
	}

	logReplicaCount := 1
	logReplyCount := 1

	for {
		//TODO handle crashed nodes NEED // don't forever loop (each node once)
		commit := <-commitchan // go routine and get feedback
		logReplyCount++
		if commit != nil && commit.Success {
			logReplicaCount++
		}
		if logReplicaCount > len(s.ipList)/2 {
			s.pendingCommits[targetIdx] <- true //successfully replica more than half; committed := make(chan bool); s.pendingCommits = append(s.pendingCommits, committed)
			s.commitIndex = targetIdx
			break
		}
		//reached all nodes already
		if logReplicaCount == len(s.ipList) {
			s.pendingCommits[targetIdx] <- false
			break
		}
	}
}

// append/replicate log
func (s *RaftSurfstore) replicEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	//go routine continueously try to update  //whole log?
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s. . etc
		//make the rest prelog and preterm here correctly! to sendheartbeat

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      s.log[:entryIdx+1],
			LeaderCommit: s.commitIndex,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output.Success {
			commitChan <- output
			return
		}

		//TODO update state. s.nextIndex, etc / If failed, we want to decrement nextIndex and try again (retry needed here)

		//TODO handle crashed / non success cases ...?should return what?

	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")

	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}
	if input.Term > s.term {
		s.term = input.Term
	}

	//1. Reply false if term < currentTerm (§5.1)
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it (§5.3)
	//4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	//TODO only do this if leaderCommit > commitIndex
	s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

	//

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}
	output.Success = true

	return nil, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	s.term++
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//you can send nothing or sent logs! nomally send nothing otherwise send logs!
	// panic("todo")
	for idx, addr := range s.ipList {
		//skip server itself
		if int64(idx) == s.serverId {
			continue
		}
		//Dial
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)

		//TODO create correct AppendEntryIput from s.nextIdx, etc
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			//TODO figure out which entries to send
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		//retrun nil means The server is crashed
		if output != nil {
			//server is alive
		}
		if output.Success {

		}
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
