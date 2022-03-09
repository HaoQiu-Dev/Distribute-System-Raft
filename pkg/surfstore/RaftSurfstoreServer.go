package surfstore

import (
	context "context"
	"fmt"
	"math"
	reflect "reflect"
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

func (s *RaftSurfstore) checkAllCrash(support *chan bool) {
	SOP := *support
	crashChan := make(chan bool)
	for idx, _ := range s.ipList {
		//skip severself
		if int64(idx) == s.serverId {
			continue
		}
		go s.chechkFollowerCrash(int64(idx), &crashChan)
	}

	crashRecoverCount := 1
	for {
		<-crashChan
		crashRecoverCount++

		if crashRecoverCount > len(s.ipList)/2 {
			SOP <- true
			return //successfully replica more than half; committed := make(chan bool); s.pendingCommits = append(s.pendingCommits, committed)
		}
		//reached all nodes already
		if crashRecoverCount == len(s.ipList) {
			SOP <- false
			return
		}
	}
}

func (s *RaftSurfstore) chechkFollowerCrash(idx int64, crashChan *chan bool) {

	for {
		CRSchan := *crashChan
		addr := s.ipList[idx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		crashInfor, _ := client.IsCrashed(ctx, &emptypb.Empty{})
		if !crashInfor.IsCrashed {
			CRSchan <- true
			return
		}
	}

}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	// return nil, nil
	fmt.Println("getfilemap")
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_NOT_LEADER
	}

	support := make(chan bool)
	go s.checkAllCrash(&support)
	SOP := <-support
	if SOP {
		return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
	}
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil

}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	// return nil, nil
	fmt.Println("getfileblock")
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_NOT_LEADER
	}

	support := make(chan bool)
	go s.checkAllCrash(&support)
	SOP := <-support
	if SOP {
		return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
	}
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// panic("todo")
	// return nil, ERR_NOT_LEADER
	fmt.Println("updatefile!!====")
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)

	// committed := make(chan bool)
	// s.pendingCommits = append(s.pendingCommits, committed)

	ActivateChan := make(chan bool)

	go s.attemptCommit(&ActivateChan) //attempt relicate to other severs only once

	// success := <-committed //commite
	success := <-ActivateChan
	if success {
		fmt.Println("update success!")
		return s.metaStore.UpdateFile(ctx, filemeta)
	} else {
		fmt.Println("update fail!")
		return nil, fmt.Errorf("update fail") //errors.New("update failed")
	}
	// return nil, nil
}

//attempt relicate to other severs //s is the leader s.attempt -> replicate
func (s *RaftSurfstore) attemptCommit(ACTchan *chan bool) {
	ActivateChan := *ACTchan
	targetIdx := s.commitIndex + 1
	commitchan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		//skip severself
		if int64(idx) == s.serverId {
			continue
		}
		go s.replicEntry(int64(idx), targetIdx, commitchan) //5ci
	}

	logReplicaCount := 1
	logReplyCount := 1
	currentTerm := -1

	for {
		if s.isCrashed {
			return
		}
		if !s.isLeader {
			return
		}
		//TODO handle crashed nodes NEED // don't forever loop (each node once)
		commit := <-commitchan // go routine and get feedback
		currentTerm = int(math.Max(float64(currentTerm), float64(commit.Term)))
		logReplyCount++
		if commit.Success {
			logReplicaCount++
		}
		if logReplicaCount > len(s.ipList)/2 && int64(currentTerm) <= s.log[targetIdx].Term {
			// s.pendingCommits[targetIdx] <- true //successfully replica more than half; committed := make(chan bool); s.pendingCommits = append(s.pendingCommits, committed)
			s.commitIndex = targetIdx
			ActivateChan <- true
			break
		}
		//reached all nodes already
		if logReplicaCount == len(s.ipList) {
			// s.pendingCommits[targetIdx] <- false
			ActivateChan <- false
			break
		}
	}
}

// append/replicate log (Only leader s -> one follower )
func (s *RaftSurfstore) replicEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {

	// if s.isCrashed {}
	fmt.Println("try to replicate")
	output := &AppendEntryOutput{
		Success: false,
	}

	//go routine continueously try to update  //whole log?
	for {
		fmt.Println("try to replicate,loop")
		if s.isCrashed {
			commitChan <- output
			return
		}

		if !s.isLeader {
			commitChan <- output
			return
		}

		addr := s.ipList[serverIdx]
		fmt.Println("Dial to follower, need replicentry")
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			// commitChan <- output
			// return
			continue
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s. . etc
		//make the rest prelog and preterm here correctly! to sendheartbeat
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		//TODO handle crashed / non success cases ...?should return what?
		// crashState, _ := client.IsCrashed(ctx, &emptypb.Empty{})
		// if crashState.IsCrashed {
		// 	continue
		// }

		//modify input
		var input *AppendEntryInput
		if entryIdx == 0 {
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: -1,
				PrevLogTerm:  -1,
				// PrevLogIndex: entryIdx - 1,
				// PrevLogTerm:  s.log[entryIdx-1].Term,
				Entries:      s.log[:entryIdx+1], //index to position
				LeaderCommit: s.commitIndex,
			}
		} else if entryIdx > 0 {
			input = &AppendEntryInput{
				Term: s.term,
				// PrevLogIndex: -1,
				// PrevLogTerm:  -1,
				PrevLogIndex: entryIdx - 1,
				PrevLogTerm:  s.log[entryIdx-1].Term,
				Entries:      s.log[:entryIdx+1], //index to position
				LeaderCommit: s.commitIndex,
			}
		}

		output, err = client.AppendEntries(ctx, input)
		fmt.Println("try to append entry!")

		if err == nil {
			if output.Success {
				commitChan <- output
				return
			}
		}

		for err != nil {
			continue
		}

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

func (s *RaftSurfstore) matchTermAndEntry(input *AppendEntryInput, output *AppendEntryOutput) {
	i := input.PrevLogIndex
	for i >= 0 {
		if s.log[i].Term == input.Entries[i].Term && reflect.DeepEqual(s.log[i].FileMetaData, input.Entries[i].FileMetaData) {
			output.ServerId = s.serverId
			output.Term = s.term
			output.Success = true
			output.MatchedIndex = i
			break
		} else {
			i--
		}
	}

	if i < 0 {
		s.log = s.log[:0]
		s.log = append(s.log, input.Entries...)
		output.ServerId = s.serverId
		output.Term = s.term
		output.Success = true
		output.MatchedIndex = int64(len(s.log) - 1)
		return
	} else {
		s.log = s.log[:0]
		s.log = append(s.log, input.Entries...)
		output.ServerId = s.serverId
		output.Term = s.term
		output.Success = true
		output.MatchedIndex = int64(len(s.log) - 1)
		return
	}

	// for j:= output.MatchedIndex;j <= input.PrevLogIndex;j++{
	// 	s.log = append(s.log, )
	// }

}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")

	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}
	// fmt.Println("In appendentries!")
	if input.Term > s.term {
		fmt.Println("term ++")
		s.isLeader = false
		s.term = input.Term
	}
	// if input.Term == s.term {
	// 	fmt.Println("term equal")
	// }

	if s.isCrashed {
		fmt.Println("This sever crashed,now return")
		return output, ERR_SERVER_CRASHED
	}

	// if s.isLeader {
	// 	s.isLeader = false
	// }

	//1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		// output.Term = input.Term
		return output, nil
	}

	if len(input.Entries) == 0 {
		return output, nil
	}

	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	if input.PrevLogIndex >= 0 {
		i := input.PrevLogIndex + 1
		if int64(len(s.log)) == i {
			if s.log[i-1].Term == input.Entries[i-1].Term {
				if !reflect.DeepEqual(s.log[i-1].FileMetaData, input.Entries[i-1].FileMetaData) {
					return output, nil
				}
			}
		}
	}

	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it (§5.3)
	if input.Term > s.term {
		s.term = input.Term
	}

	if s.isCrashed {
		return output, ERR_SERVER_CRASHED
	}

	if len(s.log) > len(input.Entries) {
		s.log = s.log[:len(input.Entries)]
	}

	//4. Append any new entries not already in the log
	// s.log = append(s.log, input.Entries...)
	s.matchTermAndEntry(input, output)

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

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	// s.isLeaderMutex.Lock()
	fmt.Println("set leader")
	// s.isLeaderMutex.Unlock()
	// defer
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	// fmt.Println("=======")
	// fmt.Println(s.term)
	s.term++
	// fmt.Println(s.term)
	// fmt.Println("leader term ++")
	s.isLeader = true
	// fmt.Println("new leaderid")
	// fmt.Println(s.serverId)
	// fmt.Println("new leader state")
	// fmt.Println(s.isLeader)
	// s.isLeaderMutex.Lock()
	// log.Printf("leader changed to %d", s.serverId)
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//you can send nothing or sent logs! nomally send nothing otherwise send logs!
	// panic("todo")
	fmt.Println("send heart beat")
	// fmt.Println("The leader term")
	// fmt.Println(s.term)
	// fmt.Println("The leader id")
	// fmt.Println(s.serverId)
	// fmt.Println(s.isLeader)
	// fmt.Println("******")

	// check leader
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	for idx, addr := range s.ipList {
		//skip server itself
		if int64(idx) == s.serverId {
			continue
		}

		//Dial
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		// if err != nil {
		// 	continue
		// }
		client := NewRaftSurfstoreClient(conn)

		//TODO create correct AppendEntryIput from s.nextIdx, etc

		var input *AppendEntryInput

		targetIdx := s.commitIndex + 1

		if targetIdx >= int64(len(s.log)) {
			targetIdx--
		}

		if targetIdx == 0 {
			fmt.Println("my term give 1!")
			if len(s.log) == 0 {
				fmt.Println("my term give 1.1!")
				input = &AppendEntryInput{
					Term:         s.term,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      make([]*UpdateOperation, 0), //index to position
					LeaderCommit: s.commitIndex,
				}

			} else if len(s.log) > 0 {
				fmt.Println("my term give 1.2!")
				input = &AppendEntryInput{
					Term:         s.term,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      s.log[:targetIdx+1], //index to position
					LeaderCommit: s.commitIndex,
				}
			}

		} else if targetIdx > 0 {
			fmt.Println("my term give 2!")
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  s.log[targetIdx-1].Term,
				PrevLogIndex: targetIdx - 1,
				//TODO figure out which entries to send
				Entries:      s.log[:targetIdx+1],
				LeaderCommit: s.commitIndex,
			}
		} else if targetIdx < 0 {
			fmt.Println("my term give 3!")
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: -1,
				PrevLogTerm:  -1,
				Entries:      make([]*UpdateOperation, 0), //index to position
				LeaderCommit: s.commitIndex,
			}
			fmt.Println(input.Term)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fmt.Println("Go to Append entry")
		output, _ := client.AppendEntries(ctx, input)
		//retrun nil means The server is crashed
		if output == nil {
			// return &Success{Flag: false}, ERR_SERVER_CRASHED
			continue
		}
		if output != nil {
			//server is alive
			continue
		}
		if output.Success {
			continue
		}
	}
	fmt.Println("send beats over")
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
