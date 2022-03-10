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

	commitIndex int64
	// pendingCommits []chan bool

	lastApplied int64

	//Sever Info
	ip       string
	ipList   []string
	serverId int64

	//Leader protection
	isLeaderMutex sync.RWMutex
	// isLeaderCond  *sync.Cond

	// rpcClient []raftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

// func (s *RaftSurfstore) checkAllCrash(support *chan bool) {
// 	fmt.Println("Begin check crash!")
// 	SOP := *support
// 	crashChan := make(chan bool)
// 	for idx, _ := range s.ipList {
// 		//skip severself
// 		if int64(idx) == s.serverId {
// 			continue
// 		}
// 		go s.chechkFollowerCrash(int64(idx), &crashChan)
// 	}

// 	crashRecoverCount := 1
// 	for {
// 		<-crashChan
// 		crashRecoverCount++

// 		if crashRecoverCount > len(s.ipList)/2 {
// 			SOP <- true
// 			return //successfully replica more than half; committed := make(chan bool); s.pendingCommits = append(s.pendingCommits, committed)
// 		}
// 		//reached all nodes already
// 		if crashRecoverCount == len(s.ipList) {
// 			SOP <- false
// 			return
// 		}
// 	}
// }

// func (s *RaftSurfstore) chechkFollowerCrash(idx int64, crashChan *chan bool) {
// 	fmt.Println("Begin check follower!")
// 	for {
// 		CRSchan := *crashChan
// 		addr := s.ipList[idx]
// 		conn, err := grpc.Dial(addr, grpc.WithInsecure())
// 		if err != nil {
// 			continue
// 		}
// 		client := NewRaftSurfstoreClient(conn)
// 		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 		defer cancel()

// 		crashInfor, _ := client.IsCrashed(ctx, &emptypb.Empty{})
// 		if !crashInfor.IsCrashed {
// 			CRSchan <- true
// 			return
// 		}
// 	}

// }

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	// return nil, nil
	fmt.Println("getfilemap")
	if s.isCrashed {
		return nil, ERR_NOT_LEADER
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	fmt.Println("client call success")
	ActivateChan := make(chan bool)
	go s.attemptCommit(ActivateChan)
	success := <-ActivateChan
	if success {
		return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
	}
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	// return nil, nil
	fmt.Println("getfileblock")
	if s.isCrashed {
		return nil, ERR_NOT_LEADER
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	fmt.Println("client call success")

	ActivateChan := make(chan bool)
	go s.attemptCommit(ActivateChan)
	success := <-ActivateChan
	if success {
		return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
	}
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// panic("todo")
	// return nil, ERR_NOT_LEADER
	fmt.Println("updatefile!!====")

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		fmt.Println("leader changed,call failed")
		return nil, ERR_NOT_LEADER
	}

	fmt.Println("client call success")
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	ActivateChan := make(chan bool)
	go s.attemptCommit(ActivateChan) //attempt relicate to other severs only once

	// success := <-committed //commite
	success := <-ActivateChan
	if success {
		fmt.Println("update success!")
		// s.metaStore.UpdateFile(ctx, filemeta)
		v, _ := s.metaStore.UpdateFile(ctx, filemeta)
		return v, nil
	} else {
		fmt.Println("update fail!")
		return nil, nil //errors.New("update failed")
	}
	// return nil, nil
}

//attempt relicate to other severs //s is the leader s.attempt -> replicate
func (s *RaftSurfstore) attemptCommit(ActivateChan chan bool) {
	fmt.Println("Begin attempt cmmit!")
	// ActivateChan := *ACTchan
	targetIdx := s.commitIndex + 1

	commitchan := make(chan *AppendEntryOutput, len(s.ipList))

	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.replicEntry(int64(idx), targetIdx, commitchan) // 1-1
	}

	replyCount := 1
	CommitNumberCount := 1
	// currentTerm := -1

	for {
		if s.isCrashed {
			return
		}
		if !s.isLeader {
			return
		}
		//TODO handle crashed nodes NEED // don't forever loop (each node once)
		commit := <-commitchan // go routine and get feedback
		// currentTerm = int(math.Max(float64(currentTerm), float64(commit.Term)))
		replyCount++
		if commit != nil && commit.Success {
			CommitNumberCount++
		}
		// && int64(currentTerm) <= s.log[targetIdx].Term
		if CommitNumberCount > len(s.ipList)/2 {
			fmt.Println("replcate greater > 1/2! commit!")
			if int(targetIdx) <= len(s.log)-1 {
				s.commitIndex = targetIdx
			}
			ActivateChan <- true
			fmt.Println("finish attempt commit!")
			return
		}
		//reached all nodes already
		if replyCount == len(s.ipList) {
			ActivateChan <- false
			return
		}
	}
}

// append/replicate log (Only leader s -> one follower )
func (s *RaftSurfstore) replicEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	fmt.Println("Begin REPLICAR!")
	output := &AppendEntryOutput{
		ServerId:     s.serverId,
		Success:      false,
		Term:         s.term,
		MatchedIndex: -1,
	}

	if s.isCrashed {
		commitChan <- output
		return
	}
	if !s.isLeader {
		commitChan <- output
		return
	}
	fmt.Println("Be in infinity loop!")
	//go routine continueously try to update  //whole log?
	// count := 0

	for {
		fmt.Println("try to replicate,loop")
		if s.isCrashed {
			// fmt.Println("leader crashd")
			commitChan <- output
			return
		}

		if !s.isLeader {
			commitChan <- output
			return
		}

		// TODO create correct AppendEntryInput from s. . etc
		//make the rest prelog and preterm here correctly! to sendheartbeat
		//TODO handle crashed / non success cases ...?should return what?
		//modify input
		var input *AppendEntryInput
		fmt.Println("make inout entry")
		fmt.Println("print entryIdx")
		fmt.Println(entryIdx)
		fmt.Println("print commited idx")
		fmt.Println(s.commitIndex)
		fmt.Println("print len(log)")
		fmt.Println(len(s.log))

		if entryIdx == 0 {
			if len(s.log) == 0 {
				input = &AppendEntryInput{
					Term:         s.term,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      make([]*UpdateOperation, 0), //index to position
					LeaderCommit: s.commitIndex}
			} else {
				input = &AppendEntryInput{
					Term:         s.term,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      s.log[:entryIdx+1], //index to position
					LeaderCommit: s.commitIndex}
			}
		} else if entryIdx > 0 {
			if len(s.log) == int(entryIdx) {
				input = &AppendEntryInput{
					Term:         s.term,
					PrevLogIndex: entryIdx - 1,
					PrevLogTerm:  s.log[entryIdx-1].Term,
					Entries:      s.log[:entryIdx], //index to position
					LeaderCommit: s.commitIndex}
			} else if len(s.log) > int(entryIdx) {
				input = &AppendEntryInput{
					Term:         s.term,
					PrevLogIndex: entryIdx - 1,
					PrevLogTerm:  s.log[entryIdx-1].Term,
					Entries:      s.log[:entryIdx+1], //index to position
					LeaderCommit: s.commitIndex}
			} else if len(s.log) < int(entryIdx) {
				fmt.Println("Queer entry")
				// 	input = &AppendEntryInput{
				// 		Term:         s.term,
				// 		PrevLogIndex: entryIdx - 1,
				// 		PrevLogTerm:  s.log[entryIdx-1].Term,
				// 		Entries:      s.log[:entryIdx-1], //index to position
				// 		LeaderCommit: s.commitIndex}
			}
		}

		addr := s.ipList[serverIdx]
		// fmt.Println("Dial to follower, need replicentry")
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fmt.Println("replica append entry in")
		fmt.Println(input.PrevLogIndex)
		output, err := client.AppendEntries(ctx, input)
		fmt.Println("replica append entry out")

		if s.isCrashed {
			output := &AppendEntryOutput{
				ServerId:     s.serverId,
				Success:      false,
				Term:         s.term,
				MatchedIndex: -1,
			}
			fmt.Println("leader crashd")
			commitChan <- output
			return
		}

		if !s.isLeader {
			output := &AppendEntryOutput{
				ServerId:     s.serverId,
				Success:      false,
				Term:         s.term,
				MatchedIndex: -1,
			}
			commitChan <- output
			return
		}

		// fmt.Println("try to append entry!")
		fmt.Println(err) //"check point"
		if err == nil {
			if output.Success {
				fmt.Println("try to append entry! Success!")
				commitChan <- output
				return
			} else {
				fmt.Println("server crash retrun attemt commit!")
				commitChan <- output
				return
			}
		}

		if err != nil {
			// if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) || strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), "DeadlineExceeded desc = context deadline exceeded") {
			// 	continue
			// } else {
			// 	commitChan <- output
			// 	fmt.Println("Append fails break!")
			// 	return
			// }
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

func (s *RaftSurfstore) matchTermAndEntry(input *AppendEntryInput, output *AppendEntryOutput) *AppendEntryOutput {
	fmt.Println("BEGIN match log")
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
		return output
	} else {
		s.log = s.log[:0]
		s.log = append(s.log, input.Entries...)
		output.ServerId = s.serverId
		output.Term = s.term
		output.Success = true
		output.MatchedIndex = int64(len(s.log) - 1)
		return output
	}
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")
	fmt.Println("Begin Append Entries!")

	output := &AppendEntryOutput{
		ServerId:     s.serverId,
		Success:      false,
		Term:         s.term,
		MatchedIndex: -1,
	}

	if s.isCrashed {
		fmt.Println("This sever crashed,now return")
		return output, ERR_SERVER_CRASHED
	}
	//modify term!!!!
	if input.Term > s.term {
		fmt.Println("term ++")
		s.isLeader = false
		s.term = input.Term
	}

	if len(input.Entries) == 0 {
		//just try to sync the state
		output.Success = false
		return output, nil
	}

	//1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		// output.Term = input.Term
		fmt.Println("small term false")
		return output, nil
	}

	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	if input.PrevLogIndex >= 0 {
		fmt.Println("doesn't contain!")
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
	fmt.Println("match log in")
	output = s.matchTermAndEntry(input, output)

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
	fmt.Println("put success! Now output!")
	output.Success = true
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")

	// s.isLeaderMutex.Lock()
	fmt.Println("Begin set leader")
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.term++
	s.isLeader = true
	// s.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//you can send nothing or sent logs! nomally send nothing otherwise send logs!
	// panic("todo")
	fmt.Println("begin send heart beat")
	// check leader
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	ActivateChan := make(chan bool)
	go s.attemptCommit(ActivateChan) //attempt relicate to other severs only once

	// success := <-committed //commite
	success := <-ActivateChan
	if success {
		fmt.Println("send beats over")
		return &Success{Flag: true}, nil
	} else {
		fmt.Println("send beats false")
		return &Success{Flag: false}, nil
	}

}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("Begin server crash!")
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("Begin server restore!")
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
	fmt.Println("Begin server getinternalstate!")
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
