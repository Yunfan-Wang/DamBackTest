package raft

// Raft implementation
//
// general notes:
// 
// - you are welcome to use additional helper functions to handle aspects of the Raft algorithm logic
//   within the scope of a single Raft peer.  you should not need to create any additional remote
//   calls between Raft peers or the Controller.  if there is a desire to create additional remote
//   calls, please talk with the course staff before doing so.
//
// - Raft peers are not able to share information with each other or with the Controller in any way
//   other than through remote calls, allowing peers the potential to operate on physically
//   distinct machines
//
// - please make sure to read the Raft paper (https://raft.github.io/raft.pdf) before attempting
//   any coding for this lab.  you will most likely need to refer to it many times during your
//   implementation and testing tasks, so please consult the paper for algorithm details.
//
// - each Raft peer will accept a lot of concurrent remote calls from other Raft peers and the 
//   Controller, so use of concurrency controls is essential.  you are expected to use such controls 
//   to prevent race conditions in your implementation.  the Makefile supports testing both without
//   and with go's race condition detector, and the testing system will enable the race condition
//   detector, which will cause tests to fail if any race conditions are encountered.
//
// - don't forget to ask for help!


import (
    "remote"
    "sync"
    "time"
    "math/rand"
    "encoding/gob"
    "fmt"
)

type LogEntry struct {
    Command []byte
    Term    int
}

// Controller sends to Raft peer at creation time. do not change.
type RaftSetupInfo struct {
    Id int
    Addr string
    Caddr string
}

// Raft peer must send to Controller on request.  do not change.
type StatusReport struct {
    Index int
    Term int
    Leader bool
    Active bool
    CallCount int
}
// RequestVote based on Raft Figure
type RequestVoteArgs struct {
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}
func init() {
    gob.Register(StatusReport{})
    gob.Register(RequestVoteArgs{})
    gob.Register(RequestVoteReply{})
    gob.Register(AppendEntriesArgs{})
    gob.Register(AppendEntriesReply{})
    gob.Register(LogEntry{})
    gob.Register([]LogEntry{})
}
// empty template for the "service interface" that specifies remote calls between Raft peers.
// it must include the two remote methods needed for the Raft algorithm.
type RaftInterface struct {
    RequestVote   func(RequestVoteArgs) (RequestVoteReply, remote.RemoteError)
    AppendEntries func(AppendEntriesArgs) (AppendEntriesReply, remote.RemoteError)
}

// complete template for the Control "service interface" that specifies remote calls from Controller
// to Raft peer.  the ControlInterface is active from the moment the Raft peer is created until the 
// Raft peer is no longer needed by the Controller.  this interface specifies six remote methods that 
// you must implement. these methods are described later in this file.
type ControlInterface struct {
    Activate func() remote.RemoteError
    Deactivate func() remote.RemoteError
    Terminate func() remote.RemoteError
    GetStatus func() (StatusReport, remote.RemoteError)
    GetCommittedCmd func(int) ([]byte, remote.RemoteError)
    NewCommand func([]byte) (StatusReport, remote.RemoteError)
}
//hkvc added module
type ApplyMsg struct {
    Index   int
    Command []byte
}

type SubmitResult struct {
    Index    int
    Term     int
    IsLeader bool
}

// explain the current status of each Raft peer.  it doesn't matter what you call this struct,
// and the test code doesn't really care what state it contains, so this part is up to you.
type RaftPeer struct {
    mu        sync.Mutex
    info      RaftSetupInfo
    peers     []*RaftInterface
    me        int
    active    bool
    currentTerm int
    votedFor    int
    log         []LogEntry
    commitIndex int
    lastApplied int
    role        int
	
	leaderHint int

    nextIndex  []int
    matchIndex []int
    heartbeatRunning bool
    electionTimer *time.Timer
    callCount int
    controlStub remote.Callee
    raftStub    remote.Callee
    dead        bool
	//hkvc
	applyCh chan ApplyMsg
}


// the Controller calls NewRaftPeer in its own go routine to spawn a new Raft peer.  the arguments
// contain everything needed for the new Raft peer to determine its own identity and callee addresses
// as well as the relevant callee address of all other Raft peers. examples of the RaftSetupInfo are
// provided in the lab description on canvas.  the index parameter indicates the index in the slice
// of RaftSetupInfo structs corresponding to this new Raft peer, and the remaining info is for other peers.
// 
func NewRaftPeer(peerInfo []RaftSetupInfo, index int) *RaftPeer{

// when a new raft peer is created, its initial state should be populated into the corresponding
// struct entries, it should create two Callee stubs and N-1 Caller stubs, where N is the Raft group size.
// the Callee stub for the ControlInterface must be started immediately, so the Raft peer can accept
// commands from the Controller, but the Callee stub for the RemoteInterface should not be started until
// the Controller issues the remote to call telling the peer to do start.
// 
// the CalleeStubs using the RemoteInterface and ControlInterface should bind to the addresses in the 
// Addr and Caddr entry in peerInfo[index], respectively.  each caller stub created using 
// remote.CallerStubCreator should be used to send Raft algorithm commands to a different Raft
// peer in the group.  the addresses provided by the Controller are guaranteed to be unique (i.e., 
// no peers will have the same ID or use the same address.
    p := &RaftPeer{
        info:        peerInfo[index],
        me:          index,
        votedFor:    -1,
		leaderHint: -1,
        log:         make([]LogEntry, 1),
        role:        0,
        peers:       make([]*RaftInterface, len(peerInfo)),
        nextIndex:   make([]int, len(peerInfo)),
        matchIndex:  make([]int, len(peerInfo)),
    }


    for i, info := range peerInfo {
        if i != index {
            p.peers[i] = &RaftInterface{}
            remote.CallerStubCreator(p.peers[i], info.Addr, false, false)
        }
    }

    // Setup Control Callee Stub immediately
    control := &ControlInterface{
        Activate:        p.Activate,
        Deactivate:      p.Deactivate,
        Terminate:       p.Terminate,
        GetStatus:       p.GetStatus,
        GetCommittedCmd: p.GetCommittedCmd,
        NewCommand:      p.NewCommand,
    }

    if p.info.Caddr != "" {
        cstub, err := remote.NewCalleeStub(control, p, p.info.Caddr, false, false)
        if err != nil {
            return nil
        }
        p.controlStub = cstub
        _ = p.controlStub.Start()
    }

    raftIface := &RaftInterface{
		RequestVote:   p.RequestVote,
		AppendEntries: p.AppendEntries,
	}
    rstub, err := remote.NewCalleeStub(raftIface, p, p.info.Addr, false, false)
    if err != nil {
        return nil
    }

    p.raftStub = rstub

	//hkvc
	p.applyCh = make(chan ApplyMsg, 256)
	go p.applyLoop()
    return p
}

// ResetElectionTimer restarts the randomized election timeout for this peer.
//
// Limitations / potential failure scenarios:
//   - Poor timer tuning can lead to too many elections or slow leader recovery.
//   - Timer behavior depends on runtime scheduling and is not perfectly precise.
func (p *RaftPeer) ResetElectionTimer() {
    timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
    if p.electionTimer == nil {
        p.electionTimer = time.AfterFunc(timeout, p.StartElection)
    } else {
        p.electionTimer.Stop()
        p.electionTimer.Reset(timeout)
    }
}

//// method implementations for the ControlInterface

// * Activate -- this remote method is used exclusively by the Controller whenever it needs 
// to start the underlying server in the Raft peer and allow it to receive calls from other Raft 
// peers.  this is used to emulate connecting a new peer to the network or recovery of a previously
// failed peer. when this method is called, the Raft peer should do whatever is necessary to enable 
// its remote.CallerStub interface to support remote calls from other Raft peers as soon as the method 
// returns (i.e., if it takes time for the remote.CallerStub to start, this method should not 
// return until that happens).  the method should not otherwise block the Controller.
//
// Limitations / potential failure scenarios:
// - The implementation does not restore state from persistent storage; it
//   only resumes the in-memory state that was kept while the peer was inactive.
// - If a peer is reactivated while it still believes it is leader, it resumes
//   heartbeat transmission based on its local role until a higher-rank reply
//   forces it to step down?
// - This lab model simulates failures by disabling RPC participation only;
//     it does not model partial crashes, delayed recovery initialization, or
//     disk-corruption scenarios.
func (p *RaftPeer) Activate() remote.RemoteError {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dead {
		return remote.RemoteError{}
	}
	if p.raftStub != nil && !p.raftStub.IsRunning() {
		if err := p.raftStub.Start(); err != nil {
			return remote.RemoteError{Err: err.Error()}
		}
	}

	p.active = true

	if p.role == 2 {
		if !p.heartbeatRunning {
			p.heartbeatRunning = true
			go p.SendHeartbeats()
		}
	} else {
		p.ResetElectionTimer()
	}

    fmt.Printf("RAFT Activate: me=%d active=%v peerCount=%d\n", p.me, p.active, len(p.peers))
    fmt.Printf("RAFT Activate result: me=%d role=%d term=%d\n", p.me, p.role, p.currentTerm)
	return remote.RemoteError{}
}

// * Deactivate -- this remote method performs the "inverse" operation to Activate, namely to stop
// the underlying server in the Raft peer to emulate disconnection / failure of the Raft peer.  when 
// called, the Raft peer should disable only the stub serving the RaftInterface, causing any remote 
// calls to this Raft peer to fail due to connection error.  when deactivated, a Raft peer should 
// not make or receive any remote calls on the stub using the RaftInterface, and any execution of 
// the Raft protocol should effectively pause.  however, local state should be maintained and the 
// stub using the ControlInterface should continue to operate without disruption.  if a Raft node 
// was the leader when it was deactivated, it should still believe it is the leader when it reactivates.
// 
// Limitations / potential failure scenarios:
//   - This implementation models deactivation as an RPC level disconnection only;
//     it does not erase memory complete, reset term state, or simulate permanent crash loss.
//   - A deactivated peer retains its previous role in memory. This includes a former
//     leader role, until later Raft traffic causes it to step down if needed.
//   - In-flight goroutines that were started before deactivation may still finish
//     their current work, although future Raft RPC handling is stopped.

func (p *RaftPeer) Deactivate() remote.RemoteError {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.active = false
    p.heartbeatRunning = false
    if p.electionTimer != nil {
        p.electionTimer.Stop()
    }
    if p.raftStub != nil && p.raftStub.IsRunning() {
        if err := p.raftStub.Stop(); err != nil {
            return remote.RemoteError{Err: err.Error()}
        }
    }
    return remote.RemoteError{}
}

// * Terminate -- this remote method is used exclusively by the Controller to permanently cease operation
// of the Raft peer.  this is called at the end of each test when the Raft peer is no longer needed,
// and it allows the Raft peer to completely terminate all services and delete all relevant state.
//
// Limitations / potential failure scenarios:
//   - This implementation does not perform persistence cleanup, or graceful state transfer before termination.
//   - Because termination is final for the lifetime of this process, the peer is
//     not expected to recover afterward.
//   - This lab setup assumes the Controller will not continue issuing meaningful
//     requests after termination. Maybe behavior outside that contract is not a target
//     use case?
func (p *RaftPeer) Terminate() remote.RemoteError {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.dead = true
    p.active = false
    p.heartbeatRunning = false
    if p.electionTimer != nil {
        p.electionTimer.Stop()
    }
    if p.raftStub != nil && p.raftStub.IsRunning() {
        _ = p.raftStub.Stop()
    }
    if p.controlStub != nil && p.controlStub.IsRunning() {
        _ = p.controlStub.Stop()
    }

    return remote.RemoteError{}
}

// Immediate helper for getCallCount
func (p *RaftPeer) getCallCountSafe() int {
	total := 0
	if p.controlStub != nil {
		total += p.controlStub.GetCallCount()
	}
	if p.raftStub != nil {
		total += p.raftStub.GetCallCount()
	}
	return total
}
// * GetStatus -- this remote method is used exclusively by the Controller.  this method takes no arguments
// and is essentially a "getter" for the state of the Raft peer, including the Raft peer's current term, 
// current last log index, role in the Raft algorithm, active/non-active state, and total number of remote
// calls handled since starting. the method returns a StatusReport as defined above.
//
// Limitations / potential failure scenarios:
//   - The returned status is only a single point in time snapshot taken under the local
//     mutex; concurrent Raft activity may change immediately after the method
//     returns which is expected.
//   - Leader indicates only the peer's current local belief, it's not a global sign
//     that no other peer temporarily believes it is leader.
//   - The reported log index is the highest local log index, not necessarily the
//     highest committed index.
func (p *RaftPeer) GetStatus() (StatusReport, remote.RemoteError) {
	p.mu.Lock()
	index := len(p.log) - 1
	term := p.currentTerm
	leader := p.role == 2
	active := p.active
	p.mu.Unlock()

	callCount := p.getCallCountSafe()

	return StatusReport{
		Index:     index,
		Term:      term,
		Leader:    leader,
		Active:    active,
		CallCount: callCount,
	}, remote.RemoteError{}
}

// * GetCommittedCmd -- this remote method is used exclusively by the Controller.  this method provides an 
// input argument `index`.  if the Raft peer has a log entry at the given `index`, and that log entry has
// been committed (per the Raft algorithm), then the command stored in the log entry should be returned
// to the Controller.  otherwise, the Raft peer should return the nil value of the command type to 
// indicate that no committed log entry exists at that index.
//
// Limitations / potential failure scenarios:
//   - This method exposes only locally known committed state; if that peer is
//     behind the cluster, it may return nil sometimes even when another peer has already
//     advanced further.
func (p *RaftPeer) GetCommittedCmd(index int) ([]byte, remote.RemoteError) {
    p.mu.Lock()
    defer p.mu.Unlock()

    // Ensure index is within bounds and has been committed 
    if index >= 0 && index < len(p.log) && index <= p.commitIndex {
        return p.log[index].Command, remote.RemoteError{}
    }
    return nil, remote.RemoteError{}
}

// * NewCommand -- this remote method is used exclusively by the Controller.  this method emulates submission
// of a new command by a Raft client to this Raft peer, which should be handled and processed according to
// the rules of the Raft algorithm.  in particular, the Raft peer should accept the command only if it is
// currently active and believes it is the leader.  regardless of whether the command is accepted and 
// processed, the Raft peer should return a StatusReport reflecting the status of the Raft peer after the
// new command message was received.
//
// Limitations / potential failure scenarios:
//   - This implementation accepts commands based on local leader belief only, a
//     leadership change may occur shortly after acceptance which is a obvious problem.

func (p *RaftPeer) NewCommand(command []byte) (StatusReport, remote.RemoteError) {
    sub := p.Submit(command)

	if !sub.IsLeader {
		return p.GetStatus()
	}

	for {
		p.mu.Lock()
		applied := p.lastApplied >= sub.Index
		p.mu.Unlock()
		if applied {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return p.GetStatus()
}
// Lab3 addition
func (p *RaftPeer) IsLeader() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.role == 2
}

func (p *RaftPeer) LogIndex() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.log) - 1
}

//// method implementations for the RaftInterface

// * RequestVote -- this remote method is called (only) by other Raft peers and should operate according
// to the description in the Raft paper.
//
// Limitations / potential failure scenarios:
//   - This only uses local log info, if the peer is behind, it may still make suboptimal decisions.
//   - Too rapid a term change (maybe due to unstable network) can cause frequent vote resets and re-elections.
func (p *RaftPeer) RequestVote(args RequestVoteArgs) (RequestVoteReply, remote.RemoteError) {
	p.mu.Lock()
	defer p.mu.Unlock()

	reply := RequestVoteReply{}

	if !p.active {
		reply.Term = p.currentTerm
		reply.VoteGranted = false
		return reply, remote.RemoteError{}
	}

	if args.Term < p.currentTerm {
		reply.Term = p.currentTerm
		reply.VoteGranted = false
		return reply, remote.RemoteError{}
	}

	if args.Term > p.currentTerm {
		p.currentTerm = args.Term
		p.role = 0
		p.votedFor = -1
	}

	myLastLogIndex := len(p.log) - 1
	myLastLogTerm := p.log[myLastLogIndex].Term

	upToDate := args.LastLogTerm > myLastLogTerm ||
		(args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)

	if (p.votedFor == -1 || p.votedFor == args.CandidateId) && upToDate {
		p.votedFor = args.CandidateId
		reply.VoteGranted = true
		p.ResetElectionTimer()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = p.currentTerm
	return reply, remote.RemoteError{}
}
// * AppendEntries -- this remote method is called (only) by other Raft peers and should operate according
// to the description in the Raft paper.
//
// Limitations / potential failure scenarios:
//   - Log backtracking is simple (one step at a time), so recovery can be slow for large mismatches.
//   - If the follower is far behind, it may take multiple rounds to fully catch up?
func (p *RaftPeer) AppendEntries(args AppendEntriesArgs) (AppendEntriesReply, remote.RemoteError) {
	p.mu.Lock()
	defer p.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    p.currentTerm,
		Success: false,
	}

	if !p.active {
		return reply, remote.RemoteError{}
	}

	if args.Term < p.currentTerm {
		reply.Term = p.currentTerm
		reply.Success = false
		return reply, remote.RemoteError{}
	}

	if args.Term > p.currentTerm {
		p.currentTerm = args.Term
		p.votedFor = -1
	}
	p.role = 0
	p.ResetElectionTimer()

	if args.PrevLogIndex >= len(p.log) || p.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = p.currentTerm
		return reply, remote.RemoteError{}
	}

	if len(args.Entries) > 0 {
		p.log = append(p.log[:args.PrevLogIndex+1], args.Entries...)
	}

	if args.LeaderCommit > p.commitIndex {
		if args.LeaderCommit < len(p.log)-1 {
			p.commitIndex = args.LeaderCommit
		} else {
			p.commitIndex = len(p.log) - 1
		}
	}
	p.leaderHint = args.LeaderId
	reply.Success = true
	reply.Term = p.currentTerm
	return reply, remote.RemoteError{}
}

// More helper functions


// StartElection begins a new election by turning the peer into a candidate, increasing the term, and requesting votes from other peers.
//
// Limitations / potential failure scenarios:
//   - Multiple peers may start elections at the same time, causing split votes and retries.
//   - Network delays can cause elections to restart even when a leader exists?
func (p *RaftPeer) StartElection() {
	p.mu.Lock()
	if !p.active {
		p.mu.Unlock()
		return
	}
	p.role = 1 // Candidate
	p.currentTerm++
	p.votedFor = p.me
	term := p.currentTerm
	lastLogIdx := len(p.log) - 1
	lastLogTerm := p.log[lastLogIdx].Term
	p.ResetElectionTimer()
	p.mu.Unlock()

	votes := 1
	var voteMu sync.Mutex
    if votes > len(p.peers)/2 {
        p.mu.Lock()
        if p.role == 1 && p.currentTerm == term {
            p.role = 2
			p.leaderHint = p.me
			p.log = append(p.log, LogEntry{
				Command: nil,
				Term:    p.currentTerm,
			})
			if p.electionTimer != nil {
				p.electionTimer.Stop()
			}
			for j := range p.nextIndex {
				p.nextIndex[j] = len(p.log)
				p.matchIndex[j] = 0
			}
			p.matchIndex[p.me] = len(p.log) - 1
			if !p.heartbeatRunning {
				p.heartbeatRunning = true
				go p.SendHeartbeats()
			}
        }
        p.mu.Unlock()
        return
    }


	for i := range p.peers {
        if i == p.me || p.peers[i] == nil {
            continue
        }
		go func(idx int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  p.me,
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := p.peers[idx].RequestVote(args)
			if err.Err == "" {
				p.mu.Lock()
				if reply.Term > p.currentTerm {
					p.currentTerm = reply.Term
					p.role = 0
					p.votedFor = -1
					p.mu.Unlock()
					return
				}
				p.mu.Unlock()

				if reply.VoteGranted {
					voteMu.Lock()
					votes++
					if votes > len(p.peers)/2 {
						p.mu.Lock()
						if p.role == 1 && p.currentTerm == term {
                            p.role = 2
							p.leaderHint = p.me
							p.log = append(p.log, LogEntry{
								Command: nil,
								Term:    p.currentTerm,
							})
							if p.electionTimer != nil {
								p.electionTimer.Stop()
							}
							for j := range p.nextIndex {
								p.nextIndex[j] = len(p.log)
								p.matchIndex[j] = 0
							}
							p.matchIndex[p.me] = len(p.log) - 1
							if !p.heartbeatRunning {
								p.heartbeatRunning = true
								go p.SendHeartbeats()
							}
                        }
						p.mu.Unlock()
					}
					voteMu.Unlock()
				}
			}
		}(i)
	}
}
// SendHeartbeats runs in a loop on the leader, sending AppendEntries RPCs
// to maintain leadership and replicate logs.
//
// Limitations / potential failure scenarios:
//   - Heartbeats are sent periodically, so new commands may not replicate instantly?
//   - Timing depends on fixed sleep; slow or fast environments may slightly affect behavior, which can be improved in some other ways.
func (p *RaftPeer) SendHeartbeats() {
	for {
		p.mu.Lock()
		if !p.active || p.role != 2 {
            p.heartbeatRunning = false
			p.mu.Unlock()
			return
		}
		term := p.currentTerm
		leaderCommit := p.commitIndex
		p.mu.Unlock()

		for i := range p.peers {
			if i == p.me || p.peers[i] == nil {
				continue
			}
			go func(idx int) {
				p.mu.Lock()
				if p.role != 2 {
					p.mu.Unlock()
					return
				}
                if p.nextIndex[idx] < 1 {
					p.nextIndex[idx] = 1
				}
				if p.nextIndex[idx] > len(p.log) {
					p.nextIndex[idx] = len(p.log)
				}
				prevIdx := p.nextIndex[idx] - 1
				prevTrm := p.log[prevIdx].Term
				entries := make([]LogEntry, len(p.log[p.nextIndex[idx]:]))
				copy(entries, p.log[p.nextIndex[idx]:])
				p.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     p.me,
					PrevLogIndex: prevIdx,
					PrevLogTerm:  prevTrm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				reply, err := p.peers[idx].AppendEntries(args)
				if err.Err == "" {
					p.mu.Lock()
					defer p.mu.Unlock()
					if reply.Term > p.currentTerm {
						p.currentTerm = reply.Term
						p.role = 0
						p.votedFor = -1
                        p.heartbeatRunning = false
						return
					}
					if reply.Success {
						p.matchIndex[idx] = prevIdx + len(entries)
						p.nextIndex[idx] = p.matchIndex[idx] + 1
						p.updateCommitIndex()
					} else {
						p.nextIndex[idx]--
						if p.nextIndex[idx] < 1 {
							p.nextIndex[idx] = 1
						}
					}
				}
			}(i)
		}
		time.Sleep(120 * time.Millisecond)
	}
}
// SendHeartbeats runs in a loop on the leader, sending AppendEntries RPCs
// to maintain leadership and replicate logs.
//
// Limitations / potential failure scenarios:
//   - Timing depends on fixed sleep slow issue as SendHeartbeats()
func (p *RaftPeer) updateCommitIndex() {
    for n := len(p.log) - 1; n > p.commitIndex; n-- {
        if p.log[n].Term == p.currentTerm {
            count := 1
            for i := range p.peers {
                if i != p.me && p.matchIndex[i] >= n {
                    count++
                }
            }
            if count > len(p.peers)/2 {
                p.commitIndex = n
                break
            }
        }
    }
}



func (p *RaftPeer) PeerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.peers)
}



// HKVC Methods
func (p *RaftPeer) ApplyChan() <-chan ApplyMsg {
    return p.applyCh
}

func (p *RaftPeer) Submit(command []byte) SubmitResult {
    p.mu.Lock()
    defer p.mu.Unlock()

    if !p.active || p.role != 2 {
        return SubmitResult{
            Index:    len(p.log) - 1,
            Term:     p.currentTerm,
            IsLeader: false,
        }
    }

    entry := LogEntry{Command: command, Term: p.currentTerm}
    p.log = append(p.log, entry)
    idx := len(p.log) - 1
    p.matchIndex[p.me] = idx
    p.nextIndex[p.me] = idx + 1

	if len(p.peers) == 1 {
		p.commitIndex = idx
	}

    return SubmitResult{
        Index:    idx,
        Term:     p.currentTerm,
        IsLeader: true,
    }
}

//worker
func (p *RaftPeer) applyLoop() {
    for {
        p.mu.Lock()
        if p.dead {
            p.mu.Unlock()
            return
        }
        if p.lastApplied < p.commitIndex {
            p.lastApplied++
            msg := ApplyMsg{
                Index:   p.lastApplied,
                Command: p.log[p.lastApplied].Command,
            }
            p.mu.Unlock()
            if p.applyCh != nil {
                p.applyCh <- msg
            }
            continue
        }
        p.mu.Unlock()
        time.Sleep(10 * time.Millisecond)
    }
}

func (p *RaftPeer) CommitIndex() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commitIndex
}

func (p *RaftPeer) LeaderIndex() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role == 2 {
		return p.me
	}
	return p.leaderHint
}

// DAMBT Specialization
func (p *RaftPeer) ForceLeaderForSingleNode() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.active = true
	p.role = 2
	p.leaderHint = p.me

	if len(p.log) == 1 {
		p.log = append(p.log, LogEntry{
			Command: nil,
			Term:    p.currentTerm,
		})
	}

	for i := range p.nextIndex {
		p.nextIndex[i] = len(p.log)
		p.matchIndex[i] = 0
	}
	p.matchIndex[p.me] = len(p.log) - 1
}