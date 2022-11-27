package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
const (
	Leader State = iota
	Candidate
	Follower
)

type State int

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index,
	Term int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	votedFor     int
	votes        int
	currentTerm  int
	currentState State
	logs         []LogEntry
	nextIndex    []int
	matchIndex   []int
	commitIndex  int
	lastApplied  int
	electionWin  chan bool
	heartBeat    chan bool
	applyChan    chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.currentState == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term,
	CandidateId,
	LastLogIndex,
	LastLogTerm int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == rf.me) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		log := fmt.Sprintf("%d voted for %d at term %d", rf.me, args.CandidateId, rf.currentTerm)
		DPrintf(log)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}

}
func (rf *Raft) getLastLogIndex() int {

	return len(rf.logs) - 1
}
func (rf *Raft) getLastLogTerm() int {

	return rf.logs[len(rf.logs)-1].Term
}
func (rf *Raft) isUpToDate(cTerm, cIdx int) bool {

	return cTerm > rf.getLastLogTerm() || (cTerm == rf.getLastLogTerm() && cIdx >= rf.getLastLogIndex())
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentState != Candidate || rf.currentTerm != args.Term {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentState = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
		}
		if reply.VoteGranted {
			rf.votes++
			if rf.votes >= len(rf.peers)/2 {
				log := fmt.Sprintf("%d won the election at term %d", rf.me, rf.currentTerm)
				DPrintf(log)
				rf.currentState = Leader
				rf.electionWin <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAllRequestVotes() {
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	rf.mu.Unlock()
	for p := range rf.peers {
		if p != rf.me && rf.currentState == Candidate {
			log := fmt.Sprintf("%d sent request vote to %d at term %d", rf.me, p, args.Term)
			DPrintf(log)
			reply := RequestVoteReply{}
			go rf.sendRequestVote(p, &args, &reply)
		}
	}
}

type AppendEntriesArgs struct {
	Term,
	LeaderId,
	PrevLogIndex,
	PrevLogTerm int
}

type AppendEntriesReply struct {
	Term,
	ConflictIndex,
	ConflictTerm int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = -1
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.logs) {
		reply.ConflictIndex = len(rf.logs)
		return
	}
	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it and append any new entries not already in the log
	log := fmt.Sprintf("%d received heart beat from %d at term %d", rf.me, args.LeaderId, rf.currentTerm)
	DPrintf(log)
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.heartBeat <- true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok || rf.currentState != Leader || args.Term != rf.currentTerm {
			return ok
		}
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.currentState = Follower
			rf.votedFor = -1
		}
		// if the leader has the conflict term in its log then it should update nextIndex to first entry of that term
		// else it should set nextIndex to conflictIndex
	}
	return ok
}

func (rf *Raft) sendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for p := range rf.peers {
		if p != rf.me && rf.currentState == Leader {
			log := fmt.Sprintf("%d sent heartbeat to %d at term %d", rf.me, p, rf.currentTerm)
			DPrintf(log)
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(p, &args, &reply)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.currentState == Leader
	if !isLeader {
		return index, term, false
	}
	term = rf.currentTerm
	index = len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term: term, Index: index, Command: command})

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) run() {
	for {
		rf.mu.Lock()
		state := rf.currentState
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.heartBeat:
				rf.mu.Lock()
				rf.currentState = Follower
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(100)):
				rf.sendAllAppendEntries()
			}

		case Follower:
			select {
			case <-rf.heartBeat:
			case <-time.After(time.Millisecond * time.Duration(300+rand.Intn(200))):
				rf.mu.Lock()
				rf.currentState = Candidate
				rf.mu.Unlock()
			}
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votes = 1
			rf.votedFor = rf.me
			curr := rf.me
			rf.mu.Unlock()
			go rf.sendAllRequestVotes()
			DPrintf("%d sent all request vote", curr)

			select {

			case <-rf.heartBeat:
				rf.mu.Lock()
				rf.currentState = Follower
				rf.mu.Unlock()
			case <-rf.electionWin:
			case <-time.After(time.Millisecond * time.Duration(300+rand.Intn(200))):
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votes = 0
	rf.votedFor = -1
	rf.electionWin = make(chan bool)
	rf.heartBeat = make(chan bool)

	rf.currentState = Follower
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.currentTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()
	return rf
}
