package raft

import (
	"math/rand"
	"sync"
	"time"

	"bytes"
	"encoding/gob"

	"../labrpc"
)

/****************************************
* note:
* Follower 每次接收过信息之后都要从新设置过期时间
* Leader   每次发送信息之后都要设置过期时间
* goroutine 中不要有共享变量
            注意前后俩个函数调用锁的竞争机制
*
*
*
 ***************************************/

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

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)
const (
	HeartCycle      = time.Millisecond * 50
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

//
//日志结构体
//
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persist state on all servers and updated on stable
	//storage before respond to RPCs
	currentTerm int
	votedFor    int //投过票的ID
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyMsg chan ApplyMsg
	state    string

	granted_vote_totals int

	timer *time.Timer //定时器
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data != nil { // bootstrap without any state?
		if data != nil {
			buf := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buf)
			dec.Decode(&rf.currentTerm)
			dec.Decode(&rf.votedFor)
			dec.Decode(&rf.log)
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//默认设置当前投票为真
	may_grant := true

	if len(rf.log) > 0 { //日志可能为空

		//首先判断Candidate's log is up-to-date
		if rf.log[len(rf.log)-1].Term > args.LastLogTerm ||

			(rf.log[len(rf.log)-1].Term == args.LastLogTerm &&
				len(rf.log)-1 > args.LastLogIndex) {

			may_grant = false
		}
	}
	//如果参数的周期小于当前周期，说明已经过时
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//如果俩个周期相同，有可能同时为Candidate，，处理此部分条件
	if args.Term == rf.currentTerm {
		//说明当前服务器还没有投票
		if may_grant && rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}

		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm { //args's 的周期大于当前周期，
		//所有服务器都要遵循的条件，变为FOllower
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if may_grant {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.resetTimer()
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// replicate logs struct
//
type AppendEntryArgs struct {
	Term         int        //当前Leader的周期
	LeaderId     int        //Leader's id
	PrevLogIndex int        //接下来发送的log前一个对应的log索引
	PrevLogTerm  int        //接下来发送的log前一个对应的log周期
	Entries      []LogEntry //要被复制的log 信息
	LeaderCommit int        //leader 上已被提交了的索引号
}
type AppendEntryReply struct {
	Term        int
	Success     bool
	CommitIndex int //用于接收来个都同意的
}

//
//用于复制log到本地
//
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//ingore old term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.state = FOLLOWER //初始化本地信息
		rf.currentTerm = args.Term
		rf.votedFor = -1
		//
		reply.Term = args.Term

		//don't contain an entry at prevLogIndex whose term matches prevLogTerm
		if args.PrevLogIndex >= 0 &&
			(len(rf.log)-1 < args.PrevLogIndex ||
				rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			//find including  the term of the conflicting entry
			// and the first index it stores for that term
			reply.CommitIndex = len(rf.log) - 1

			if reply.CommitIndex > args.PrevLogIndex {
				reply.CommitIndex = args.PrevLogIndex
			}

			//找到log中index最大的和args中prevlogTerm相同
			for reply.CommitIndex >= 0 {
				if rf.log[reply.CommitIndex].Term == args.PrevLogTerm {
					break
				}
				reply.CommitIndex--
			}

			reply.Success = false

		} else {
			if args.Entries != nil {

				//删除不相同的日志
				rf.log = rf.log[:args.PrevLogIndex+1]
				//复制log
				rf.log = append(rf.log, args.Entries...)

				if args.LeaderCommit <= len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
					go rf.commitLogs()

				}
				reply.CommitIndex = len(rf.log) - 1
				reply.Success = true

			} else {

				if args.LeaderCommit <= len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
					go rf.commitLogs()
				}
				reply.CommitIndex = args.PrevLogIndex
				reply.Success = true
			}
		}
	}
	rf.persist()
	rf.resetTimer()
}

//
//处理投票结果
//
func (rf *Raft) handleRequestVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Vote server's Term is lower than current server's,ignore it
	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm { //如果大于当前，则当前服务器过时
		//更新当前状态信息
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		//rf.persist()
		rf.resetTimer()
		return

	}
	//如果投了同意票，并且当前状态也还处于候选状态
	if reply.VoteGranted && rf.state == CANDIDATE {

		rf.granted_vote_totals += 1 //得票数加一

		if rf.granted_vote_totals >= majority(len(rf.peers)) { //判断当前票数是否获得绝大部分服务器支持

			rf.state = LEADER                    //当前服务器状态更新为Leader
			rf.sendAppendEntryToALl()            //给所有服务器发送心跳信息，停止选举
			for i := 0; i < len(rf.peers); i++ { //初始化所有的nextIndex 和 matchIndex
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = -1
			}
			rf.resetTimer() //重新设置定时任务
		}
		return
	}
}

//
//提交日志
//
func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.log)-1 {
		rf.commitIndex = len(rf.log) - 1
	}

	//lastApplied 是最后一个被apply的，所以要加1
	for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
		rf.applyMsg <- ApplyMsg{Index: index + 1, Command: rf.log[index].Command}
	}

	//提交后需要修改已经应用的日志数
	rf.lastApplied = rf.commitIndex

}

//
//rpc调用：往Follower添加Log
//
func (rf *Raft) sendAppendEntryToFollower(server int,
	args AppendEntryArgs, reply *AppendEntryReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
//给所有Follower发送log
//此处不需要获得锁，此部分的所有操作只是发送logEntry
//其他地点发送的时候会先获取锁的
//
func (rf *Raft) sendAppendEntryToALl() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
		}
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}
		//复制要发送的LogEntry
		if rf.nextIndex[i] < len(rf.log) {
			args.Entries = rf.log[rf.nextIndex[i]:]
		}
		args.LeaderCommit = rf.commitIndex
		go func(server int, args AppendEntryArgs) {
			var reply AppendEntryReply
			ok := rf.sendAppendEntryToFollower(server, args, &reply)
			if ok {
				rf.handleAppendEntriesReply(server, reply)
			}
		}(i, args)
	}
}
func (rf *Raft) handleAppendEntriesReply(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	// Leader should degenerate to Follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.matchIndex[server] = reply.CommitIndex

		applyCount := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[server] <= rf.matchIndex[i] {
				applyCount++
			}
		}
		if applyCount >= majority(len(rf.peers)) &&
			rf.commitIndex < rf.matchIndex[server] &&
			rf.log[rf.matchIndex[server]].Term == rf.currentTerm {

			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLogs()
		}
	} else {
		//再一次发送信息
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.sendAppendEntryToALl()
	}
}

//
//用于设置定时任务
//
func (rf *Raft) resetTimer() {
	if rf.timer == nil { //当服务启动时，启动一个goruntine来启动定时器
		rf.timer = time.NewTimer(time.Millisecond * 1000)
		go func() {
			for {
				<-rf.timer.C     //如果能取出就说时间已经过时
				rf.handleTimer() //处理时间过时
			}
		}()
	}
	new_out := HeartCycle   //leader的过期时间是心跳时间，需要定时的发送心跳信息
	if rf.state != LEADER { //follower的过期时间，follower需要选取一个随机时间用于防止出现split vote
		new_out = time.Millisecond * time.Duration(ElectionMinTime+
			rand.Int63n(ElectionMaxTime-ElectionMinTime))
	}
	rf.timer.Reset(new_out)
}

//
// 处理时间过时
//
func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER { //开始election

		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.granted_vote_totals = 1
		rf.persist()
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
		}

		if len(rf.log) > 0 {
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
		}
		//给所有服务器发起投票请求
		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}
			//使用goroutine 来加速函数的返回
			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					rf.handleRequestVoteReply(reply)
				}
			}(server, args)
		}
	} else {
		rf.sendAppendEntryToALl()
	}
	rf.resetTimer()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.state != LEADER {
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index = len(rf.log)
	term = rf.currentTerm
	rf.persist()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.log = make([]LogEntry, 0)
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = FOLLOWER
	rf.applyMsg = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist() //持久化存储
	rf.resetTimer()

	return rf
}

//
// 用于决定大多数是多少
//
func majority(len int) int {
	return len/2 + 1
}
