# Barasona essentials
This document presents the essentials of the Barasona protocol as an aid for protocol implementators. As such, this document assumes at least moderate familiarity with the Barasona spec.

## Definitions
- `server`: a process running an implementation of the Barasona protocol.
- `cluster`: a group of servers working together to implement the requirements of the Barasona spec.
- `committed` (log): a log entry is committed once the leader that created the entry has replicated it on a majority of the servers. 
- `election timeout`: a randomized period of time which is reset with a new randomized value after every AppendEntries and RequestVote RPC is received (unless they are rejected). If the time elapses, the server will start a new election.
- `election timeout minimum`: the minimum period of time which a server must wait before starting an election. For compliance, all servers should be configured with the same minimum value. The randomized election timeout must always be greater than this value.

## Basics
- A leader is elected for the cluster, and is then given complete responsibility for managing the replicated log.
- The leader accepts log entries from clients, applies them to its state machine and replicates them on other servers.
- Data always flows from leader to followers, never any other direction.
- When a leader goes offline, a new leader is elected.
- At any given time each server is in one of four states: leader, candidate, follower, or non-voter.

## Terms
- Time is divided into terms of arbitrary length.
- Each server stores a current term number, which increases monotonically over time.
- Each term begins with an election, in which one or more candidates attempt to become leader.
- If a candidate wins the election, then it serves as leader for the rest of the term.
- In some situations an election will result in a split vote. In this case the term will end with no leader; a new term (with a new election) will begin shortly. 
- Barasona ensures that there is at most one leader in a given term.

## Server roles
- When servers start up, they begin as followers; and remain as followers as long as they receive valid RPCs from a leader or candidate.
- If a server receives a request with a stale term number, it rejects the request.
- Current terms are exchanged whenever servers communicate; if one server's current term is smaller than the other's, then it updates its current term to the larger value.
- To avoid issues where removed servers may disrupt the cluster by starting new elections, servers disregard RequestVote RPCs when they believe a current leader exists; specifically, if a server receives a RequestVote RPC within the election timeout minimum of hearing from a current leader, it does not update its term or grant its vote.
- If a leader or candidate discovers that its term is stale (via an RPC from a peer), it immediately reverts to follower state.
- A voter must deny its vote if its own log is more up-to-date than that of the candidate; if the logs have last entries with different terms, then the log with the later term is more up-to-date; if the logs end with the same term, then whichever log is longer is more up-to-date.

### Follower
Follower servers simply respond to requests from leaders and candidates. 
- A new randomized election timeout is started after each valid RPC received from the leader or candidates.
- If a follower receives no communication during the election timeout window, then it assumes there is no viable leader, transitions to the candidate state, and begins campaigning to become the new leader.

### Non-voter
Non-voters are the same as followers, except are not considered for votes, and do not have election timeouts.

### Candidate
Candidate servers campaign to become the cluster leader for a given term using the RequestVote RPC.

- The first action a candidate takes is to increment the current term.
- It then starts a new randomized election timeout within the configured range.
- It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
- A candidate continues in this state until one of three things happens:   
  - (a) it wins the election.
  - (b) another server establishes itself as leader.
  - (c) a period of time goes by with no winner.

#### Winning elections
- A candidate wins an election if it receives votes from a majority of the servers in the cluster for the same term.
- Each server will vote for at most one candidate in a given term, on a first-come-first-served basis.
- Once a candidate wins an election, it becomes leader for that term.

#### Loosing elections
- While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
- If the leader's term (included in its RPC) is at least as large as the candidate's current term, then the candidate recognizes the leader as legitimate and returns to follower state.
- If the term in the RPC is smaller than the candidate's current term, then the candidate rejects the RPC and continues in candidate state.

#### Elections without winner (split vote)
- If the candidate times out before receiving enough votes, and before another leader wins the election, it starts a new election by incrementing the term and initiating another round of RequestVote RPCs.

#### Receiver implementation: RequestVote RPC
1. Reply false if term < currentTerm or if a heartbeat was received from the leader within the election timeout minimum window.
2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.

### Leader
The cluster leader handles all client requests and client requests must be redirected to the leader if received by another node.

- A leader commits a blank no-op entry into the log at the start of its term.

- It must send periodic heartbeats (empty AppendEntries RPCs) to all followers in order to maintain their authority and prevent new elections.

- It must service client requests. Each client request contains a command to be executed by the replicated state machines:
  - The leader applies the client request to the state machine
  - The leader appends the command to its log as a new entry.
  - Then issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
  - At the same time it returns the result of that execution to the client, without waiting for followers to reply.
  - Once followers reply, the leader mark the entry as commited.
  - Notice that an uncommited entry could be lost even the leader has alredy replied to the client. This is a tradeoff betweeen performance and data durability. Every time a client requests to read data from the state machine, the leader will also inform whether it is commited or not.
- If followers crash or run slowly, or if network packets are lost, the leader retries AppendEntries RPCs indefinitely until all followers eventually store all log entries; AppendEntries RPCs should be issued in parallel for best performance; AppendEntries RPCs are idempotent.
- A leader never overwrites or deletes entries in its own log.
- If leader becomes follower, it fallbacks to previous snapshot if any, otherwise resets the state machine. This is done to ensure its state machine does not have any uncommited entry applied.

#### Replication
- A leader creates at most one entry with a given log index in a given term, and log entries never change their position in the log.
- When sending an AppendEntries RPC, the leader includes the index and term of the entry in its log that immediately precedes the new entries. If the follower does not find an entry in its log with the same index and term, then it refuses the new entries.
- The leader handles inconsistencies by forcing the followers' logs to duplicate its own. 
- To maintain the log consistency with the leader, the follower will reset its state machine to the latest snapshot and remove log entries from that state machine. Then the leader will send log entries that happened after the snapshot.
- All of these actions happen in response to the consistency check performed by AppendEntries RPCs. The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.
- When a leader first comes to power, it initializes all nextIndex values for all cluster peers to the index just after the last one commited in its log.
- If a follower's log is inconsistent with the leader's, the AppendEntries consistency check will fail in the next AppendEntries RPC to that follower. The response contains the last term and index commited for that follower.
- After a rejection, the leader decrements nextIndex until the term and index returned by the follower on the failed AppendEntries RPC and it retries the AppendEntries RPC. Once AppendEntries succeeds, the follower's log is consistent with the leader's, and it will remain that way for the rest of the term.

##### Receiver implementation: AppendEntries RPC
1. Reply false if term < currentTerm.
1. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm. Include last commited term and index in the response.
1. If an existing entry conflicts with a new one (same index but different terms), reset state machine to previous snapshot.
1. Commit all new entries after applying it to the state machine.
1. If leaderCommit > commitIndex, set commitIndex to min(leaderCommit, index of last new entry).

## Configuration changes
In order to ensure safety, configuration changes must use a two-phase approach, called `joint consensus`.

- Once a server adds a new configuration entry to its log, it uses that configuration for all future decisions (a server always uses the latest configuration in its log, regardless of whether the entry is committed).
- When joint consensus is entered, log entries are replicated to all servers in both configurations.
- Any server from either configuration may serve as leader.
- Agreement (for elections and entry commitment) requires separate majorities from both the old and new configurations.
- In order to avoid availability gaps, Barasona introduces an additional phase before the configuration change, in which the new servers join the cluster as non-voting members (the leader replicates log entries to them, but they are not considered for majorities). Once the new servers have caught up with the rest of the cluster, the reconfiguration can proceed as described above.
- In the case where the cluster leader is not part of the new configuration, the leader steps down (returns to follower state) once it has committed the log entry of the new configuration. This means that there will be a period of time (while it is committing the new configuration) when the leader is managing a cluster that does not include itself; it replicates log entries but does not count itself in majorities. The leader transition occurs when the new configuration is committed because this is the first point when the new configuration can operate independently (it will always be possible to choose a leader from the new configuration). Before this point, it may be the case that only a server from the old configuration can be elected leader.

## Log compaction
In log compaction, the entire system state is written to a snapshot on stable storage, then the entire log up to that point is discarded.

- Each server takes snapshots independently, covering just the committed entries in its log.
- Most of the work consists of the state machine writing its current state to the snapshot. §7
- A small amount of metadata is included in the snapshot: the last included index is the index of the last entry in the log that the snapshot replaces, and the last included term is the term of this entry.
- To enable cluster membership changes, the snapshot also includes the latest configuration in the log as of last included index.
- Once a server completes writing a snapshot, it may delete all log entries up through the last included index, as well as any prior snapshot.
- The last index and term included in the snapshot must still be identifiable in the log to support the log consistency check. Practically, this may just be a special type of log entry which points to the snapshot and holds the pertinent metadata of the snapshot including the index and term.

### Leader to follower snapshots
Although servers normally take snapshots independently, the leader must occasionally send snapshots to followers that lag behind.

- The cluster leader uses the InstallSnapshot RPC for sending snapshots to followers.
- This happens when the leader has already discarded the next log entry that it needs to send to a follower (due to its own compaction process).
- A follower that has kept up with the leader would already have this entry; however, an exceptionally slow follower or a new server joining the cluster would not. The way to bring such a follower up-to-date is for the leader to send it a snapshot over the network.
- When a follower receives a snapshot with this RPC, it must decide what to do with its existing log entries. Usually the snapshot will contain new information not already in the recipient's log. In this case, the follower discards its entire log and resets the state machine, and then installs the snapshot.
- If instead the follower receives a snapshot that describes a prefix of its log (due to retransmission or by mistake), then log entries coveres by the snapshot are discarded but entries following the snapshot are still valid a must be kept.


### receiver implementation: InstallSnapshot RPC
1. Reply immediately if term < currentTerm.
2. Create new snapshot file if first chunk (offset is 0).
3. Write data into snapshot file at given offset.
4. Reply and wait for more data chunks if done is false.
5. Save snapshot file, discard any existing or partial snapshot with a smaller index.
6. If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply.
7. Discard the entire log.
8. Reset state machine using snapshot contents (and load snapshot's cluster configuration).

## Client interaction
Clients of Barasona send all of their requests to the leader.

- When a client first starts up, it connects to a randomly-chosen server. 
- If the client's first choice is not the leader, that server will reject the client's request and supply information about the most recent leader it has heard from.
- If the leader crashes, client requests will timeout; clients then try again with randomly-chosen servers.