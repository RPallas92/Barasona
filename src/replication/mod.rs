use std::{cmp::min, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{interval, timeout, Interval},
};

use crate::{
    barasona::{AppendEntriesRequest, Entry},
    config::{BarasonaConfig, SnapshotPolicy},
    network::BarasonaNetwork,
    storage::{BarasonaStorage, CurrentSnapshotData},
    AppData, AppDataResponse, NodeId,
};

/// The public handle to a spawned replication stream.
pub(crate) struct ReplicationStream<D: AppData> {
    /// The spawn handle the `ReplicationCore` task.
    pub handle: JoinHandle<()>,
    /// The channel used for communicating with the replication task.
    pub repl_tx: mpsc::UnboundedSender<BarasonaEvent<D>>,
}

impl<D: AppData> ReplicationStream<D> {
    /// Create a new replication stream for the target peer.
    pub(crate) fn new<R: AppDataResponse, N: BarasonaNetwork<D>, S: BarasonaStorage<D, R>>(
        id: NodeId,
        target: NodeId,
        term: u64,
        config: Arc<BarasonaConfig>,
        last_log_index: u64,
        last_log_term: u64,
        commit_index: u64,
        network: Arc<N>,
        storage: Arc<S>,
        replication_tx: mpsc::UnboundedSender<ReplicaEvent<S::Snapshot>>,
    ) -> Self {
        // TODO Ricardo continue here
    }
}

/// A task responsible for sending replication events to a target follower in the Barasona cluster.
///
/// NOTE: we do not stack replication requests to targets because this could result in
/// out-of-order delivery. We always buffer until we receive a success response, then send the
/// next payload from the buffer.
struct ReplicationCore<
    D: AppData,
    R: AppDataResponse,
    N: BarasonaNetwork<D>,
    S: BarasonaStorage<D, R>,
> {
    //////////////////////////////////////////////////////////////////////////
    // Static Fields /////////////////////////////////////////////////////////
    /// The ID of this Barasona node.
    id: NodeId,
    /// The ID of the target Barasona node which replication events are to be sent to.
    target: NodeId,
    /// The current term, which will never change during the lifetime of this task.
    term: u64,
    /// A channel for sending events to the Barasona node.
    barasona_tx: mpsc::UnboundedSender<ReplicaEvent<S::Snapshot>>,
    /// A channel for receiving events from the Raft node.
    barasona_rx: mpsc::UnboundedReceiver<BarasonaEvent<D>>,
    /// The `BarasonaNetwork` interface.
    network: Arc<N>,
    /// The `BarasonaStorage` interface.
    storage: Arc<S>,
    /// The Raft's runtime config.
    config: Arc<BarasonaConfig>,
    /// The configured max payload entries, simply as a usize.
    max_payload_entries: usize,
    marker_r: std::marker::PhantomData<R>, // TODO Ricardo why is this needed?

    //////////////////////////////////////////////////////////////////////////
    // Dynamic Fields ////////////////////////////////////////////////////////
    /// The target state of this replication stream.
    target_state: TargetReplState,

    /// The index of the log entry to most recently be appended to the log by the leader.
    last_log_index: u64,
    /// The index of the highest log entry which is known to be committed in the cluster.
    commit_index: u64,

    /// The index of the next log to send.
    ///
    /// This is initialized to leader's last log index + 1. Per the Barasona protocol spec,
    /// this value may be decremented as new nodes enter the cluster and need to catch-up per the
    /// log consistency check.
    ///
    /// If a follower's log is inconsistent with the leader's, the AppendEntries consistency check
    /// will fail in the next AppendEntries RPC. After a rejection, the leader decrements
    /// `next_index` and retries the AppendEntries RPC. Eventually `next_index` will reach a point
    /// where the leader and follower logs match. When this happens, AppendEntries will succeed,
    /// which removes any conflicting entries in the follower's log and appends entries from the
    /// leader's log (if any). Once AppendEntries succeeds, the follower’s log is consistent with
    /// the leader's, and it will remain that way for the rest of the term.
    ///
    /// This Barasona implementation also uses a _conflict optimization_ pattern for reducing the
    /// number of RPCs which need to be sent back and forth between a peer which is lagging
    /// behind.
    next_index: u64,
    /// The last know index to be successfully replicated on the target.
    ///
    /// This will be initialized to the leader's last_log_index, and will be updated as
    /// replication proceeds.
    match_index: u64,
    /// The term of the last know index to be successfully replicated on the target.
    ///
    /// This will be initialized to the leader's last_log_term, and will be updated as
    /// replication proceeds.
    match_term: u64,

    /// A buffer of data to replicate to the target follower.
    ///
    /// The buffered payload here will be expanded as more replication commands come in from the
    /// Barasona node. Data from this buffer will flow into the `outbound_buffer` in chunks.
    replication_buffer: Vec<Arc<Entry<D>>>,
    /// A buffer of data which is being sent to the follower.
    ///
    /// Data in this buffer comes directly from the `replication_buffer` in chunks, and will
    /// remain here until it is confirmed that the payload has been successfully received by the
    /// target node. This allows for retransmission of payloads in the face of transient errors.
    outbound_buffer: Vec<OutboundEntry<D>>,
    /// The heartbeat interval for ensuring that heartbeats are always delivered in a timely fashion.
    heartbeat: Interval,
    /// The timeout duration for heartbeats.
    heartbeat_timeout: Duration,
}

impl<D: AppData, R: AppDataResponse, N: BarasonaNetwork<D>, S: BarasonaStorage<D, R>>
    ReplicationCore<D, R, N, S>
{
    /// Spawn a new replication task for the target node.
    pub(self) fn spawn(
        id: NodeId,
        target: NodeId,
        term: u64,
        config: Arc<BarasonaConfig>,
        last_log_index: u64,
        last_log_term: u64,
        commit_index: u64,
        network: Arc<N>,
        storage: Arc<S>,
        barasona_tx: mpsc::UnboundedSender<ReplicaEvent<S::Snapshot>>,
    ) -> ReplicationStream<D> {
        let (barasona_rx_tx, barasona_rx) = mpsc::unbounded_channel();
        let heartbeat_timeout = Duration::from_millis(config.heartbeat_interval_ms);
        let max_payload_entries = config.max_payload_entries as usize;
        let this = Self {
            id,
            target,
            term,
            network,
            storage,
            config,
            max_payload_entries,
            marker_r: std::marker::PhantomData,
            target_state: TargetReplState::Lagging,
            last_log_index,
            commit_index,
            next_index: last_log_index + 1,
            match_index: last_log_index,
            match_term: last_log_term,
            barasona_tx,
            barasona_rx,
            heartbeat: interval(heartbeat_timeout),
            heartbeat_timeout,
            replication_buffer: Vec::new(),
            outbound_buffer: Vec::new(),
        };
        let handle = tokio::spawn(this.main());
        ReplicationStream {
            handle,
            repl_tx: barasona_rx_tx,
        }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.id, target=self.target, cluster=%self.config.cluster_name))]
    async fn main(mut self) {
        // TODO Ricardo implement
        loop {}
    }

    /// Send an AppendEntries RPC to the target.
    ///
    /// This request will timeout if no response is received within the
    /// configured heartbeat interval.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn send_append_entries(&mut self) {
        // Attempt to fill the send buffer from the replication buffer
        if self.outbound_buffer.is_empty() {
            let repl_len = self.replication_buffer.len();
            if repl_len > 0 {
                let chunk_size = min(repl_len, self.max_payload_entries);
                self.outbound_buffer.extend(
                    self.replication_buffer
                        .drain(..chunk_size)
                        .map(OutboundEntry::Arc),
                );
            }
        }

        // Build the heartbeat frame to be sent to the follower.
        let payload = AppendEntriesRequest {
            term: self.term,
            leader_id: self.id,
            prev_log_index: self.match_index,
            prev_log_term: self.match_term,
            leader_commit: self.commit_index,
            entries: self
                .outbound_buffer
                .iter()
                .map(|entry| entry.as_ref().clone())
                .collect(),
        };

        // Send the payload.
        let res = match timeout(
            self.heartbeat_timeout,
            self.network.append_entries(self.target, payload),
        )
        .await
        {
            Ok(outer_res) => match outer_res {
                Ok(res) => res,
                Err(err) => {
                    tracing::error!({error=%err}, "error sending AppendEntries RPC to target");
                    return;
                }
            },
            Err(err) => {
                tracing::error!({error=%err}, "timeout while sending AppendEntries RPC to target");
                return;
            }
        };
        let last_index_and_term = self
            .outbound_buffer
            .last()
            .map(|last| (last.as_ref().index, last.as_ref().term));
        self.outbound_buffer.clear(); // Once we've successfully sent a payload of entries, don't send them again.

        // Handle success conditions.
        if res.success {
            tracing::trace!("append entries succedeed");
            // If this was a proper replication event (last index & term were provided), then update state.
            if let Some((index, term)) = last_index_and_term {
                self.next_index = index + 1; // This should always be the next expected index.
                self.match_index = index;
                self.match_term = term;
                let _ = self.barasona_tx.send(ReplicaEvent::UpdateMatchIndex {
                    target: self.target,
                    match_index: index,
                    match_term: term,
                });

                // If running at line rate, and our buffered outbound requests have accumulated too
                // much, we need to purge and transition to a lagging state. The target is not able to
                // replicate data fast enough.
                let is_lagging = self
                    .last_log_index
                    .checked_sub(self.match_index)
                    .map(|diff| diff > self.config.replication_lag_threshold)
                    .unwrap_or(false);
                if is_lagging {
                    self.target_state = TargetReplState::Lagging;
                }
            }
            return;
        }

        // Replication was not successful, if a newer term has been returned, revert to follower.
        if res.term > self.term {
            tracing::trace!({ res.term }, "append entries failed, reverting to follower");
            let _ = self.barasona_tx.send(ReplicaEvent::RevertToFollower {
                target: self.target,
                term: res.term,
            });
            self.target_state = TargetReplState::Shutdown;
            return;
        }

        // Replication was not successful, handle conflict optimization record, else decrement `next_index`.
        if let Some(conflict) = res.conflict_opt {
            tracing::trace!({?conflict, res.term}, "append entries failed, handling conflict opt");
            // If the returned conflict opt index is greater than last_log_index, then this is a
            // logical error, and no action should be taken. This represents a replication failure.
            if conflict.index > self.last_log_index {
                return;
            }
            self.next_index = conflict.index + 1;
            self.match_index = conflict.index;
            self.match_term = conflict.term;

            // If conflict index is 0, we will not be able to fetch that index from storage because
            // it will never exist. So instead, we just return, and accept the conflict data.
            if conflict.index == 0 {
                self.target_state = TargetReplState::Lagging;
                let _ = self.barasona_tx.send(ReplicaEvent::UpdateMatchIndex {
                    target: self.target,
                    match_index: self.match_index,
                    match_term: self.match_term,
                });
                return;
            }

            // Fetch the entry at conflict index and use the term specified there.
            match self
                .storage
                .get_log_entries(conflict.index, conflict.index + 1)
                .await
                .map(|entries| entries.get(0).map(|entry| entry.term))
            {
                Ok(Some(term)) => {
                    self.match_term = term; // If we have the specified log, ensure we use its term.
                }
                Ok(None) => {
                    // This condition would only ever be reached if the log has been removed due to
                    // log compaction (barring critical storage failure), so transition to snapshotting.
                    self.target_state = TargetReplState::Snapshotting;
                    let _ = self.barasona_tx.send(ReplicaEvent::UpdateMatchIndex {
                        target: self.target,
                        match_index: self.match_index,
                        match_term: self.match_term,
                    });
                    return;
                }
                Err(err) => {
                    tracing::error!({error=%err}, "error fetching log entry due to returned AppendEntries RPC conflict_opt");
                    let _ = self.barasona_tx.send(ReplicaEvent::Shutdown);
                    self.target_state = TargetReplState::Shutdown;
                    return;
                }
            };

            let _ = self.barasona_tx.send(ReplicaEvent::UpdateMatchIndex {
                target: self.target,
                match_index: self.match_index,
                match_term: self.match_term,
            });
            match &self.config.snapshot_policy {
                SnapshotPolicy::LogsSinceLast(threshold) => {
                    let diff = self.last_log_index - conflict.index; // NOTE WELL: underflow is guarded against above.
                    if &diff >= threshold {
                        // Follower is far behind and needs to receive an InstallSnapshot RPC.
                        self.target_state = TargetReplState::Snapshotting;
                        return;
                    }
                    // Follower is behind, but not too far behind to receive an InstallSnapshot RPC.
                    self.target_state = TargetReplState::Lagging;
                    return;
                }
            }
        }
    }
}

/// A type which wraps two possible forms of an outbound entry for replication.
enum OutboundEntry<D: AppData> {
    /// An entry owned by an Arc, hot off the replication stream from the Barasona leader.
    Arc(Arc<Entry<D>>),
    /// An entry which was fetched directly from storage.
    Raw(Entry<D>),
}

impl<D: AppData> AsRef<Entry<D>> for OutboundEntry<D> {
    fn as_ref(&self) -> &Entry<D> {
        match self {
            Self::Arc(inner) => inner.as_ref(),
            Self::Raw(inner) => inner,
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// The state of the replication stream.
#[derive(Eq, PartialEq)]
enum TargetReplState {
    /// The replication stream is running at line rate.
    LineRate,
    /// The replication stream is lagging behind.
    Lagging,
    /// The replication stream is streaming a snapshot over to the target node.
    Snapshotting,
    /// The replication stream is shutting down.
    Shutdown,
}

/// An event from the Barasona node.
pub(crate) enum BarasonaEvent<D: AppData> {
    Replicate {
        /// The new entry which needs to be replicated.
        ///
        /// This entry will always be the most recent entry to have been appended to the log, so its
        /// index is the new last_log_index value.
        entry: Arc<Entry<D>>,
        /// The index of the highest log entry which is known to be committed in the cluster.
        commit_index: u64,
    },
    /// A message from Barasona indicating a new commit index value.
    UpdateCommitIndex {
        /// The index of the highest log entry which is known to be committed in the cluster.
        commit_index: u64,
    },
    Terminate,
}

/// An event coming from a replication stream.
pub(crate) enum ReplicaEvent<S>
where
    S: AsyncRead + AsyncSeek + Send + Unpin + 'static,
{
    /// An event representing an update to the replication rate of a replication stream.
    RateUpdate {
        /// The ID of the Barasona node to which this event relates.
        target: NodeId,
        /// A flag indicating if the corresponding target node is replicating at line rate.
        ///
        /// When replicating at line rate, the replication stream will receive log entires to
        /// replicate as soon as they are ready. When not running at line rate, the Barasona node will
        /// only send over metadata without entries to replicate.
        is_line_rate: bool,
    },
    /// An event from a replication stream which updates the target node's match index.
    UpdateMatchIndex {
        /// The ID of the target node for which the match index is to be updated.
        target: NodeId,
        /// The index of the most recent log known to have been successfully replicated on the target.
        match_index: u64,
        /// The term of the most recent log known to have been successfully replicated on the target.
        match_term: u64,
    },
    /// An event indicating that the Baarasona node needs to revert to follower state.
    RevertToFollower {
        /// The ID of the target node from which the new term was observed.
        target: NodeId,
        /// The new term observed.
        term: u64,
    },
    /// An event from a replication stream requesting snapshot info.
    NeedsSnapshot {
        /// The ID of the target node from which the event was sent.
        target: NodeId,
        /// The response channel for delivering the snapshot data.
        tx: oneshot::Sender<CurrentSnapshotData<S>>,
    },
    /// Some critical error has taken place, and Barasona needs to shutdown.
    Shutdown,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////
