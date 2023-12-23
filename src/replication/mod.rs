use std::{sync::Arc, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{interval, Interval},
};

use crate::{
    barasona::Entry,
    config::BarasonaConfig,
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
        loop {}
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
