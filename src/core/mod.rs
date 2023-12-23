use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::stream::{AbortHandle, FuturesOrdered};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    barasona::Entry,
    barasona::{BarasonaMsg, MembershipConfig},
    config::BarasonaConfig,
    error::{BarasonaError, BarasonaResult},
    metrics::BarasonaMetrics,
    network::BarasonaNetwork,
    storage::BarasonaStorage,
    AppData, AppDataResponse, NodeId,
};

/// The core type implementing the Barasona protocol
pub struct BarasonaCore<D, R, N, S>
where
    D: AppData,
    R: AppDataResponse,
    N: BarasonaNetwork<D>,
    S: BarasonaStorage<D, R>,
{
    /// This node's ID.
    id: NodeId,
    /// This node's runtime config
    config: Arc<BarasonaConfig>,
    /// The cluster's current membership configuration.
    membership: MembershipConfig,
    /// The `BarasonaNetwork` implementation.
    network: Arc<N>,
    /// The `BarasonaStorage` implementation.
    storage: Arc<S>,
    /// The target state of a node
    target_state: State,
    /// The index of the highest log entry known to be committed cluster-wide.
    ///
    /// The definition of a committed log is that the leader which has created the log has
    /// successfully replicated the log to a majority of the cluster. This value is updated via
    /// AppendEntries RPC from the leader, or if a node is the leader, it will update this value
    /// as new entries have been successfully replicated to a majority of the cluster.
    ///
    /// Is initialized to 0, and increases monotonically. This is always based on the leader's
    /// commit index which is communicated to other members via the AppendEntries protocol.
    commit_index: u64,
    /// The current term.
    ///
    /// Is initialized to 0 on first boot, and increases monotonically. This is normally based on
    /// the leader's term which is communicated to other members via the AppendEntries protocol,
    /// but this may also be incremented when a follower becomes a candidate.
    current_term: u64,
    /// The ID of the current leader of the Barasona cluster.
    current_leader: Option<NodeId>,
    /// The ID of the candidate which received this node's vote for the current term.
    ///
    /// Each server will vote for at most one candidate in a given term, on a
    /// first-come-first-served basis.
    voted_for: Option<NodeId>,
    /// The index of the last entry to be appended to the log.
    last_log_index: u64,
    /// The term of the last entry to be appended to the log.
    last_log_term: u64,
    /// The node's current snapshot state.
    snapshot_state: Option<SnapshotState<S::Snapshot>>,
    /// The index of the current snapshot, if a snapshot exists.
    ///
    /// This is primarily used in making a determination on when a compaction job needs to be triggered.
    snapshot_index: u64,
    /// A cache of entries which are waiting to be replicated to the state machine.
    ///
    /// It is important to note that this cache must only be populated from the AppendEntries RPC
    /// handler, as these values must only ever represent the entries which have been sent from
    /// the current cluster leader.
    ///
    /// Whenever there is a leadership change, this cache will be cleared.
    entries_cache: BTreeMap<u64, Entry<D>>,
    /// The stream of join handles from state machine replication tasks. There will only ever be
    /// a maximum of 1 element at a time.
    ///
    /// This abstraction is needed to ensure that replicating to the state machine does not block
    /// the AppendEntries RPC flow, and to ensure that we have a smooth transition to becoming
    /// leader without concern over duplicate application of entries to the state machine.
    replicate_to_sm_handle: FuturesOrdered<JoinHandle<anyhow::Result<Option<u64>>>>,
    /// A bool indicating if this system has performed its initial replication of
    /// outstanding entries to the state machine.
    has_completed_initial_replication_to_sm: bool,
    /// The last time a heartbeat was received.
    last_heartbeat: Option<Instant>,
    /// The duration until the next election timeout.
    next_election_timeout: Option<Instant>,
    /// The sender channel for notifying the Barasona node about snapshot updates or completion of log compaction.
    tx_compaction: mpsc::Sender<SnapshotUpdate>,
    /// The receiver channel for receiving notifications about snapshot updates or completion of log compaction.
    rx_compaction: mpsc::Receiver<SnapshotUpdate>,
    /// An unbounded receiver channel for receiving Barasona messages from the external API.
    /// Used for communication with the external world, receiving commands or queries that
    /// need to be processed by the Barasona node.
    rx_api: mpsc::UnboundedReceiver<BarasonaMsg<D, R>>,
    /// sender channel for reporting Barasona metrics. Used to send metrics data, such as the current state,
    /// term, leader, etc., to an external entity that may be monitoring or tracking the Barasona node's performance
    tx_metrics: watch::Sender<BarasonaMetrics>,
    /// A oneshot receiver channel used to signal the Barasona node to shut down. The Barasona node may be
    /// gracefully shut down when this channel receives a signal.
    rx_shutdown: oneshot::Receiver<()>,
}

impl<D: AppData, R: AppDataResponse, N: BarasonaNetwork<D>, S: BarasonaStorage<D, R>>
    BarasonaCore<D, R, N, S>
{
    pub(crate) fn spawn(
        id: NodeId,
        config: Arc<BarasonaConfig>,
        network: Arc<N>,
        storage: Arc<S>,
        rx_api: mpsc::UnboundedReceiver<BarasonaMsg<D, R>>,
        tx_metrics: watch::Sender<BarasonaMetrics>,
        rx_shutdown: oneshot::Receiver<()>,
    ) -> JoinHandle<BarasonaResult<()>> {
        let membership = MembershipConfig::new_initial(id);
        let (tx_compaction, rx_compaction) = mpsc::channel(1);
        let this = Self {
            id,
            config,
            membership,
            network,
            storage,
            target_state: State::Follower,
            commit_index: 0,
            current_term: 0,
            current_leader: None,
            voted_for: None,
            last_log_index: 0,
            last_log_term: 0,
            snapshot_state: None,
            snapshot_index: 0,
            entries_cache: Default::default(),
            replicate_to_sm_handle: FuturesOrdered::new(),
            has_completed_initial_replication_to_sm: false,
            last_heartbeat: None,
            next_election_timeout: None,
            tx_compaction,
            rx_compaction,
            rx_api,
            tx_metrics,
            rx_shutdown,
        };

        tokio::spawn(this.main())
    }

    /// The main loop of the Barasona protocol.
    #[tracing::instrument(level="trace", skip(self), fields(id=self.id, cluster=%self.config.cluster_name))]
    async fn main(mut self) -> BarasonaResult<()> {
        tracing::trace!("Barasona node is initializing");
        let state = self
            .storage
            .get_initial_state()
            .await
            .map_err(|err| self.map_fatal_storage_error(err))?;
        self.last_log_index = state.last_log_index;
        self.last_log_term = state.last_log_term;
        self.current_term = state.persistent_state.current_term;
        self.voted_for = state.persistent_state.voted_for;
        self.membership = state.membership;
        // NOTE: this is repeated here for clarity. It is unsafe to initialize the node's commit
        // index to any other value. The commit index must be determined by a leader after
        // successfully committing a new log to the cluster.
        self.commit_index = 0;

        // Fetch the most recent snapshot in the system.
        if let Some(snapshot) = self
            .storage
            .get_current_snapshot()
            .await
            .map_err(|err| self.map_fatal_storage_error(err))?
        {
            self.snapshot_index = snapshot.index;
        }

        // Set initial state based on state recovered from disk.
        let is_only_configured_member =
            self.membership.members.len() == 1 && self.membership.contains(&self.id);

        // If this is the only configured member and there is live state, then this is
        // a single-node cluster. Become leader.
        // TODO Ricardo why min_value if last log index should be 0 if the first one, right?
        if is_only_configured_member && self.last_log_index != u64::min_value() {
            self.target_state = State::Leader;
        }
        // Else if there are other members, that can only mean that state was recovered. Become follower.
        // Here we use a 30 second overhead on the initial next_election_timeout. This is because we need
        // to ensure that restarted nodes don't disrupt a stable cluster by timing out and driving up their
        // term before network communication is established.
        else if !is_only_configured_member && self.membership.contains(&self.id) {
            self.target_state = State::Follower;
            let inst = Instant::now()
                + Duration::from_secs(30)
                + Duration::from_millis(self.config.new_rand_election_timeout_ms());
            self.next_election_timeout = Some(inst);
        }
        // Else, for any other condition, stay non-voter.
        else {
            self.target_state = State::NonVoter;
        }

        // TODO Ricardo continue here
        // TODO Ricardo I first need to implement the replication module as it is called by this module

        loop {}
    }

    /// Update core's target state, ensuring all invariants are upheld.
    #[tracing::instrument(level = "trace", skip(self))]
    fn set_target_state(&mut self, target_state: State) {
        if target_state == State::Follower && !self.membership.contains(&self.id) {
            self.target_state = State::NonVoter;
        } else {
            self.target_state = target_state;
        }
    }

    /// Trigger the shutdown sequence due to a non-recoverable error from the storage layer.
    ///
    /// This method assumes that a storage error observed here is non-recoverable. As such, the
    /// Barasona node will be instructed to stop. If such behavior is not needed, then don't use this
    /// interface.
    #[tracing::instrument(level = "trace", skip(self))]
    fn map_fatal_storage_error(&mut self, err: anyhow::Error) -> BarasonaError {
        tracing::error!({error=%err, id=self.id}, "fatal storage error, shutting down");
        self.set_target_state(State::Shutdown);
        BarasonaError::BarasonaStorage(err)
    }
}

/// The current snapshot state of the Barasona node.
pub(self) enum SnapshotState<S> {
    /// The Barasona node is compacting itself.
    Snapshotting {
        /// A handle to abort the compaction process early if needed.
        handle: AbortHandle,
        /// A sender for notifiying any other tasks of the completion of this compaction.
        sender: broadcast::Sender<u64>,
    },
    /// The Barasona node is streaming in a snapshot from the leader.
    Streaming {
        /// The offset of the last byte written to the snapshot.
        offset: u64,
        /// The ID of the snapshot being written.
        id: String,
        /// A handle to the snapshot writer.
        snapshot: Box<S>,
    },
}

/// An update on a snapshot creation process.
#[derive(Debug)]
pub(self) enum SnapshotUpdate {
    /// Snapshot creation has finished successfully and covers the given index.
    SnapshotComplete(u64),
    /// Snapshot creation failed.
    SnapshotFailed,
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/// All possible states of a Barasona node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    /// The node is completely passive; replicating entries, but neither voting nor timing out.
    NonVoter,
    /// The node is replicating logs from the leader.
    Follower,
    /// The node is campaigning to become the cluster leader.
    Candidate,
    /// The node is the Barasona cluster leader.
    Leader,
    /// The node is shutting down.
    Shutdown,
}

impl State {
    /// Check if currently in non-voter state.
    pub fn is_non_voter(&self) -> bool {
        matches!(self, Self::NonVoter)
    }

    /// Check if currently in follower state.
    pub fn is_follower(&self) -> bool {
        matches!(self, Self::Follower)
    }

    /// Check if currently in candidate state.
    pub fn is_candidate(&self) -> bool {
        matches!(self, Self::Candidate)
    }

    /// Check if currently in leader state.
    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leader)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
