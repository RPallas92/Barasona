//! Barasona runtime configuration.

use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use crate::error::ConfigError;

/// Default election timeout minimum, in milliseconds.
pub const DEFAULT_ELECTION_TIMEOUT_MIN_MS: u64 = 150;
/// Default election timeout maximum, in milliseconds.
pub const DEFAULT_ELECTION_TIMEOUT_MAX_MS: u64 = 300;
/// Default heartbeat interval, in milliseconds.
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 50;
/// Default threshold for when to trigger a snapshot.
pub const DEFAULT_LOGS_SINCE_LAST: u64 = 5000;
/// Default maximum number of entries per replication payload.
pub const DEFAULT_MAX_PAYLOAD_ENTRIES: u64 = 300;
/// Default replication lag threshold.
pub const DEFAULT_REPLICATION_LAG_THRESHOLD: u64 = 1000;
/// Default snapshot chunksize, in MiB.
pub const DEFAULT_SNAPSHOT_CHUNKSIZE_MIB: u64 = 1024 * 1024 * 3;

/// Log compaction and snapshot policy.
///
/// This governs when periodic snapshots will be taken, and also governs the conditions which
/// would cause a leader to send an `InstallSnapshot` RPC to a follower based on replication lag.
///
/// Additional policies may become available in the future.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SnapshotPolicy {
    /// A snapshot will be generated once the log has grown the specified number of logs since
    /// the last snapshot.
    LogsSinceLast(u64),
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        SnapshotPolicy::LogsSinceLast(DEFAULT_LOGS_SINCE_LAST)
    }
}

/// The runtime configuration for a Barasona node.
///
/// The default values used by this type should generally work well for Barasona clusters which will
/// be running with nodes in multiple datacenter availability zones with low latency between
/// zones. These values should typically be made configurable from the perspective of the
/// application which is being built on top of Barasona.
///
/// When building the Barasona configuration for your application, take into account this inequality:
/// `broadcastTime ≪ electionTimeout ≪ MTBF`.
///
/// > In this inequality `broadcastTime` is the average time it takes a server to send RPCs in
/// > parallel to every server in the cluster and receive their responses; `electionTimeout` is the
/// > election timeout; and `MTBF` is the average time between failures for
/// > a single server. The broadcast time should be an order of magnitude less than the election
/// > timeout so that leaders can reliably send the heartbeat messages required to keep followers
/// > from starting elections; given the randomized approach used for election timeouts, this
/// > inequality also makes split votes unlikely. The election timeout should be a few orders of
/// > magnitude less than `MTBF` so that the system makes steady progress. When the leader crashes,
/// > the system will be unavailable for roughly the election timeout; we would like this to
/// > represent only a small fraction of overall time.
///
/// What does all of this mean? Simply keep your election timeout settings high enough that the
/// performance of your network will not cause election timeouts, but don't keep it so high that
/// a real leader crash would cause prolonged downtime.
#[derive(Debug, Serialize, Deserialize)]
pub struct BarasonaConfig {
    /// The application specific name of this Barasona cluster.
    ///
    /// Only used for observability.
    pub cluster_name: String,
    /// The minimum election timeout in milliseconds.
    pub election_timeout_min_ms: u64,
    /// The maximum election timeout in milliseconds.
    pub election_timeout_max_ms: u64,
    /// The heartbeat interval in milliseconds at which leaders will send heartbeats to followers.
    ///
    /// Defaults to 50 milliseconds.
    ///
    /// **NOTE WELL:** it is very important that this value be greater than the amount of time
    /// it will take on average for heartbeat frames to be sent between nodes. No data processing
    /// is performed for heartbeats, so the main item of concern here is network latency. This
    /// value is also used as the default timeout for sending heartbeats.
    pub heartbeat_interval_ms: u64,
    /// The maximum number of entries per payload allowed to be transmitted during replication.
    ///
    /// When configuring this value, it is important to note that setting this value too low could
    /// cause sub-optimal performance. This will primarily impact the speed at which slow nodes,
    /// nodes which have been offline, or nodes which are new to the cluster, are brought
    /// up-to-speed. If this is too low, it will take longer for the nodes to be brought up to
    /// consistency with the rest of the cluster.
    pub max_payload_entries: u64,
    /// The distance behind in log replication a follower must fall before it is considered "lagging".
    ///
    /// This configuration parameter controls replication streams from the leader to followers in
    /// the cluster. Once a replication stream is considered lagging, it will stop buffering
    /// entries being replicated, and instead will fetch entries directly from the log until it is
    /// up-to-speed, at which time it will transition out of "lagging" state back into "line-rate" state.
    pub replication_lag_threshold: u64,
    /// The snapshot policy to use for a Barasona node.
    pub snapshot_policy: SnapshotPolicy,
    /// The maximum snapshot chunk size allowed when transmitting snapshots (in bytes).
    ///
    /// Defaults to 3Mib.
    pub snapshot_max_chunk_size_mib: u64,
}

impl BarasonaConfig {
    /// Returns a builder object. Call the `build` method to get a valid BarasonaConfig instance.
    /// The builder will validate the config.
    pub fn builder(cluster_name: String) -> BarasonaConfigBuilder {
        BarasonaConfigBuilder {
            cluster_name,
            election_timeout_min_ms: None,
            election_timeout_max_ms: None,
            heartbeat_interval_ms: None,
            max_payload_entries: None,
            replication_lag_threshold: None,
            snapshot_policy: None,
            snapshot_max_chunk_size_mib: None,
        }
    }

    /// Generate a new random election timeout within the configured min & max.
    pub fn new_rand_election_timeout_ms(&self) -> u64 {
        thread_rng().gen_range(self.election_timeout_min_ms..=self.election_timeout_max_ms)
    }
}

/// A configuration builder to ensure that runtime config is valid.
#[derive(Debug, Serialize, Deserialize)]
pub struct BarasonaConfigBuilder {
    /// The application specific name of this Barasona cluster.
    pub cluster_name: String,
    /// The minimum election timeout, in milliseconds.
    pub election_timeout_min_ms: Option<u64>,
    /// The maximum election timeout, in milliseconds.
    pub election_timeout_max_ms: Option<u64>,
    /// The interval at which leaders will send heartbeats to followers to avoid election timeout.
    pub heartbeat_interval_ms: Option<u64>,
    /// The maximum number of entries per payload allowed to be transmitted during replication.
    pub max_payload_entries: Option<u64>,
    /// The distance behind in log replication a follower must fall before it is considered "lagging".
    pub replication_lag_threshold: Option<u64>,
    /// The snapshot policy.
    pub snapshot_policy: Option<SnapshotPolicy>,
    /// The maximum snapshot chunk size.
    pub snapshot_max_chunk_size_mib: Option<u64>,
}

impl BarasonaConfigBuilder {
    /// Set the desired value for `election_timeout_min_ms`.
    pub fn election_timeout_min_ms(mut self, val: u64) -> Self {
        self.election_timeout_min_ms = Some(val);
        self
    }

    /// Set the desired value for `election_timeout_max_ms`
    pub fn election_timeout_max_ms(mut self, val: u64) -> Self {
        self.election_timeout_max_ms = Some(val);
        self
    }

    /// Set the desired value for `heartbeat_interval_ms`
    pub fn heartbeat_interval_ms(mut self, val: u64) -> Self {
        self.heartbeat_interval_ms = Some(val);
        self
    }

    /// Set the desired value for `max_payload_entries`
    pub fn max_payload_entries(mut self, val: u64) -> Self {
        self.max_payload_entries = Some(val);
        self
    }

    /// Set the desired value for `replication_lag_threshold`
    pub fn replication_lag_threshold(mut self, val: u64) -> Self {
        self.replication_lag_threshold = Some(val);
        self
    }

    /// Set the desired value for `snapshot_policy`
    pub fn snapshot_policy(mut self, val: SnapshotPolicy) -> Self {
        self.snapshot_policy = Some(val);
        self
    }

    /// Set the desired value for `snapshot_max_chunk_size_mib`
    pub fn snapshot_max_chunk_size_mib(mut self, val: u64) -> Self {
        self.snapshot_max_chunk_size_mib = Some(val);
        self
    }

    /// Validate the state of this builder and produce a new `Config` instance if valid.
    pub fn build(self) -> Result<BarasonaConfig, ConfigError> {
        let election_timeout_min_ms = self
            .election_timeout_min_ms
            .unwrap_or(DEFAULT_ELECTION_TIMEOUT_MIN_MS);
        let election_timeout_max_ms = self
            .election_timeout_max_ms
            .unwrap_or(DEFAULT_ELECTION_TIMEOUT_MAX_MS);

        let heartbeat_interval_ms = self
            .heartbeat_interval_ms
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let max_payload_entries = self
            .max_payload_entries
            .unwrap_or(DEFAULT_MAX_PAYLOAD_ENTRIES);

        let replication_lag_threshold = self
            .replication_lag_threshold
            .unwrap_or(DEFAULT_REPLICATION_LAG_THRESHOLD);
        let snapshot_policy = self.snapshot_policy.unwrap_or_else(SnapshotPolicy::default);
        let snapshot_max_chunk_size_mib = self
            .snapshot_max_chunk_size_mib
            .unwrap_or(DEFAULT_SNAPSHOT_CHUNKSIZE_MIB);

        if election_timeout_min_ms >= election_timeout_max_ms {
            return Err(ConfigError::InvalidElectionTimeoutMinMax);
        }
        if max_payload_entries == 0 {
            return Err(ConfigError::MaxPayloadEntriesTooSmall);
        }

        Ok(BarasonaConfig {
            cluster_name: self.cluster_name,
            election_timeout_min_ms,
            election_timeout_max_ms,
            heartbeat_interval_ms,
            max_payload_entries,
            replication_lag_threshold,
            snapshot_policy,
            snapshot_max_chunk_size_mib,
        })
    }
}
