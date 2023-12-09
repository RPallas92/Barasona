//! Barasona metrics for observability.
//!
//! Applications may use this data in whatever way is needed. The obvious use cases are to expose
//! these metrics to a metrics collection system like Prometheus. Applications may also
//! use this data to trigger events.
//!
//! Metrics are observed on a running Barasona node via the `Barasona::metrics()` method, which will
//! return a stream of metrics.

use serde::{Deserialize, Serialize};

use crate::{barasona::MembershipConfig, core::State, NodeId};

/// A set of metrics describing the current state of a Barasona node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarasonaMetrics {
    /// The ID of the Barasona node.
    pub id: NodeId,
    /// The state of the Barasona node.
    pub state: State,
    /// The current term of the a node.
    pub current_term: u64,
    /// The last log index to be appended to this Barasona node's log.
    pub last_log_index: u64,
    /// The current cluster leader.
    pub current_leader: Option<NodeId>,
    /// The current membership config of the cluster.
    pub membership_config: MembershipConfig,
}

impl BarasonaMetrics {
    pub(crate) fn new_initial(id: NodeId) -> Self {
        let membership_config = MembershipConfig::new_initial(id);
        Self {
            id,
            state: State::Follower,
            current_term: 0,
            last_log_index: 0,
            current_leader: None,
            membership_config,
        }
    }
}
