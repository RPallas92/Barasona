//! The Barasona network interface.
use crate::{
    barasona::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    AppData, NodeId,
};
use anyhow::Result;
use async_trait::async_trait;

/// A trait defining the interface for a Barasona network between cluster members.
#[async_trait]
pub trait BarasonaNetwork<D>: Send + Sync + 'static
where
    D: AppData,
{
    /// Send an AppendEntries RPC to the target Barasona node.
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<D>,
    ) -> Result<AppendEntriesResponse>;

    /// Send an InstallSnapshot RPC to the target Barasona node.
    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse>;

    /// Send an RequestVote RPC to the target Barasona node.
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse>;
}
