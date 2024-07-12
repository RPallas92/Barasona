use anyhow::anyhow;
use std::{sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{sync::oneshot, time::timeout};

use crate::{
    barasona::{
        AppendEntriesRequest, ClientReadResponseTx, ClientWriteRequest, ClientWriteResponse,
        ClientWriteResponseTx, Entry, EntryPayload,
    },
    core::{LeaderState, State},
    error::{BarasonaError, BarasonaResult, ClientReadError, ClientWriteError},
    network::BarasonaNetwork,
    replication::BarasonaEvent,
    storage::BarasonaStorage,
    AppData, AppDataResponse,
};

/// A wrapper around a ClientRequest which has been transformed into an Entry, along with its response channel.
pub(super) struct ClientRequestEntry<D: AppData> {
    /// The Arc'd entry of the ClientRequest.
    ///
    /// This value is Arc'd so that it may be sent across thread boundaries for replication
    /// without having to clone the data payload itself.
    pub entry: Arc<Entry<D>>,
    /// The response channel for the request.
    pub tx: ClientOrInternalResponseTx<D>,
}

impl<D: AppData> ClientRequestEntry<D> {
    /// Create a new instance from the raw components of a client request.
    pub(crate) fn from_entry<T: Into<ClientOrInternalResponseTx<D>>>(
        entry: Entry<D>,
        tx: T,
    ) -> Self {
        Self {
            entry: Arc::new(entry),
            tx: tx.into(),
        }
    }
}

/// An enum type wrapping either a client response channel or an internal Barasona response channel.
#[derive(derive_more::From)]
pub enum ClientOrInternalResponseTx<D: AppData> {
    Client(ClientWriteResponseTx<D>),
    Internal(oneshot::Sender<Result<u64, BarasonaError>>),
}

impl<'a, D: AppData, R: AppDataResponse, N: BarasonaNetwork<D>, S: BarasonaStorage<D, R>>
    LeaderState<'a, D, R, N, S>
{
    /// Commit the initial entry which new leaders are obligated to create when first coming to power.
    #[tracing::instrument(level = "trace", skip(self))]
    pub(super) async fn commit_initial_leader_entry(&mut self) -> BarasonaResult<()> {
        // If the cluster has just formed, and the current index is 0, then commit the current
        // config, else a blank payload.
        let req: ClientWriteRequest<D> = if self.core.last_log_index == 0 {
            ClientWriteRequest::new_config(self.core.membership.clone())
        } else {
            ClientWriteRequest::new_blank_payload()
        };

        // Check to see if we have any config change logs newer than our commit index. If so, then
        // we need to drive the commitment of the config change to the cluster.
        let mut pending_config = None; // The inner bool represents `is_in_joint_consensus`.
        if self.core.last_log_index > self.core.commit_index {
            let (state_logs_start, stale_logs_end) =
                (self.core.commit_index + 1, self.core.last_log_index + 1);
            pending_config = self
                .core
                .storage
                .get_log_entries(state_logs_start, stale_logs_end)
                .await
                .map_err(|err| self.core.map_fatal_storage_error(err))?
                .iter()
                .rev()
                .filter_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.is_in_joint_consensus()),
                    EntryPayload::SnapshotPointer(cfg) => {
                        Some(cfg.membership.is_in_joint_consensus())
                    }
                    _ => None,
                })
                .next();
        }

        // Commit the initial payload to the cluster.
        let (tx_entry_committed, rx_entry_committed) = oneshot::channel();
        let entry = self.append_payload_to_log(req.entry).await?;
        self.core.last_log_term = self.core.current_term; // This only ever needs to be updated once per term.
        let cr_entry = ClientRequestEntry::from_entry(entry, tx_entry_committed);
        self.replicate_client_request(cr_entry).await;
        self.core.report_metrics();

        // Setup any callbacks needed for responding to commitment of a pending config.
        if let Some(is_in_joint_consensus) = pending_config {
            if is_in_joint_consensus {
                self.joint_consensus_cb.push_back(rx_entry_committed);
            } else {
                self.uniform_consensus_cb.push_back(rx_entry_committed);
            }
        }
        Ok(())
    }

    /// Handle client read requests.
    ///
    /// Spawn requests to all members of the cluster, include members being added in joint
    /// consensus. Each request will have a timeout, and we respond once we have a majority
    /// agreement from each config group. Most of the time, we will have a single uniform
    /// config group.
    ///
    /// From the spec:
    /// Second, a leader must check whether it has been deposed before processing a read-only
    /// request (its information may be stale if a more recent leader has been elected). Barasona
    /// handles this by having the leader exchange heartbeat messages with a majority of the
    /// cluster before responding to read-only requests.
    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(super) async fn handle_client_read_request(&mut self, tx: ClientReadResponseTx) {
        // Setup sentinel values to track when we've received majority confirmation of leadership.
        let mut c0_confirmed = 0usize;
        let len_members = self.core.membership.members.len(); // Will never be zero, as we don't allow it when proposing config changes.
        let c0_needed = if len_members % 2 == 0 {
            (len_members / 2) - 1
        } else {
            len_members / 2
        };

        let mut c1_confirmed = 0usize;
        let mut c1_needed = 0usize;
        if let Some(joint_members) = self.core.membership.members_after_consensus {
            let len = joint_members.len(); // Will never be zero, as we don't allow it when proposing config changes.
            c1_needed = if (len % 2) == 0 {
                (len / 2) - 1
            } else {
                len / 2
            };
        }

        // Increment confirmations for self, including post-joint-consensus config if applicable.
        c0_confirmed += 1;
        let is_in_post_join_consensus_config = self
            .core
            .membership
            .members_after_consensus
            .as_ref()
            .map(|members| members.contains(&self.core.id))
            .unwrap_or(false);

        if is_in_post_join_consensus_config {
            c1_confirmed += 1;
        }

        // If we already have all needed confirmations — which would be the case for singlenode
        // clusters — then respond.
        if c0_confirmed >= c0_needed && c1_confirmed >= c1_needed {
            let _ = tx.send(Ok(()));
            return;
        }

        // Spawn parallel requests, all with the standard timeout for heartbeats.
        let mut pending = FuturesUnordered::new();
        for (id, node) in self.nodes.iter() {
            let rpc = AppendEntriesRequest {
                term: self.core.current_term,
                leader_id: self.core.id,
                prev_log_index: node.match_index,
                prev_log_term: node.match_term,
                entries: vec![],
                leader_commit: self.core.commit_index,
            };

            let target = *id;
            let network = self.core.network.clone();
            let ttl = Duration::from_millis(self.core.config.heartbeat_interval_ms);
            let task = tokio::spawn(async move {
                match timeout(ttl, network.append_entries(target, rpc)).await {
                    Ok(Ok(data)) => Ok((target, data)),
                    Ok(Err(err)) => Err((target, err)),
                    Err(_timeout) => Err((
                        target,
                        anyhow!("timeout waiting for leadership confirmation"),
                    )),
                }
            });
            pending.push(task);
        }

        // Handle responses as they return
        while let Some(res) = pending.next().await {
            let (target, data) = match res {
                Ok(Ok(res)) => res,
                Ok(Err((target, err))) => {
                    tracing::error!({target, error=%err}, "timeout while confirming leadership for read request");
                    continue;
                }
                Err(err) => {
                    tracing::error!("{}", err);
                    continue;
                }
            };

            // If we receive a response with a greater term, then revert to follower and abort this request.
            if data.term != self.core.current_term {
                self.core.update_current_term(data.term, None);
                self.core.set_target_state(State::Follower);
            }

            // If the term is the same, then it means we are still the leader.
            if self.core.membership.members.contains(&target) {
                c0_confirmed += 1;
            }
            if self
                .core
                .membership
                .members_after_consensus
                .as_ref()
                .map(|members| members.contains(&target))
                .unwrap_or(false)
            {
                c1_confirmed += 1;
            }

            if c0_confirmed >= c0_needed && c1_confirmed >= c1_needed {
                let _ = tx.send(Ok(()));
                return;
            }
        }

        // If we've hit this location, then we've failed to gather needed confirmations due to
        // request failures.
        let _ = tx.send(Err(ClientReadError::BarasonaError(
            BarasonaError::BarasonaNetwork(anyhow!(
                "too many requests failed, could not confirm leadership"
            )),
        )));
    }

    /// Handle client write requests.
    #[tracing::instrument(level = "trace", skip(self, rpc, tx))]
    pub(super) async fn handle_client_write_request(
        &mut self,
        rpc: ClientWriteRequest<D>,
        tx: ClientWriteResponseTx<D>,
    ) {
        let entry = match self.append_payload_to_log(rpc.entry).await {
            Ok(entry) => ClientRequestEntry::from_entry(entry, tx),
            Err(err) => {
                let _ = tx.send(Err(ClientWriteError::BarasonaError(err)));
                return;
            }
        };

        self.replicate_client_request(entry).await;
    }

    /// Transform the given payload into an entry, assign an index and term, and append the entry to the log.
    #[tracing::instrument(level = "trace", skip(self, payload))]
    pub(super) async fn append_payload_to_log(
        &mut self,
        payload: EntryPayload<D>,
    ) -> BarasonaResult<Entry<D>> {
        let entry = Entry {
            term: self.core.current_term,
            index: self.core.last_log_index + 1,
            payload,
        };
        self.core
            .storage
            .append_entry_to_log(&entry)
            .await
            .map_err(|err| self.core.map_fatal_storage_error(err.into()))?;
        self.core.last_log_index = entry.index;
        Ok(entry)
    }

    /// Begin the process of replicating the given client request.
    ///
    /// NOTE WELL: this routine does not wait for the request to actually finish replication, it
    /// merely begings the process. Once the request is committed to the cluster, its response will
    /// be generated asynchronously.
    #[tracing::instrument(level = "trace", skip(self, req))]
    pub(super) async fn replicate_client_request(&mut self, req: ClientRequestEntry<D, R>) {
        // Replicate the request if there are other cluster members. The client response will be
        // returned elsewhere after the entry has been committed to the cluster.
        let entry_arc = req.entry.clone();
        if !self.nodes.is_empty() {
            self.awaiting_committed.push(req);
            for node in self.nodes.values() {
                let _ = node
                    .replication_stream
                    .repl_tx
                    .send(BarasonaEvent::Replicate {
                        entry: entry_arc.clone(),
                        commit_index: self.core.commit_index,
                    });
            }
        } else {
            self.core.commit_index = req.entry.index;
            self.core.report_metrics();
            self.client_request_post_commit(req).await;
        }

        // Replicate to non-voters
        for node in self.non_voters.values() {
            let _ = node
                .state
                .replication_stream
                .repl_tx
                .send(BarasonaEvent::Replicate {
                    entry: entry_arc.clone(),
                    commit_index: self.core.commit_index,
                });
        }
    }

    /// Handle the post-commit logic for a client request.
    #[tracing::instrument(level = "trace", skip(self, req))]
    pub(super) async fn client_request_post_commit(&mut self, req: ClientRequestEntry<D>) {
        match req.tx {
            ClientOrInternalResponseTx::Client(tx) => {
                match &req.entry.payload {
                    EntryPayload::Normal(_) => {
                        // TODO Ricardo here the entry is applied to the state machine.
                        let _ = tx.send(Ok(ClientWriteResponse {
                            index: req.entry.index,
                        }));
                    }
                    _ => {
                        // Why is this a bug, and why are we shutting down? This is because we can not easily
                        // encode these constraints in the type system, and client requests should be the only
                        // log entry types for which a `ClientOrInternalResponseTx::Client` type is used. This
                        // error should never be hit unless we've done a poor job in code review.
                        tracing::error!("critical error in Barasona, this is a programming bug, please open an issue");
                        self.core.set_target_state(State::Shutdown);
                    }
                }
            }
            ClientOrInternalResponseTx::Internal(tx) => {
                self.core.report_metrics();
                let _ = tx.send(Ok(req.entry.index));
            }
        }

        // Trigger log compaction if needed.
        self.core.trigger_log_compaction_if_needed();
    }
}
