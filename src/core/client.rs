use std::sync::Arc;

use tokio::sync::oneshot;

use crate::{
    barasona::{ClientWriteResponse, ClientWriteResponseTx, Entry, EntryPayload},
    core::{LeaderState, State},
    error::{BarasonaError, BarasonaResult},
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
