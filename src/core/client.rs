use std::sync::Arc;

use tokio::sync::oneshot;

use crate::{
    barasona::{ClientWriteResponseTx, Entry},
    error::BarasonaError,
    AppData, AppDataResponse,
};

/// A wrapper around a ClientRequest which has been transformed into an Entry, along with its response channel.
pub(super) struct ClientRequestEntry<D: AppData, R: AppDataResponse> {
    /// The Arc'd entry of the ClientRequest.
    ///
    /// This value is Arc'd so that it may be sent across thread boundaries for replication
    /// without having to clone the data payload itself.
    pub entry: Arc<Entry<D>>,
    /// The response channel for the request.
    pub tx: ClientOrInternalResponseTx<D, R>,
}

impl<D: AppData, R: AppDataResponse> ClientRequestEntry<D, R> {
    /// Create a new instance from the raw components of a client request.
    pub(crate) fn from_entry<T: Into<ClientOrInternalResponseTx<D, R>>>(
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
pub enum ClientOrInternalResponseTx<D: AppData, R: AppDataResponse> {
    Client(ClientWriteResponseTx<D, R>),
    Internal(oneshot::Sender<Result<u64, BarasonaError>>),
}
