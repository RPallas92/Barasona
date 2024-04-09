use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::{
    barasona::{InstallSnapshotRequest, InstallSnapshotResponse},
    error::BarasonaResult,
    network::BarasonaNetwork,
    storage::BarasonaStorage,
    AppData, AppDataResponse,
};

use crate::core::{BarasonaCore, SnapshotState, State};

impl<D: AppData, R: AppDataResponse, N: BarasonaNetwork<D>, S: BarasonaStorage<D, R>>
    BarasonaCore<D, R, N, S>
{
    /// Invoked by the leader to send chunks of a snapshot to a follower.
    ///
    /// Leaders always send chunks in order. It is important to note that, according to the Barasona spec,
    /// a log may only have one snapshot at any time. As snapshot contents are application specific,
    /// the Barasona log will only store a pointer to the snapshot file along with the index & term.
    #[tracing::instrument(level = "trace", skip(self, req))]
    pub(super) async fn handle_install_snapshot_request(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> BarasonaResult<InstallSnapshotResponse> {
        // If message's term is less than most recent term, then we do not honor the request.
        if req.term < self.current_term {
            return Ok(InstallSnapshotResponse {
                term: self.current_term,
            });
        }

        // Update election timeout.
        self.update_next_election_timeout(true);

        // Update current term if needed.
        let mut report_metrics = false;
        if req.term > self.current_term {
            self.update_current_term(req.term, None);
            report_metrics = true;
        }

        // Update current leader if needed.
        if self.current_leader.as_ref() != Some(&req.leader_id) {
            self.update_current_leader(super::UpdateCurrentLeader::OtherNode(req.leader_id));
            report_metrics = true;
        }

        // If not follower, become follower.
        if !self.target_state.is_follower() && !self.target_state.is_non_voter() {
            self.set_target_state(State::Follower); // State update will emit metrics
        }

        if report_metrics {
            self.report_metrics();
        }

        // Compare current snapshot state with received RPC and handle as needed.
        match self.snapshot_state.take() {
            None => Ok(self.begin_installing_snapshot(req).await?),
            Some(SnapshotState::Snapshotting { handle, .. }) => {
                handle.abort(); // Abort the current compaction in favor of installation from leader.
                Ok(self.begin_installing_snapshot(req).await?)
            }
            Some(SnapshotState::Streaming {
                offset,
                id,
                snapshot,
            }) => Ok(self
                .continue_installing_snapshot(req, offset, id, snapshot)
                .await?),
        }
    }

    #[tracing::instrument(level = "trace", skip(self, req))]
    async fn begin_installing_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> BarasonaResult<InstallSnapshotResponse> {
        let (id, mut snapshot) = self
            .storage
            .create_snapshot()
            .await
            .map_err(|err| self.map_fatal_storage_error(err))?;

        snapshot.as_mut().write_all(&req.data).await?;

        // If this was a small snapshot, and it is already done, then finish up.
        if req.done {
            self.finalize_snapshot_installation(req, id, snapshot)
                .await?;
            return Ok(InstallSnapshotResponse {
                term: self.current_term,
            });
        }

        self.snapshot_state = Some(SnapshotState::Streaming {
            offset: req.data.len() as u64,
            id,
            snapshot,
        });

        Ok(InstallSnapshotResponse {
            term: self.current_term,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, req, offset, snapshot))]
    async fn continue_installing_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
        offset: u64,
        id: String,
        mut snapshot: Box<S::Snapshot>,
    ) -> BarasonaResult<InstallSnapshotResponse> {
        // Always seek to the target offset if not an exact match
        if req.offset != offset {
            if let Err(err) = snapshot
                .as_mut()
                .seek(std::io::SeekFrom::Start(req.offset))
                .await
            {
                self.snapshot_state = Some(SnapshotState::Streaming {
                    offset: offset,
                    id: id,
                    snapshot: snapshot,
                });
                return Err(err.into());
            }
        }

        let mut offset = req.offset;

        // Write the next segment and update offset
        if let Err(err) = snapshot.as_mut().write_all(&req.data).await {
            self.snapshot_state = Some(SnapshotState::Streaming {
                offset: offset,
                id,
                snapshot,
            });
            return Err(err.into());
        }
        offset += req.data.len() as u64;

        // If the snapshot stream is done, then finalize.
        if req.done {
            self.finalize_snapshot_installation(req, id, snapshot)
                .await?
        } else {
            self.snapshot_state = Some(SnapshotState::Streaming {
                offset,
                id,
                snapshot,
            });
        }

        Ok(InstallSnapshotResponse {
            term: self.current_term,
        })
    }

    /// Finalize the installation of a new snapshot.
    ///
    /// Any errors which come up from this routine will cause the Barasona node to go into shutdown.
    #[tracing::instrument(level = "trace", skip(self, req, snapshot))]
    async fn finalize_snapshot_installation(
        &mut self,
        req: InstallSnapshotRequest,
        id: String,
        mut snapshot: Box<S::Snapshot>,
    ) -> BarasonaResult<()> {
        snapshot
            .as_mut()
            .shutdown()
            .await
            .map_err(|err| self.map_fatal_storage_error(err.into()))?;
        let delete_through = if self.last_log_index > req.last_included_index {
            Some(self.last_log_index)
        } else {
            None
        };
        self.storage
            .finalize_snapshot_installation(
                req.last_included_index,
                req.last_included_term,
                delete_through,
                id,
                snapshot,
            )
            .await
            .map_err(|err| self.map_fatal_storage_error(err.into()))?;
        let membership = self
            .storage
            .get_membership_config()
            .await
            .map_err(|err| self.map_fatal_storage_error(err.into()))?;
        self.update_membership(membership)?;
        self.last_log_index = req.last_included_index;
        self.last_log_term = req.last_included_term;
        self.snapshot_index = req.last_included_index;
        Ok(())
    }
}
