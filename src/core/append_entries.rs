use crate::barasona::{
    AppendEntriesRequest, AppendEntriesResponse, ConflictOpt, Entry, EntryPayload,
};
use crate::core::{BarasonaCore, BarasonaResult, State};
use crate::{network::BarasonaNetwork, storage::BarasonaStorage, AppData, AppDataResponse};

impl<D: AppData, R: AppDataResponse, N: BarasonaNetwork<D>, S: BarasonaStorage<D, R>>
    BarasonaCore<D, R, N, S>
{
    /// An RPC invoked by the leader to replicate log entries; also used as heartbeat.
    ///
    /// See `receiver implementation: AppendEntries RPC` in barasona-essentials.md in this repo.
    #[tracing::instrument(
        level="trace", skip(self, msg),
        fields(term=msg.term, leader_id=msg.leader_id, prev_log_index=msg.prev_log_index, prev_log_term=msg.prev_log_term, leader_commit=msg.leader_commit),
    )]
    pub(super) async fn handle_append_entries_request(
        &mut self,
        msg: AppendEntriesRequest<D>,
    ) -> BarasonaResult<AppendEntriesResponse> {
        // If the message's term is less than current term, do not honor the request.
        if msg.term < self.current_term {
            tracing::trace!({self.current_term, rpc_term=msg.term}, "AppendEntries RPC term is less than current term");
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_opt: None,
            });
        }

        let mut report_metrics = false;

        // Update election timeout.
        self.update_next_election_timeout(true);

        // Update commit index.
        self.commit_index = msg.leader_commit;

        // Update leader if needed.
        if self.current_leader.as_ref() != Some(&msg.leader_id) {
            self.update_current_leader(super::UpdateCurrentLeader::OtherNode(msg.leader_id));
            report_metrics = true;
        }

        // Update term if needed.
        if self.current_term != msg.term {
            self.update_current_term(msg.term, None);
            self.save_hard_state().await?;
            report_metrics = true;
        }

        // Change state to follower.
        if !self.target_state.is_follower() && !self.target_state.is_non_voter() {
            self.set_target_state(State::Follower);
        }

        // Check if it is heart beat.
        if msg.entries.is_empty() {
            if report_metrics {
                self.report_metrics();
            }
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: true,
                conflict_opt: None,
            });
        }

        // If RPC's `prev_log_index` is 0, or the RPC's previous log info matches the local
        // log info, then replication is good to go.
        let msg_prev_index_is_min = msg.prev_log_index == u64::MIN;
        let msg_index_and_term_match = (msg.prev_log_index == self.last_log_index)
            && (msg.prev_log_term == self.last_log_term);

        if msg_prev_index_is_min || msg_index_and_term_match {
            self.append_log_entries(&msg.entries).await?;
            if report_metrics {
                self.report_metrics();
            }
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: true,
                conflict_opt: None,
            });
        }

        /////////////////////////////////////
        //// Begin Log Consistency Check ////
        tracing::trace!("begin log consistency check");

        // Previous log info doesn't immediately line up, so perform log consistency check and proceed based on its result.
        let entries = self
            .storage
            .get_log_entries(msg.prev_log_index, msg.prev_log_index + 1)
            .await
            .map_err(|err| self.map_fatal_storage_error(err.into()))?;
        let target_entry = match entries.first() {
            Some(target_entry) => target_entry,
            // The target entry was not found. This can only mean that we don't have the
            // specified index yet. Use the last known index & term as a conflict opt.
            None => {
                if report_metrics {
                    self.report_metrics();
                }
                return Ok(AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    conflict_opt: Some(ConflictOpt {
                        term: self.last_log_term,
                        index: self.last_log_index,
                    }),
                });
            }
        };

        // The target entry was found. Compare its term with target to ensure everything is consistent.
        if target_entry.term == msg.prev_log_term {
            // We've found a point of agreement with the leader. If we have any logs present
            // with an index greater than this, then we must delete them.
            if self.last_log_index > target_entry.index {
                self.storage
                    .delete_logs_from(target_entry.index + 1, None)
                    .await
                    .map_err(|err| self.map_fatal_storage_error(err.into()))?;

                let membership = self
                    .storage
                    .get_membership_config()
                    .await
                    .map_err(|err| self.map_fatal_storage_error(err))?;
                self.update_membership(membership)?;
            }
        }
        // The target entry does not have the same term. Fetch the last 50 logs, and use the last
        // entry of that payload which is still in the target term for conflict optimization.
        else {
            let start = if msg.prev_log_index >= 50 {
                msg.prev_log_index - 50
            } else {
                0
            };
            let old_entries = self
                .storage
                .get_log_entries(start, msg.prev_log_index)
                .await
                .map_err(|err| self.map_fatal_storage_error(err.into()))?;
            let conflict_opt = match old_entries
                .iter()
                .find(|old_entry| old_entry.term == msg.prev_log_term)
            {
                Some(entry) => Some(ConflictOpt {
                    term: entry.term,
                    index: entry.index,
                }),
                None => Some(ConflictOpt {
                    term: self.last_log_term,
                    index: self.last_log_index,
                }),
            };
            if report_metrics {
                self.report_metrics();
            }
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_opt,
            });
        }

        ///////////////////////////////////
        //// End Log Consistency Check ////
        tracing::trace!("end log consistency check");

        self.append_log_entries(&msg.entries).await?;
        if report_metrics {
            self.report_metrics();
        }

        Ok(AppendEntriesResponse {
            term: msg.term,
            success: true,
            conflict_opt: None,
        })
    }

    /// Append the given entries to the log.
    ///
    /// Configuration changes are also detected and applied here. See `configuration changes`
    /// in the barasona-essentials.md in this repo.
    ///
    /// Very importantly, this routine must not block the main control loop main task, else it
    /// may cause the Barasona leader to timeout the requests to this node.

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_log_entries(&mut self, entries: &[Entry<D>]) -> BarasonaResult<()> {
        let last_config_change = entries
            .iter()
            .filter_map(|entry| match &entry.payload {
                EntryPayload::ConfigChange(conf) => Some(conf),
                _ => None,
            })
            .last();

        if let Some(conf) = last_config_change {
            tracing::debug!({membership=?conf}, "applying new membership config received from leader");
            self.update_membership(conf.membership.clone())?;
        }

        // Replicate entries to log (same as append, but in follower mode).
        self.storage
            .replicate_to_log(entries)
            .await
            .map_err(|err| self.map_fatal_storage_error(err.into()))?;

        if let Some(entry) = entries.last() {
            self.last_log_index = entry.index;
            self.last_log_term = entry.term;
        }

        Ok(())
    }
}
