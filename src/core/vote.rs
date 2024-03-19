use tokio::time::Instant;

use crate::barasona::{VoteRequest, VoteResponse};
use crate::core::{BarasonaCore, State};
use crate::error::BarasonaResult;
use crate::{network::BarasonaNetwork, storage::BarasonaStorage, AppData, AppDataResponse};

impl<D: AppData, R: AppDataResponse, N: BarasonaNetwork<D>, S: BarasonaStorage<D, R>>
    BarasonaCore<D, R, N, S>
{
    /// A RPC invoked by candidates to gather votes.
    ///
    /// See `receiver implementation: RequestVote RPC` in barasona-essentials.md in this repo.
    #[tracing::instrument(level = "trace", skip(self, msg))]
    pub(super) async fn handle_vote_request(
        &mut self,
        msg: VoteRequest,
    ) -> BarasonaResult<VoteResponse> {
        // If candidate's current term is less than this nodes current term, reject.
        if msg.term < self.current_term {
            tracing::trace!({candidate=msg.candidate_id, self.current_term, candidate_term=msg.term}, "RequestVote RPC term is less than current term");

            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }

        // Do not respond to the request if we have received a heartbeat during the election timeout minimum
        if let Some(inst) = self.last_heartbeat {
            let now = Instant::now();
            let delta = now.duration_since(inst);
            if self.config.election_timeout_min_ms >= (delta.as_millis() as u64) {
                tracing::trace!(
                    { candidate = msg.candidate_id },
                    "rejecting vote request received within election timeout minimum"
                );

                return Ok(VoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                });
            }
        }

        // Per spec, if we observe a term greater than our own outside of the election timeout
        // minimum, then we must update term & immediately become follower. We still need to
        // do vote checking after this.
        if msg.term > self.current_term {
            self.update_current_term(msg.term, None);
            self.update_next_election_timeout(false);
            self.set_target_state(State::Follower);
            self.save_hard_state().await?
        }

        // Check if candidate's log is at least as up-to-date as this node's.
        // If candidate's log is not at least as up-to-date as this node, then reject.
        let client_is_uptodate = (msg.last_log_index >= self.last_log_index)
            && (msg.last_log_term >= self.last_log_term);
        if !client_is_uptodate {
            tracing::trace!(
                { candidate = msg.candidate_id },
                "rejecting vote request as candidate's log is not up-to-date"
            );
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }

        // Candidate's log is up-to-date so handle voting conditions.
        match self.voted_for {
            // This node has already voted for the candidate.
            Some(candidate_id) if candidate_id == msg.candidate_id => Ok(VoteResponse {
                term: self.current_term,
                vote_granted: true,
            }),
            // This node has already voted for a different candidate.
            Some(_) => Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
            }),
            // This node has not yet voted for the current term, so vote for the candidate.
            None => {
                self.voted_for = Some(msg.candidate_id);
                self.set_target_state(State::Follower);
                self.update_next_election_timeout(false);
                self.save_hard_state().await?;
                tracing::trace!({candidate=msg.candidate_id, msg.term}, "voted for candidate");
                Ok(VoteResponse {
                    term: self.current_term,
                    vote_granted: true,
                })
            }
        }
    }
}
