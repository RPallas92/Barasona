//! Error types exposed by Barasona.

use thiserror::Error;

/// Error variants related to configuration.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    /// A configuration error indicating that the given values for election timeout min & max are invalid: max must be greater than min.
    #[error(
        "given values for election timeout min & max are invalid: max must be greater than min"
    )]
    InvalidElectionTimeoutMinMax,
    /// The given value for max_payload_entries is too small, must be > 0.
    #[error("the given value for max_payload_entries is too small, must be > 0")]
    MaxPayloadEntriesTooSmall,
}
