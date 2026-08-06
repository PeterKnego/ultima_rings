//! Bounded lock-free SPSC and MPSC rings with pluggable wait strategies.
//!
//! Extracted from the `hi-perf-cmp` thread-handoff benchmarks and hardened
//! for production use: generic payloads, blocking and non-blocking APIs,
//! close/disconnect semantics, and a loom/miri-verified concurrency core.
//! See `docs/design.md` for the memory-ordering invariants.

#![warn(missing_docs)]

mod atomic;
pub mod mpsc;
mod notify;
pub mod spsc;
mod wait;

pub use wait::WaitStrategy;

use std::fmt;

/// Error for `try_send` (both [`spsc::Sender::try_send`] and
/// [`mpsc::Sender::try_send`]): the value is handed back in both cases.
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The ring is full.
    Full(T),
    /// The receiver was dropped.
    Disconnected(T),
}

/// Error for blocking `send`: the receiver was dropped; the value is returned.
#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

/// Error for `try_recv`.
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// The ring is empty (senders still live).
    Empty,
    /// All senders dropped and the ring is drained.
    Disconnected,
}

/// Error for blocking `recv`: all senders dropped and the ring is drained.
#[derive(Debug, PartialEq, Eq)]
pub struct RecvError;

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "ring is full"),
            TrySendError::Disconnected(_) => write!(f, "receiver disconnected"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiver disconnected")
    }
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "ring is empty"),
            TryRecvError::Disconnected => write!(f, "senders disconnected"),
        }
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "senders disconnected")
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {}
impl<T: fmt::Debug> std::error::Error for SendError<T> {}
impl std::error::Error for TryRecvError {}
impl std::error::Error for RecvError {}

pub(crate) fn assert_cap(cap: usize) {
    assert!(
        cap > 0 && cap.is_power_of_two(),
        "capacity must be a positive power of two"
    );
}
