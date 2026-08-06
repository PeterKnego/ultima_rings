//! Facade over `std` vs `loom` sync primitives so the cores can be
//! model-checked. Everything in the crate uses these re-exports, never
//! `std::sync::atomic` directly.

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicI64, fence};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicI64, fence};

/// `UnsafeCell` with loom's closure API in both builds.
#[cfg(not(loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub(crate) fn new(v: T) -> Self {
        Self(std::cell::UnsafeCell::new(v))
    }
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;

/// Pins a value to its own 64-byte cache line (same trick as the bench cells).
#[repr(align(64))]
#[derive(Debug)]
pub(crate) struct CachePadded<T>(pub(crate) T);
