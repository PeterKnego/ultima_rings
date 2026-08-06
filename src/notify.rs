//! The notify layer: all parking lives here, none in the lock-free cores.
//!
//! Wake correctness is the Dekker protocol (see docs/design.md): the waiter
//! stores its flag, fences SeqCst, re-checks the ring, then parks; the waker
//! publishes, fences SeqCst, then checks the flag. `std::thread::park`'s
//! token makes a wake that races ahead of the park harmless.

#[cfg(not(loom))]
mod imp {
    use crate::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::thread::{self, Thread};

    /// Single-waiter parker (the single consumer, or the SPSC producer).
    #[derive(Debug)]
    pub(crate) struct Parker {
        parked: AtomicBool,
        slot: Mutex<Option<Thread>>, // cold path only (park/wake transitions)
    }

    impl Parker {
        pub(crate) fn new() -> Self {
            Parker {
                parked: AtomicBool::new(false),
                slot: Mutex::new(None),
            }
        }

        /// Register intent to park. Caller MUST fence(SeqCst) and re-check
        /// its wait condition before calling `park`.
        pub(crate) fn prepare_park(&self) {
            *self.slot.lock().unwrap() = Some(thread::current());
            self.parked.store(true, Ordering::Relaxed);
        }

        /// Withdraw after a failed re-check.
        pub(crate) fn cancel(&self) {
            self.parked.store(false, Ordering::Relaxed);
        }

        /// Block until woken (or spuriously). Always re-check after return.
        pub(crate) fn park(&self) {
            thread::park();
            self.parked.store(false, Ordering::Relaxed);
        }

        /// Wake the registered waiter if one is parked. Caller MUST have
        /// fenced SeqCst after its publish.
        pub(crate) fn wake(&self) {
            if self.parked.load(Ordering::Relaxed) {
                self.parked.store(false, Ordering::Relaxed);
                if let Some(t) = self.slot.lock().unwrap().take() {
                    t.unpark();
                }
            }
        }
    }

    /// Multi-waiter list (MPSC producers blocked on a full ring). Cold path
    /// by construction: it only runs once a sender has decided to park.
    #[derive(Debug)]
    pub(crate) struct WaiterList {
        waiting: AtomicBool,
        list: Mutex<Vec<Thread>>,
    }

    impl WaiterList {
        pub(crate) fn new() -> Self {
            WaiterList {
                waiting: AtomicBool::new(false),
                list: Mutex::new(Vec::new()),
            }
        }

        /// Register the current thread. Caller MUST fence(SeqCst) and
        /// re-check before `park`.
        pub(crate) fn prepare_wait(&self) {
            self.list.lock().unwrap().push(std::thread::current());
            self.waiting.store(true, Ordering::Relaxed);
        }

        /// Block until woken (or spuriously). Always re-check after return.
        pub(crate) fn park(&self) {
            std::thread::park();
        }

        /// Wake every registered waiter (each re-checks its own condition).
        /// Caller MUST have fenced SeqCst after advancing head/disconnecting.
        pub(crate) fn wake_all(&self) {
            if self.waiting.swap(false, Ordering::Relaxed) {
                for t in self.list.lock().unwrap().drain(..) {
                    t.unpark();
                }
            }
        }
    }
}

pub(crate) use imp::{Parker, WaiterList};
