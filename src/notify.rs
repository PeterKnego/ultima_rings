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

#[cfg(loom)]
mod imp {
    //! Loom-modeled parking: Mutex+Condvar so loom explores wakeups and
    //! detects deadlocks (a lost wakeup = all-threads-blocked = loom failure).
    use crate::atomic::{AtomicBool, Ordering};
    use loom::sync::{Condvar, Mutex};

    #[derive(Debug)]
    pub(crate) struct Parker {
        parked: AtomicBool,
        state: Mutex<bool>, // token
        cv: Condvar,
    }

    impl Parker {
        pub(crate) fn new() -> Self {
            Parker {
                parked: AtomicBool::new(false),
                state: Mutex::new(false),
                cv: Condvar::new(),
            }
        }
        pub(crate) fn prepare_park(&self) {
            self.parked.store(true, Ordering::Relaxed);
        }
        pub(crate) fn cancel(&self) {
            self.parked.store(false, Ordering::Relaxed);
        }
        pub(crate) fn park(&self) {
            let mut token = self.state.lock().unwrap();
            while !*token {
                token = self.cv.wait(token).unwrap();
            }
            *token = false;
            drop(token);
            self.parked.store(false, Ordering::Relaxed);
        }
        pub(crate) fn wake(&self) {
            if self.parked.load(Ordering::Relaxed) {
                self.parked.store(false, Ordering::Relaxed);
                *self.state.lock().unwrap() = true;
                self.cv.notify_one();
            }
        }
    }

    #[derive(Debug)]
    pub(crate) struct WaiterList {
        waiting: AtomicBool,
        // NOTE: named `epoch`, not `gen` (the brief's name) — `gen` is a
        // reserved keyword as of edition 2024, and the keyword/identifier
        // distinction is lexer-level (applies before `#[cfg(loom)]`
        // stripping), so it breaks both the loom and non-loom builds.
        // Same generation-counter protocol as the brief, renamed only.
        epoch: Mutex<usize>,
        cv: Condvar,
    }

    impl WaiterList {
        pub(crate) fn new() -> Self {
            WaiterList {
                waiting: AtomicBool::new(false),
                epoch: Mutex::new(0),
                cv: Condvar::new(),
            }
        }
        pub(crate) fn prepare_wait(&self) {
            self.waiting.store(true, Ordering::Relaxed);
        }
        pub(crate) fn park(&self) {
            let mut g = self.epoch.lock().unwrap();
            let g0 = *g;
            while *g == g0 && self.waiting.load(Ordering::Relaxed) {
                g = self.cv.wait(g).unwrap();
            }
        }
        pub(crate) fn wake_all(&self) {
            if self.waiting.swap(false, Ordering::Relaxed) {
                *self.epoch.lock().unwrap() += 1;
                self.cv.notify_all();
            }
        }
    }
}

pub(crate) use imp::{Parker, WaiterList};
