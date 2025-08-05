//! Manages tokio runtimes for the application.
//!
//! Runtime is per-cluster and can be changed with `cass_cluster_set_num_threads_io`.

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use tokio::runtime::Runtime;

/// Manages tokio runtimes for the application.
///
/// Runtime is per-cluster and can be changed with `cass_cluster_set_num_threads_io`.
/// Once a runtime is created, it is cached for future use.
/// Once all `CassSession` instances that reference the runtime are dropped,
/// the runtime is also dropped.
pub(crate) struct Runtimes {
    // Weak pointers are used to make runtimes dropped once all `CassSession` instances
    // that reference them are freed.
    default_runtime: Option<Weak<Runtime>>,
    // This is Option to allow creating a static instance of Runtimes.
    // (`HashMap::new` is not `const`).
    n_thread_runtimes: Option<HashMap<usize, Weak<Runtime>>>,
}

pub(crate) static RUNTIMES: std::sync::Mutex<Runtimes> = {
    std::sync::Mutex::new(Runtimes {
        default_runtime: None,
        n_thread_runtimes: None,
    })
};

impl Runtimes {
    fn cached_or_new_runtime(
        weak_runtime: &mut Weak<Runtime>,
        create_runtime: impl FnOnce() -> Result<Arc<Runtime>, std::io::Error>,
    ) -> Result<Arc<Runtime>, std::io::Error> {
        match weak_runtime.upgrade() {
            Some(cached_runtime) => Ok(cached_runtime),
            None => {
                let runtime = create_runtime()?;
                *weak_runtime = Arc::downgrade(&runtime);
                Ok(runtime)
            }
        }
    }

    /// Returns a default tokio runtime.
    ///
    /// If it's not created yet, it will create a new one with the default configuration
    /// and cache it for future use.
    pub(crate) fn default_runtime(&mut self) -> Result<Arc<Runtime>, std::io::Error> {
        let default_runtime_slot = self.default_runtime.get_or_insert_with(Weak::new);
        Self::cached_or_new_runtime(default_runtime_slot, || Runtime::new().map(Arc::new))
    }

    /// Returns a tokio runtime with `n_threads` worker threads.
    ///
    /// If it's not created yet, it will create a new one and cache it for future use.
    pub(crate) fn n_thread_runtime(
        &mut self,
        n_threads: usize,
    ) -> Result<Arc<Runtime>, std::io::Error> {
        let n_thread_runtimes = self.n_thread_runtimes.get_or_insert_with(HashMap::new);
        let n_thread_runtime_slot = n_thread_runtimes.entry(n_threads).or_default();

        Self::cached_or_new_runtime(n_thread_runtime_slot, || {
            match n_threads {
                0 => tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build(),
                n => tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(n)
                    .enable_all()
                    .build(),
            }
            .map(Arc::new)
        })
    }
}
