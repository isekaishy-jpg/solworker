//! Platform-specific setup performed by each worker before it reports ready.

use std::error::Error;
use std::fmt;
use std::io;

use crate::runtime::config::SWThreadPriority;

/// A requested worker policy could not be established.
#[derive(Debug)]
pub enum SWWorkerSetupError {
    UnsupportedPriority(SWThreadPriority),
    System(io::Error),
}

impl fmt::Display for SWWorkerSetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPriority(priority) => {
                write!(
                    formatter,
                    "worker priority {priority:?} is unsupported on this platform"
                )
            }
            Self::System(error) => write!(formatter, "worker priority setup failed: {error}"),
        }
    }
}

impl Error for SWWorkerSetupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedPriority(_) => None,
            Self::System(error) => Some(error),
        }
    }
}

/// Applies an optional OS priority on the calling worker thread.
///
/// Call this inside the worker's startup gate. A helping caller does not run
/// worker setup and must meet its work's context requirements independently.
pub(crate) fn apply_worker_priority(
    requested_priority: Option<SWThreadPriority>,
) -> Result<(), SWWorkerSetupError> {
    let Some(priority) = requested_priority else {
        return Ok(());
    };

    #[cfg(windows)]
    {
        windows::apply_thread_priority(priority).map_err(SWWorkerSetupError::System)
    }
    #[cfg(not(windows))]
    {
        Err(SWWorkerSetupError::UnsupportedPriority(priority))
    }
}

#[cfg(windows)]
mod windows {
    use std::ffi::c_void;
    use std::io;

    use crate::runtime::config::SWThreadPriority;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "GetCurrentThread"]
        fn get_current_thread() -> *mut c_void;
        #[link_name = "SetThreadPriority"]
        fn set_thread_priority(thread: *mut c_void, priority: i32) -> i32;
    }

    pub(super) fn apply_thread_priority(priority: SWThreadPriority) -> io::Result<()> {
        let relative_priority = match priority {
            SWThreadPriority::BelowNormal => -1,
            SWThreadPriority::Normal => 0,
            SWThreadPriority::AboveNormal => 1,
        };

        // SAFETY: GetCurrentThread returns a valid pseudo-handle for this
        // calling thread. SetThreadPriority consumes it only during the call;
        // the pseudo-handle must not be closed. The priority values above are
        // documented THREAD_PRIORITY_* constants accepted by this API.
        let applied = unsafe { set_thread_priority(get_current_thread(), relative_priority) };
        if applied == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
