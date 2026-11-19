// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Resolving configured database checkpoint roots without implicit fallbacks.

use super::CheckpointStore;
use otel_arrow_dfe_engine::context::PipelineContext;
use std::path::{Component, Path};

const ENGINE_ROOT: &str = "${engine.state_dir}";

/// Failure to resolve or open a configured checkpoint directory.
///
/// Diagnostics deliberately exclude potentially sensitive filesystem paths.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointDirectoryError {
    /// Invalid or unsupported configuration expression.
    #[error("{0}")]
    Invalid(&'static str),
    /// Managed storage could not be opened safely.
    #[error("failed to open engine-owned checkpoint directory")]
    Io(#[source] std::io::Error),
}

pub(crate) fn validate_directory(directory: &str) -> Result<(), CheckpointDirectoryError> {
    let path = Path::new(directory);
    if directory.trim().is_empty() || directory.contains('\0') {
        return Err(CheckpointDirectoryError::Invalid(
            "checkpoint.directory must be non-empty and contain no NUL bytes",
        ));
    }
    if path.components().any(|part| part == Component::ParentDir) {
        return Err(CheckpointDirectoryError::Invalid(
            "checkpoint.directory must not contain '..' components",
        ));
    }
    let remainder = path.strip_prefix(ENGINE_ROOT).unwrap_or(path);
    if remainder
        .as_os_str()
        .as_encoded_bytes()
        .windows(2)
        .any(|s| s == b"${")
    {
        return Err(CheckpointDirectoryError::Invalid(
            "checkpoint.directory contains an unsupported or unresolved placeholder; only the ${engine.state_dir} path prefix is supported",
        ));
    }
    Ok(())
}

impl CheckpointStore {
    /// Resolves configuration and retains the engine-owned directory capability.
    ///
    /// Native absolute and relative paths keep their historical locations.
    /// The engine prefix requires a provisioned root; neither environment
    /// lookup nor a working-directory fallback is performed.
    pub fn from_config(
        directory: &str,
        pipeline: &PipelineContext,
        receiver_name: &str,
        source_id: &str,
        config_fingerprint: String,
    ) -> Result<Self, CheckpointDirectoryError> {
        validate_directory(directory)?;
        let path = Path::new(directory);
        let managed = path.strip_prefix(ENGINE_ROOT).ok();
        let mut store = Self::new_native(
            managed.unwrap_or(path),
            pipeline.pipeline_group_id().as_ref(),
            pipeline.pipeline_id().as_ref(),
            receiver_name,
            source_id,
            config_fingerprint,
        );
        if managed.is_some() {
            let root = pipeline.state_directory().ok_or(CheckpointDirectoryError::Invalid(
                "checkpoint.directory uses ${engine.state_dir}, but engine.state_dir is not configured; configure the existing state root or use an explicit checkpoint.directory (no OTAP_DF_STATE_DIR or .otap-state fallback)",
            ))?;
            store.pin_directory(root)?;
        }

        Ok(store)
    }

    #[cfg(target_os = "linux")]
    fn pin_directory(
        &mut self,
        root: &otel_arrow_dfe_engine::state_dir::StateDirectory,
    ) -> Result<(), CheckpointDirectoryError> {
        use nix::errno::Errno;
        use nix::fcntl::{OFlag, openat};
        use nix::sys::stat::{Mode, fstat, mkdirat};
        use nix::unistd::geteuid;
        use std::fs::File;
        use std::os::fd::{AsFd, AsRawFd};
        use std::sync::Arc;

        let io = |error: Errno| CheckpointDirectoryError::Io(error.into());
        let mut parent = root
            .as_fd()
            .try_clone_to_owned()
            .map_err(CheckpointDirectoryError::Io)?;
        // Pin every descendant, including the existing versioned identity
        // namespace. Never reopen StateDirectory::path(), even after a rename.
        for component in self
            .prefix
            .parent()
            .expect("checkpoint parent")
            .components()
        {
            let Component::Normal(name) = component else {
                if component == Component::CurDir {
                    continue;
                }
                return Err(CheckpointDirectoryError::Invalid(
                    "engine-owned checkpoint directory must contain only relative path components",
                ));
            };
            let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
            let next = match openat(&parent, name, flags, Mode::empty()) {
                Ok(next) => next,
                Err(Errno::ENOENT) => {
                    match mkdirat(&parent, name, Mode::S_IRWXU) {
                        Ok(()) | Err(Errno::EEXIST) => (),
                        Err(error) => return Err(io(error)),
                    }
                    openat(&parent, name, flags, Mode::empty()).map_err(io)?
                }
                Err(error) => return Err(io(error)),
            };
            let stat = fstat(&next).map_err(io)?;
            // Permit existing private or read/searchable namespaces without
            // repairing permissions or following an untrusted writer's links.
            if stat.st_uid != geteuid().as_raw() || stat.st_mode & 0o022 != 0 {
                return Err(CheckpointDirectoryError::Invalid(
                    "engine-owned checkpoint descendants must belong to the collector UID and not be group/other writable",
                ));
            }
            File::from(parent)
                .sync_all()
                .map_err(CheckpointDirectoryError::Io)?;
            parent = next;
        }
        let parent = File::from(parent);
        parent.sync_all().map_err(CheckpointDirectoryError::Io)?;
        // The legacy checkpoint/lease implementation is path-based. Linux
        // procfs exposes the pinned directory to it without reopening ancestors.
        // Retaining the descriptor in every clone prevents fd-number reuse.
        let pinned = std::path::PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd()));
        let _ = std::fs::metadata(&pinned).map_err(CheckpointDirectoryError::Io)?;
        self.prefix = pinned.join(self.prefix.file_name().expect("checkpoint filename"));
        self._directory = Some(Arc::new(parent));
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn pin_directory(
        &mut self,
        _root: &otel_arrow_dfe_engine::state_dir::StateDirectory,
    ) -> Result<(), CheckpointDirectoryError> {
        Err(CheckpointDirectoryError::Invalid(
            "engine-owned checkpoint directories require Linux; use an explicit native checkpoint.directory",
        ))
    }
}

#[cfg(test)]
#[path = "checkpoint_directory_tests.rs"]
mod tests;
