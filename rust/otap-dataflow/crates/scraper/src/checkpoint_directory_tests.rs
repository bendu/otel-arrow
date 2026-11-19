// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::database::{Cursor, ScalarValue};
use crate::{LeaseError, SourceBinding, SourceLease};
use otel_arrow_dfe_engine::context::ControllerContext;
use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

fn context() -> PipelineContext {
    ControllerContext::new(TelemetryRegistryHandle::new()).pipeline_context_with(
        "group".into(),
        "pipeline".into(),
        0,
        1,
        0,
    )
}

/// Scenario: A checkpoint directory contains a malformed, nested, or unknown placeholder.
/// Guarantees: Only an exact engine-root path prefix is accepted, without leaking configured paths.
#[test]
fn checkpoint_directory_rejects_unresolved_placeholders() {
    for directory in [
        "${engine.state_dir}suffix/sql",
        "state/${engine.state_dir}/sql",
        "${env:SECRET}/sql",
        "${engine.state_dir}/${OTHER}/sql",
        "${engine.state_dir}/../sql",
        "state/${SENSITIVE",
        "state\0sql",
    ] {
        let error = validate_directory(directory).expect_err("invalid directory");
        assert!(!format!("{error} {error:?}").contains("SENSITIVE"));
    }
    for directory in [
        "${engine.state_dir}",
        "${engine.state_dir}/sql",
        ".otap-state/sql",
        "state/native-\u{e9}",
    ] {
        validate_directory(directory).expect("supported directory");
    }
}

/// Scenario: A receiver uses the engine prefix without a provisioned engine root.
/// Guarantees: Construction fails explicitly instead of consulting the environment or .otap-state.
#[test]
fn checkpoint_directory_requires_engine_capability() {
    let error = CheckpointStore::from_config(
        "${engine.state_dir}/sql",
        &context(),
        "receiver",
        "source",
        "fingerprint".into(),
    )
    .expect_err("missing engine root");
    assert!(
        error
            .to_string()
            .contains("engine.state_dir is not configured")
    );
    assert!(
        error
            .to_string()
            .contains("no OTAP_DF_STATE_DIR or .otap-state fallback")
    );
}

/// Scenario: An existing checkpoint uses an explicit relative or absolute native directory.
/// Guarantees: Construction preserves its namespace, acknowledged cursor, and exclusive ownership.
#[test]
fn checkpoint_directory_preserves_native_locations() {
    let directory = tempfile::tempdir_in(".").expect("state fixture");
    for root in [
        directory.path().to_owned(),
        directory.path().canonicalize().unwrap(),
    ] {
        let root = root.join("native-\u{e9}");
        let old = CheckpointStore::new(
            &root,
            "group",
            "pipeline",
            "receiver",
            "source",
            "fp".into(),
        );
        let (before, _) = old
            .write(0, &Cursor::Scalar(ScalarValue::Int64(42)))
            .unwrap();
        let current = CheckpointStore::from_config(
            root.to_str().unwrap(),
            &context(),
            "receiver",
            "source",
            "fp".into(),
        )
        .unwrap();
        assert_eq!(current.lease_key(), old.lease_key());
        assert_eq!(current.read().unwrap(), Some(before));
        let binding = SourceBinding::acquire(current).unwrap();
        assert!(matches!(
            SourceLease::acquire(old.lease_key()),
            Err(LeaseError::AlreadyOwned)
        ));
        drop(binding);
        std::fs::remove_dir_all(&root).unwrap();
    }
}

#[cfg(target_os = "linux")]
fn managed_fixture() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::Builder::new()
        .prefix("otap-checkpoint-test-")
        .tempdir_in(otel_arrow_dfe_engine::testing::state_dir_test_parent())
        .expect("create state fixture beneath trusted test parent");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

#[cfg(target_os = "linux")]
fn managed_context(path: &Path) -> PipelineContext {
    use otel_arrow_dfe_engine::state_dir::StateDirectory;
    ControllerContext::new(TelemetryRegistryHandle::new())
        .with_state_directory(
            StateDirectory::provision(path).expect("test parent must have trusted ancestors"),
        )
        .pipeline_context_with("group".into(), "pipeline".into(), 0, 1, 0)
}

/// Scenario: An engine root is configured but a source explicitly selects a different native directory.
/// Guarantees: Its existing checkpoint remains outside the managed root and is read without rebasing.
#[cfg(target_os = "linux")]
#[test]
fn checkpoint_directory_does_not_rebase_explicit_paths() {
    let directory = managed_fixture();
    let base = directory.path().canonicalize().unwrap();
    let root = base.join("managed");
    let context = managed_context(&root);
    let native = base.join("existing");
    let old = CheckpointStore::new(
        &native,
        "group",
        "pipeline",
        "receiver",
        "source",
        "fp".into(),
    );
    let (before, _) = old
        .write(0, &Cursor::Scalar(ScalarValue::Int64(42)))
        .unwrap();
    let current = CheckpointStore::from_config(
        native.to_str().unwrap(),
        &context,
        "receiver",
        "source",
        "fp".into(),
    )
    .unwrap();
    assert_eq!(current.lease_key(), old.lease_key());
    assert_eq!(current.read().unwrap(), Some(before));
    assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
}

/// Scenario: Existing SQL progress is selected through the engine prefix after its root is renamed.
/// Guarantees: Progress and leases use the retained directory, never a replacement at the configured path.
#[cfg(target_os = "linux")]
#[test]
fn checkpoint_directory_pins_engine_namespace_and_preserves_progress() {
    let directory = managed_fixture();
    let base = directory.path().canonicalize().unwrap();
    let root = base.join("state");
    let context = managed_context(&root);
    let old = CheckpointStore::new(
        &root.join("sql"),
        "group",
        "pipeline",
        "receiver",
        "source",
        "fp".into(),
    );
    let (before, _) = old
        .write(0, &Cursor::Scalar(ScalarValue::Int64(42)))
        .unwrap();
    let moved = base.join("moved");
    std::fs::rename(&root, &moved).unwrap();
    std::fs::create_dir(&root).unwrap();
    let current = CheckpointStore::from_config(
        "${engine.state_dir}/sql",
        &context,
        "receiver",
        "source",
        "fp".into(),
    )
    .unwrap();
    drop(context);
    assert_eq!(current.read().unwrap(), Some(before.clone()));
    let moved_store = CheckpointStore::new(
        &moved.join("sql"),
        "group",
        "pipeline",
        "receiver",
        "source",
        "fp".into(),
    );
    let binding = SourceBinding::acquire(current.clone()).unwrap();
    assert!(matches!(
        SourceLease::acquire(moved_store.lease_key()),
        Err(LeaseError::AlreadyOwned)
    ));
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    drop(binding);
    let (next, _) = current
        .write(before.revision, &Cursor::Scalar(ScalarValue::Int64(43)))
        .unwrap();
    assert_eq!(moved_store.read().unwrap(), Some(next));
}

/// Scenario: A configured suffix or identity namespace is a symlink outside the engine root.
/// Guarantees: Managed construction refuses the link without opening or populating the target.
#[cfg(target_os = "linux")]
#[test]
fn checkpoint_directory_refuses_managed_symlinks() {
    use std::os::unix::fs::symlink;
    let directory = managed_fixture();
    let base = directory.path().canonicalize().unwrap();
    let root = base.join("state");
    let context = managed_context(&root);
    let outside = base.join("outside");
    std::fs::create_dir(&outside).unwrap();
    symlink(&outside, root.join("sql")).unwrap();
    assert!(
        CheckpointStore::from_config(
            "${engine.state_dir}/sql",
            &context,
            "receiver",
            "source",
            "fp".into(),
        )
        .is_err()
    );
    std::fs::remove_file(root.join("sql")).unwrap();
    std::fs::create_dir(root.join("sql")).unwrap();
    symlink(&outside, root.join("sql").join("@v1")).unwrap();
    assert!(
        CheckpointStore::from_config(
            "${engine.state_dir}/sql",
            &context,
            "receiver",
            "source",
            "fp".into(),
        )
        .is_err()
    );
    assert_eq!(std::fs::read_dir(outside).unwrap().count(), 0);
}
