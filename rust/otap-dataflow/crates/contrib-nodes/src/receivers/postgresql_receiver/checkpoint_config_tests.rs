// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use otel_arrow_dfe_config::{error::Error, node::NodeUserConfig};
use otel_arrow_dfe_engine::{
    capability::{
        auth::basic_auth_provider::BasicAuthProvider as Registration,
        registry::{CapabilityRegistry, LocalCapabilityEntry},
    },
    context::{ControllerContext, PipelineContext},
    receiver::ReceiverWrapper,
    testing::{capability::resolve_bindings_for_test, receiver::TestRuntime, test_node},
};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_scraper::{CheckpointStore, LeaseError, SourceLease};
use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
use std::sync::Arc;

fn context() -> PipelineContext {
    ControllerContext::new(TelemetryRegistryHandle::new()).pipeline_context_with(
        "group".into(),
        "pipeline".into(),
        0,
        1,
        0,
    )
}

fn build(
    context: PipelineContext,
    raw: Value,
    runtime: &TestRuntime<OtapPdata>,
) -> Result<ReceiverWrapper<OtapPdata>, Error> {
    let mut registry = CapabilityRegistry::new();
    registry
        .register_local(
            std::any::TypeId::of::<Registration>(),
            LocalCapabilityEntry::new("test".into(), || -> Box<dyn std::any::Any> {
                Box::new(test_provider())
            }),
        )
        .unwrap();
    let capabilities = resolve_bindings_for_test(
        &std::collections::HashMap::from([("basic_auth_provider".into(), "test".into())]),
        &registry,
        &std::collections::HashSet::from(["test".into()]),
    )
    .unwrap();
    let mut node = NodeUserConfig::new_receiver_config(super::super::POSTGRESQL_RECEIVER_URN);
    node.config = raw;
    (super::super::POSTGRESQL_RECEIVER.create)(
        context,
        test_node(runtime.config().name.clone()),
        Arc::new(node),
        runtime.config(),
        &capabilities,
    )
}

/// Scenario: PostgreSQL selects an absent engine root or an unresolved checkpoint placeholder.
/// Guarantees: Factory and validation errors identify checkpoint configuration without literal-directory fallback.
#[test]
fn factory_checkpoint_directory_requires_engine_root() {
    let mut raw = config(&sql(KEY));
    raw["checkpoint"]["directory"] = json!("${engine.state_dir}/postgresql");
    let Err(error) = build(context(), raw.clone(), &TestRuntime::<OtapPdata>::new()) else {
        panic!("missing engine root");
    };
    assert!(
        error
            .to_string()
            .contains("engine.state_dir is not configured")
    );
    raw["checkpoint"]["directory"] = json!("${UNKNOWN}/postgresql");
    let error = (super::super::POSTGRESQL_RECEIVER.validate_config)(&raw).unwrap_err();
    assert!(error.to_string().contains("checkpoint.directory"));
    assert!(error.to_string().contains("placeholder"));
}

/// Scenario: PostgreSQL selects explicit native storage containing an existing acknowledged cursor.
/// Guarantees: Factory construction preserves progress and excludes another receiver from its original namespace.
#[test]
fn factory_checkpoint_directory_preserves_native_progress() {
    let directory = tempfile::tempdir_in(".").unwrap();
    let raw_root = directory.path().join("postgresql");
    let mut raw = config(&sql(KEY));
    raw["checkpoint"]["directory"] = json!(raw_root);
    preserves_progress(context(), raw, &raw_root);
}

fn preserves_progress(context: PipelineContext, raw: Value, native_root: &std::path::Path) {
    let validated = validate(&raw).unwrap();
    let runtime = TestRuntime::<OtapPdata>::new();
    let store = CheckpointStore::new(
        native_root,
        "group",
        "pipeline",
        runtime.config().name.as_ref(),
        &validated.config.source_id,
        validated.fingerprint,
    );
    let (before, _) = store
        .write(0, &validated.common.watermark().initial())
        .unwrap();
    let receiver = build(context, raw, &runtime).unwrap();
    assert!(matches!(
        SourceLease::acquire(store.lease_key()),
        Err(LeaseError::AlreadyOwned)
    ));
    assert_eq!(store.read().unwrap(), Some(before));
    drop(receiver);
    let _lease = SourceLease::acquire(store.lease_key()).unwrap();
}

/// Scenario: PostgreSQL selects an engine-owned root holding progress in the existing checkpoint layout.
/// Guarantees: The registered factory leases and preserves that cursor rather than choosing a fallback directory.
#[cfg(target_os = "linux")]
#[test]
fn factory_checkpoint_directory_uses_engine_root() {
    use otel_arrow_dfe_engine::state_dir::StateDirectory;
    let directory = tempfile::tempdir_in(".").unwrap();
    let root = directory.path().canonicalize().unwrap().join("state");
    let context = ControllerContext::new(TelemetryRegistryHandle::new())
        .with_state_directory(StateDirectory::provision(&root).unwrap())
        .pipeline_context_with("group".into(), "pipeline".into(), 0, 1, 0);
    let mut raw = config(&sql(KEY));
    raw["checkpoint"]["directory"] = json!("${engine.state_dir}/postgresql");
    preserves_progress(context, raw, &root.join("postgresql"));
}
