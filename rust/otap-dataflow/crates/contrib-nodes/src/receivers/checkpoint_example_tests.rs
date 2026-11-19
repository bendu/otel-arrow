// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::engine::OtelDataflowSpec;
use otel_arrow_dfe_engine::{ReceiverFactory, context::ControllerContext};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_scraper::{CheckpointStore, LeaseError, SourceLease, database::Cursor};
use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
use std::path::{Path, PathBuf};
use std::process::Command;

struct Example {
    name: &'static str,
    yaml: &'static str,
    factory: &'static ReceiverFactory<OtapPdata>,
}

fn examples() -> Vec<Example> {
    vec![
        #[cfg(feature = "postgresql")]
        Example {
            name: "postgresql",
            yaml: include_str!("../../../../configs/postgresql-console.yaml"),
            factory: &super::postgresql_receiver::POSTGRESQL_RECEIVER,
        },
        #[cfg(feature = "oracle")]
        Example {
            name: "oracle",
            yaml: include_str!("../../../../configs/oracle-oci-console.yaml"),
            factory: &super::oracle_receiver::ORACLE_RECEIVER,
        },
    ]
}

fn literal_directory_yaml(example: &Example, path: &Path) -> String {
    let path = path.to_str().expect("Unicode YAML checkpoint path");
    // The production loader expands dollars before YAML deserialization.
    let scalar = serde_yaml::to_string(&path.replace('$', "$$")).expect("serialize native path");
    assert_eq!(example.yaml.matches("directory: ''").count(), 1);
    example.yaml.replacen(
        "directory: ''",
        &format!("directory: {}", scalar.trim_end()),
        1,
    )
}

fn check_example(example: &Example, case: &str, native_root: &Path) {
    let literal = matches!(case, "literal" | "placeholder-literal");
    let yaml = if literal {
        literal_directory_yaml(example, native_root)
    } else {
        example.yaml.to_owned()
    };
    let spec = OtelDataflowSpec::from_yaml(&yaml).expect("load actual console example");
    let pipeline = spec
        .groups
        .values()
        .next()
        .expect("example pipeline group")
        .pipelines
        .values()
        .next()
        .expect("example pipeline");
    let (name, node) = pipeline
        .nodes()
        .iter()
        .find(|(_, node)| node.config.get("checkpoint").is_some())
        .expect("SQL receiver");
    let directory = node.config["checkpoint"]["directory"]
        .as_str()
        .expect("checkpoint directory string");
    let validation = (example.factory.validate_config)(&node.config);
    if !literal || case == "placeholder-literal" {
        let error =
            validation.expect_err("reject unsupported checkpoint path before database work");
        let message = error.to_string();
        assert!(message.contains("checkpoint.directory"), "{message}");
        if case == "placeholder-literal" {
            assert_eq!(Path::new(directory).as_os_str(), native_root.as_os_str());
            assert!(message.contains("placeholder"), "{message}");
        } else {
            assert_eq!(directory, "", "environment cannot replace the empty marker");
            assert!(message.contains("non-empty"), "{message}");
        }
        assert!(!native_root.exists(), "validation must not create state");
        return;
    }
    validation.expect("validate explicitly edited native checkpoint configuration");
    assert_eq!(
        Path::new(directory).as_os_str(),
        native_root.as_os_str(),
        "YAML and dollar escaping must preserve every native path byte"
    );
    let source = node.config["source_id"].as_str().expect("source ID string");
    let previous = CheckpointStore::new(
        native_root,
        "default",
        "main",
        name.as_ref(),
        source,
        "fingerprint".into(),
    );
    let watermark = &node.config["watermark"];
    let cursor = Cursor::composite(
        watermark["timestamp"]["initial"]
            .as_str()
            .expect("initial timestamp")
            .to_owned(),
        watermark["tie_breaker"]["initial"]
            .as_i64()
            .expect("initial tie breaker"),
    );
    let (acknowledged, _) = previous.write(0, &cursor).expect("seed native progress");
    let prior_lease =
        SourceLease::acquire(previous.lease_key()).expect("hold previous native lease");
    let generation = prior_lease.generation();
    let pipeline_context = ControllerContext::new(TelemetryRegistryHandle::new())
        .pipeline_context_with("default".into(), "main".into(), 0, 1, 0);
    let current = CheckpointStore::from_config(
        directory,
        &pipeline_context,
        name.as_ref(),
        source,
        "fingerprint".into(),
    )
    .expect("select explicit native checkpoint");
    assert_eq!(current.lease_key(), previous.lease_key());
    assert_eq!(
        current.read().expect("read acknowledged progress"),
        Some(acknowledged.clone())
    );
    assert!(matches!(
        SourceLease::acquire(current.lease_key()),
        Err(LeaseError::AlreadyOwned)
    ));
    drop(prior_lease);
    let resumed = SourceLease::acquire(current.lease_key()).expect("resume original ownership");
    assert_eq!(resumed.generation(), generation + 1);
    assert!(matches!(
        SourceLease::acquire(previous.lease_key()),
        Err(LeaseError::AlreadyOwned)
    ));
    assert_eq!(
        current.read().expect("progress after lease reacquisition"),
        Some(acknowledged)
    );
}

/// Scenario: Shipped console examples retain an empty marker despite checkpoint environment settings.
/// Guarantees: Startup validation fails until YAML is edited; escaped literals resume the exact cursor and exclusive lease.
#[test]
fn console_checkpoint_directory_literal_contract() {
    const CASE: &str = "OTAP_CHECKPOINT_EXAMPLE_CASE";
    const EXAMPLE: &str = "OTAP_CHECKPOINT_EXAMPLE_NAME";
    const ROOT: &str = "OTAP_CHECKPOINT_EXAMPLE_NATIVE_ROOT";
    if let Ok(case) = std::env::var(CASE) {
        let name = std::env::var(EXAMPLE).expect("example name");
        let example = examples().into_iter().find(|e| e.name == name).unwrap();
        let root = PathBuf::from(std::env::var_os(ROOT).expect("native checkpoint root"));
        check_example(&example, &case, &root);
        return;
    }
    let directory = tempfile::tempdir().expect("isolated example state");
    let base = directory.path().canonicalize().unwrap();
    for example in examples() {
        let mut cases = vec![
            "unchanged",
            "legacy-only",
            "introduced-only",
            "all-env",
            "empty-env",
            "literal",
            "placeholder-literal",
        ];
        #[cfg(unix)]
        cases.push("non-utf8-env");
        for case in cases.drain(..) {
            let component = if case == "placeholder-literal" {
                "pair'' path-\u{e9} ${UNKNOWN}"
            } else {
                "pair'' and single' path-\u{e9} $cash"
            };
            let native_root = base.join(example.name).join(case).join(component);
            let mut command = Command::new(std::env::current_exe().expect("test executable"));
            let _ = command
                .args([
                    "--exact",
                    "receivers::checkpoint_example_tests::console_checkpoint_directory_literal_contract",
                    "--test-threads=1",
                    "--nocapture",
                ])
                .current_dir(&base)
                .env(CASE, case)
                .env(EXAMPLE, example.name)
                .env(ROOT, &native_root)
                .env_remove("POSTGRESQL_CHECKPOINT_DIRECTORY")
                .env_remove("ORACLE_CHECKPOINT_DIRECTORY")
                .env_remove("OTAP_DF_STATE_DIR")
                .env("PG_HOST", "localhost")
                .env("PG_USERNAME_FILE", "username")
                .env("PG_PASSWORD_FILE", "password")
                .env("PG_CA_FILE", "ca.pem")
                .env("ORACLE_PASSWORD_FILE", "password")
                .env("ORACLE_CONNECT_STRING", "//localhost:1521/FREEPDB1")
                .env("ORACLE_INSTANT_CLIENT_DIR", "instantclient");
            if matches!(case, "legacy-only" | "all-env" | "literal") {
                let _ = command.env("OTAP_DF_STATE_DIR", &native_root);
            }
            if matches!(case, "introduced-only" | "all-env" | "literal") {
                let _ = command
                    .env("POSTGRESQL_CHECKPOINT_DIRECTORY", &native_root)
                    .env("ORACLE_CHECKPOINT_DIRECTORY", &native_root);
            }
            if case == "empty-env" {
                let _ = command
                    .env("OTAP_DF_STATE_DIR", "")
                    .env("POSTGRESQL_CHECKPOINT_DIRECTORY", "")
                    .env("ORACLE_CHECKPOINT_DIRECTORY", "");
            }
            #[cfg(unix)]
            if case == "non-utf8-env" {
                use std::os::unix::ffi::OsStringExt;
                let invalid = std::ffi::OsString::from_vec(b"invalid-\xff".to_vec());
                let _ = command
                    .env("OTAP_DF_STATE_DIR", &invalid)
                    .env("POSTGRESQL_CHECKPOINT_DIRECTORY", &invalid)
                    .env("ORACLE_CHECKPOINT_DIRECTORY", &invalid);
            }
            let output = command.output().expect("console example child");
            assert!(
                output.status.success(),
                "{} {case}: {}\n{}",
                example.name,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
