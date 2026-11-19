// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::{engine::OtelDataflowSpec, error::Error};
use otel_arrow_dfe_engine::{ReceiverFactory, context::ControllerContext};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_scraper::{
    CheckpointStore,
    database::{Cursor, ScalarValue},
};
use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
use std::path::Path;
use std::process::Command;

struct Example {
    name: &'static str,
    setting: &'static str,
    yaml: &'static str,
    factory: &'static ReceiverFactory<OtapPdata>,
}

fn examples() -> Vec<Example> {
    vec![
        #[cfg(feature = "postgresql")]
        Example {
            name: "postgresql",
            setting: "POSTGRESQL_CHECKPOINT_DIRECTORY",
            yaml: include_str!("../../../../configs/postgresql-console.yaml"),
            factory: &super::postgresql_receiver::POSTGRESQL_RECEIVER,
        },
        #[cfg(feature = "oracle")]
        Example {
            name: "oracle",
            setting: "ORACLE_CHECKPOINT_DIRECTORY",
            yaml: include_str!("../../../../configs/oracle-oci-console.yaml"),
            factory: &super::oracle_receiver::ORACLE_RECEIVER,
        },
    ]
}

fn check_example(example: &Example, case: &str) {
    let result = OtelDataflowSpec::from_yaml(example.yaml);
    match case {
        "missing" | "legacy-only" => {
            assert!(
                matches!(result, Err(Error::EnvVarNotFound { ref var }) if var == example.setting),
                "{result:?}"
            );
            return;
        }
        "non-utf8" => {
            assert!(
                matches!(result, Err(Error::EnvVarCannotBeParsed { ref var }) if var == example.setting),
                "{result:?}"
            );
            return;
        }
        _ => (),
    }
    let spec = result.expect("load the actual console example");
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
    let validation = (example.factory.validate_config)(&node.config);
    if case == "empty" {
        let error = validation.expect_err("reject empty directory before database work");
        let message = error.to_string();
        assert!(message.contains("checkpoint.directory"), "{message}");
        assert!(message.contains("non-empty"), "{message}");
        return;
    }
    validation.expect("validate explicit native checkpoint configuration");
    let expected = std::env::var(example.setting).expect("Unicode directory");
    let directory = node.config["checkpoint"]["directory"]
        .as_str()
        .expect("checkpoint directory string");
    assert_eq!(directory, expected, "no default or appended suffix");
    let source = node.config["source_id"].as_str().expect("source ID string");
    let previous = CheckpointStore::new(
        Path::new(&expected),
        "default",
        "main",
        name.as_ref(),
        source,
        "fingerprint".into(),
    );
    let (acknowledged, _) = previous
        .write(0, &Cursor::Scalar(ScalarValue::Int64(42)))
        .expect("seed acknowledged native progress");
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
        Some(acknowledged)
    );
}

/// Scenario: Actual console YAML loads with missing, legacy-only, empty, explicit, or non-UTF8 directory settings.
/// Guarantees: Missing/invalid settings fail before DB work; full native paths select existing acknowledged progress.
#[test]
fn console_checkpoint_directory_environment_contract() {
    const CASE: &str = "OTAP_CHECKPOINT_EXAMPLE_CASE";
    const EXAMPLE: &str = "OTAP_CHECKPOINT_EXAMPLE_NAME";
    if let Ok(case) = std::env::var(CASE) {
        let name = std::env::var(EXAMPLE).expect("example name");
        let example = examples().into_iter().find(|e| e.name == name).unwrap();
        check_example(&example, &case);
        return;
    }
    let directory = tempfile::tempdir().expect("isolated example state");
    let base = directory.path().canonicalize().unwrap();
    for example in examples() {
        let mut cases = vec!["missing", "legacy-only", "empty", "explicit"];
        #[cfg(unix)]
        cases.push("non-utf8");
        for case in cases.drain(..) {
            let mut command = Command::new(std::env::current_exe().expect("test executable"));
            let _ = command
                .args([
                    "--exact",
                    "receivers::checkpoint_example_tests::console_checkpoint_directory_environment_contract",
                    "--test-threads=1",
                    "--nocapture",
                ])
                .current_dir(&base)
                .env(CASE, case)
                .env(EXAMPLE, example.name)
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
            match case {
                "legacy-only" => {
                    let _ = command.env("OTAP_DF_STATE_DIR", base.join("old-root"));
                }
                "empty" => {
                    let _ = command.env(example.setting, "");
                }
                "explicit" => {
                    let _ = command
                        .env("OTAP_DF_STATE_DIR", base.join("must-not-be-selected"))
                        .env(
                            example.setting,
                            base.join(format!("{} full-path-\u{e9}", example.name)),
                        );
                }
                #[cfg(unix)]
                "non-utf8" => {
                    use std::os::unix::ffi::OsStringExt;
                    let _ = command.env(
                        example.setting,
                        std::ffi::OsString::from_vec(b"invalid-\xff".to_vec()),
                    );
                }
                _ => (),
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
