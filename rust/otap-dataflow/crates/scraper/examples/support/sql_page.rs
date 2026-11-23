// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared deterministic fixtures and checks for the timing/heap executables.

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::{
    otlp::{OtlpProtoBytes, batching::make_bytes_batches_owned},
    proto::opentelemetry::{common::v1::any_value, logs::v1::LogsData},
};
use otel_arrow_dfe_scraper::database::{
    CellValue, ColumnMetadata, Cursor, CursorRow, DatabaseSystem, EncodedPage, OutputConfig,
    QueryPage, Row, ScalarValue, encode_page,
};
use prost::Message;
use serde_json::json;
use std::{error::Error, hint::black_box, io::Write, num::NonZeroU64, time::Instant};

const PAGE_LIMIT: u64 = 64 * 1024 * 1024;
const TARGET_BYTES: u64 = 64 * 1024;
const OBSERVED_TIME: u64 = 1_800_000_000_000_000_000;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Copy)]
enum Stage {
    Encode,
    Rebatch,
}

struct Fixture {
    stage: Stage,
    columns: usize,
    value_bytes: usize,
    rows: usize,
    source: String,
    repetitions: usize,
}

impl Fixture {
    fn from_args(heap: bool) -> Result<Self> {
        let args: Vec<_> = std::env::args().skip(1).collect();
        if args.len() != 5 {
            return Err(
                "usage: <example> encode|rebatch narrow|wide rows source_id_bytes repetitions"
                    .into(),
            );
        }
        let stage = match args[0].as_str() {
            "encode" => Stage::Encode,
            "rebatch" => Stage::Rebatch,
            _ => return Err("stage must be encode or rebatch".into()),
        };
        let (columns, value_bytes) = match args[1].as_str() {
            "narrow" => (4, 32),
            "wide" => (32, 1024),
            _ => return Err("shape must be narrow or wide".into()),
        };
        let rows = args[2].parse::<usize>()?;
        let source_bytes = args[3].parse::<usize>()?;
        let repetitions = args[4].parse::<usize>()?;
        if !(1..=4096).contains(&rows)
            || !(1..=1024).contains(&source_bytes)
            || !(1..=20).contains(&repetitions)
            || rows * repetitions > 32_768
        {
            return Err(
                "bounds: rows=1..4096, source_id_bytes=1..1024, repetitions=1..20, total rows<=32768"
                    .into(),
            );
        }
        if heap && repetitions != 1 {
            return Err("heap mode requires repetitions=1; repeat in fresh processes".into());
        }
        Ok(Self {
            stage,
            columns,
            value_bytes,
            rows,
            source: "s".repeat(source_bytes),
            repetitions,
        })
    }

    fn cell(&self, row: usize, column: usize) -> CellValue {
        match column % 4 {
            0 => CellValue::Int64(row as i64),
            1 => CellValue::String(
                char::from(b'a' + (row % 26) as u8)
                    .to_string()
                    .repeat(self.value_bytes),
            ),
            2 => CellValue::Bytes(vec![(row % 251) as u8; self.value_bytes / 2]),
            _ => CellValue::Null,
        }
    }

    fn page(&self) -> QueryPage {
        QueryPage {
            columns: (0..self.columns)
                .map(|column| ColumnMetadata {
                    name: format!("field_{column:02}"),
                    source_type: ["BIGINT", "VARCHAR", "VARBINARY", "VARCHAR"][column % 4]
                        .to_owned(),
                    nullable: column % 4 == 3,
                })
                .collect(),
            rows: (0..self.rows)
                .map(|row| CursorRow {
                    row: Row {
                        values: (0..self.columns)
                            .map(|column| self.cell(row, column))
                            .collect(),
                    },
                    cursor: ScalarValue::Int64(row as i64).into(),
                })
                .collect(),
        }
    }

    fn encode(&self, page: QueryPage, limit: u64) -> Result<EncodedPage> {
        encode_page(
            page,
            DatabaseSystem::PostgreSQL,
            &self.source,
            &OutputConfig::default(),
            OBSERVED_TIME,
            limit,
        )?
        .ok_or_else(|| "fixture unexpectedly encoded an empty page".into())
    }

    fn prepare(&self) -> Result<Input> {
        match self.stage {
            Stage::Encode => Ok(Input::Rows(self.page())),
            Stage::Rebatch => Ok(self.rebatch_input(self.encode(self.page(), PAGE_LIMIT)?)),
        }
    }

    fn verify_encoded_metadata(&self, encoded: &EncodedPage) {
        assert_eq!(encoded.row_count, self.rows);
        assert_eq!(encoded.deferred_rows, 0);
        assert_eq!(encoded.event_time_fallbacks, 0);
        assert_eq!(
            encoded.candidate,
            Cursor::from(ScalarValue::Int64(self.rows as i64 - 1))
        );
        let bytes = encoded
            .pdata
            .payload_ref()
            .encoded_bytes()
            .expect("encoded OTLP");
        assert_eq!(encoded.encoded_bytes, bytes.len());
        assert!(bytes.len() as u64 <= PAGE_LIMIT);
    }

    fn rebatch_input(&self, encoded: EncodedPage) -> Input {
        self.verify_encoded_metadata(&encoded);
        let bytes = encoded
            .pdata
            .payload_ref()
            .encoded_bytes()
            .expect("encoded OTLP");
        Input::Bytes(vec![OtlpProtoBytes::ExportLogsRequest(bytes.clone())])
    }

    fn execute(&self, input: Input) -> Result<Output> {
        match input {
            Input::Rows(page) => Ok(Output::Encoded(self.encode(page, PAGE_LIMIT)?)),
            Input::Bytes(bytes) => {
                let batches = make_bytes_batches_owned(
                    SignalType::Logs,
                    NonZeroU64::new(TARGET_BYTES),
                    NonZeroU64::new(4096),
                    NonZeroU64::new(PAGE_LIMIT),
                    NonZeroU64::new(4096),
                    bytes,
                )?;
                Ok(Output::Rebatched(batches))
            }
        }
    }
}

enum Input {
    Rows(QueryPage),
    Bytes(Vec<OtlpProtoBytes>),
}

impl Input {
    // Only heap setup copies input. The original, untracked fixture is dropped
    // before the measured operation, so no duplicate page is retained across it.
    fn owned_copy(&self) -> Self {
        match self {
            Self::Rows(page) => Self::Rows(page.clone()),
            Self::Bytes(inputs) => Self::Bytes(
                inputs
                    .iter()
                    .map(|input| {
                        OtlpProtoBytes::new_from_bytes(SignalType::Logs, input.as_bytes().to_vec())
                    })
                    .collect(),
            ),
        }
    }
}

enum Output {
    Encoded(EncodedPage),
    Rebatched(otel_arrow_dfe_pdata::otlp::batching::BytesBatches),
}

impl Output {
    fn verify(&self, fixture: &Fixture) -> (usize, usize) {
        match self {
            Self::Encoded(encoded) => {
                fixture.verify_encoded_metadata(encoded);
                let bytes = encoded
                    .pdata
                    .payload_ref()
                    .encoded_bytes()
                    .expect("encoded OTLP");
                verify_logs(std::iter::once(bytes.as_ref()), fixture)
            }
            Self::Rebatched(batches) => {
                assert_eq!(batches.budget_fallbacks, 0);
                assert!(batches.completed_inputs.is_empty());
                for (bytes, provenance) in &batches.batches {
                    assert_eq!(*provenance, 0..=0);
                    assert!(bytes.num_bytes() as u64 <= TARGET_BYTES);
                }
                verify_logs(
                    batches.batches.iter().map(|(bytes, _)| bytes.as_bytes()),
                    fixture,
                )
            }
        }
    }
}

fn verify_logs<'a>(
    payloads: impl IntoIterator<Item = &'a [u8]>,
    fixture: &Fixture,
) -> (usize, usize) {
    let mut row = 0;
    let mut encoded_bytes = 0;
    let mut output_batches = 0;
    for bytes in payloads {
        encoded_bytes += bytes.len();
        output_batches += 1;
        let logs = LogsData::decode(bytes).expect("valid OTLP");
        assert_eq!(logs.resource_logs.len(), 1);
        for resource in logs.resource_logs {
            let attributes = resource.resource.expect("resource identity").attributes;
            for (key, expected) in [
                ("db.system.name", "postgresql"),
                ("receiver.database.source_id", fixture.source.as_str()),
            ] {
                let value = attributes
                    .iter()
                    .find(|kv| kv.key == key)
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|value| value.value.as_ref())
                    .expect("resource attribute");
                assert_eq!(*value, any_value::Value::StringValue(expected.to_owned()));
            }
            assert_eq!(resource.scope_logs.len(), 1);
            for scope in resource.scope_logs {
                assert_eq!(
                    scope.scope.expect("scope identity").name,
                    "otel-arrow.database_receiver"
                );
                for record in scope.log_records {
                    assert_eq!(record.time_unix_nano, OBSERVED_TIME);
                    assert_eq!(record.observed_time_unix_nano, OBSERVED_TIME);
                    let Some(any_value::Value::KvlistValue(body)) =
                        record.body.and_then(|body| body.value)
                    else {
                        panic!("expected database body");
                    };
                    assert_eq!(body.values.len(), fixture.columns);
                    for (column, field) in body.values.into_iter().enumerate() {
                        assert_eq!(field.key, format!("field_{column:02}"));
                        let actual = field.value.expect("body value").value;
                        let expected = match fixture.cell(row, column) {
                            CellValue::Int64(value) => Some(any_value::Value::IntValue(value)),
                            CellValue::String(value) => Some(any_value::Value::StringValue(value)),
                            CellValue::Bytes(value) => Some(any_value::Value::BytesValue(value)),
                            CellValue::Null => None,
                            _ => unreachable!("fixture cell model"),
                        };
                        assert_eq!(actual, expected);
                    }
                    row += 1;
                }
            }
        }
    }
    assert_eq!(row, fixture.rows, "preserve row order and duplicate counts");
    (encoded_bytes, output_batches)
}

pub(super) fn run(heap: bool) -> Result<()> {
    let fixture = Fixture::from_args(heap)?;
    let page = fixture.page();
    let normalized_row_bytes: u64 = page.rows.iter().map(|row| row.row.normalized_size()).sum();
    assert!(normalized_row_bytes <= PAGE_LIMIT);
    drop(page);
    // Verify once and warm lazy initialization outside either measurement.
    let expected = fixture.execute(fixture.prepare()?)?.verify(&fixture);
    let mut report = json!({
        "mode": if heap { "heap" } else { "timing" },
        "stage": match fixture.stage { Stage::Encode => "one_shot_encode", Stage::Rebatch => "otlp_byte_rebatch" },
        "columns": fixture.columns,
        "value_bytes": fixture.value_bytes,
        "rows_per_page": fixture.rows,
        "source_id_bytes": fixture.source.len(),
        "repetitions": fixture.repetitions,
        "page_limit_bytes": PAGE_LIMIT,
        "rebatch_target_bytes": TARGET_BYTES,
        "normalized_row_bytes": normalized_row_bytes,
        "encoded_bytes_per_page": expected.0,
        "output_batches": expected.1,
        "debug_assertions": cfg!(debug_assertions),
        "copy_count": null,
        "rss_bytes": null,
    });
    if heap {
        let prepared = fixture.prepare()?;
        // Single-threaded executable: fixture setup and this operation are the
        // only allocations inside this profiler. No runtime or test harness runs.
        let profiler = dhat::Profiler::builder().testing().build();
        let input = prepared.owned_copy();
        drop(prepared);
        let before = dhat::HeapStats::get();
        let output = fixture.execute(black_box(input))?;
        let after = dhat::HeapStats::get();
        drop(profiler);
        assert_eq!(output.verify(&fixture), expected);
        report["heap"] = json!({
            "operation_allocation_events": after.total_blocks - before.total_blocks,
            "operation_allocated_bytes": after.total_bytes - before.total_bytes,
            "input_live_bytes": before.curr_bytes,
            "output_live_bytes": after.curr_bytes,
            "setup_peak_bytes": before.max_bytes,
            "lifetime_peak_bytes": after.max_bytes,
        });
        drop(black_box(output));
    } else {
        let mut elapsed_ns = Vec::with_capacity(fixture.repetitions);
        for _ in 0..fixture.repetitions {
            let input = fixture.prepare()?;
            let started = Instant::now();
            let output = fixture.execute(black_box(input))?;
            elapsed_ns.push(started.elapsed().as_nanos());
            assert_eq!(output.verify(&fixture), expected);
            drop(black_box(output));
        }
        let total_ns: u128 = elapsed_ns.iter().sum();
        report["rows_per_second"] =
            json!((fixture.rows * fixture.repetitions) as f64 * 1_000_000_000.0 / total_ns as f64);
        report["elapsed_ns"] = json!(elapsed_ns);
    }
    serde_json::to_writer(std::io::stdout().lock(), &report)?;
    writeln!(std::io::stdout().lock())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(stage: Stage, wide: bool, rows: usize, source_bytes: usize) -> Fixture {
        Fixture {
            stage,
            columns: if wide { 32 } else { 4 },
            value_bytes: if wide { 1024 } else { 32 },
            rows,
            source: "s".repeat(source_bytes),
            repetitions: 1,
        }
    }

    /// Scenario: Narrow/wide pages with short/long source IDs pass through both measured boundaries.
    /// Guarantees: Every scalar, text, binary and null cell, resource ID, row order and cursor survives.
    #[test]
    fn profile_matrix_preserves_content_and_bounds() {
        for wide in [false, true] {
            for rows in [32, 1024] {
                for source_bytes in [8, 256] {
                    for stage in [Stage::Encode, Stage::Rebatch] {
                        let fixture = fixture(stage, wide, rows, source_bytes);
                        _ = fixture
                            .execute(fixture.prepare().expect("input"))
                            .expect("operation")
                            .verify(&fixture);
                    }
                }
            }
        }
    }

    /// Scenario: Rebatch preparation receives valid payload bytes but corrupted encoder metadata.
    /// Guarantees: Row count, deferred rows, cursor, reported size and fallback errors fail before metadata is discarded.
    #[test]
    fn rebatch_preparation_checks_encoder_metadata() {
        let fixture = fixture(Stage::Rebatch, false, 32, 8);
        let valid = fixture.rebatch_input(
            fixture
                .encode(fixture.page(), PAGE_LIMIT)
                .expect("encoded input"),
        );
        _ = fixture
            .execute(valid)
            .expect("valid rebatching")
            .verify(&fixture);
        let corruptions: [fn(&mut EncodedPage); 5] = [
            |encoded| encoded.row_count -= 1,
            |encoded| encoded.deferred_rows = 1,
            |encoded| encoded.candidate = ScalarValue::Int64(-1).into(),
            |encoded| encoded.encoded_bytes += 1,
            |encoded| encoded.event_time_fallbacks = 1,
        ];
        for corrupt in corruptions {
            let mut encoded = fixture
                .encode(fixture.page(), PAGE_LIMIT)
                .expect("encoded input");
            corrupt(&mut encoded);
            // Each attempt owns its payload and changes no state shared with the next attempt.
            let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                _ = fixture.rebatch_input(encoded);
            }));
            assert!(
                rejected.is_err(),
                "rebatch preparation accepted corrupt metadata"
            );
        }
    }

    /// Scenario: The encoded byte budget fits exactly one row but not a two-row input page.
    /// Guarantees: Prefix encoding respects the exact budget, defers one row and returns its predecessor's cursor.
    #[test]
    fn exact_byte_limit_preserves_prefix_cursor() {
        let mut fixture = fixture(Stage::Encode, false, 1, 256);
        let first = fixture.encode(fixture.page(), PAGE_LIMIT).expect("one row");
        let limit = first.encoded_bytes as u64;
        fixture.rows = 2;
        let prefix = fixture.encode(fixture.page(), limit).expect("prefix");
        assert_eq!(prefix.encoded_bytes as u64, limit);
        assert_eq!(prefix.row_count, 1);
        assert_eq!(prefix.deferred_rows, 1);
        assert_eq!(prefix.candidate, first.candidate);
        fixture.rows = 1;
        let bytes = prefix
            .pdata
            .payload_ref()
            .encoded_bytes()
            .expect("prefix OTLP");
        _ = verify_logs(std::iter::once(bytes.as_ref()), &fixture);
        assert!(matches!(
            fixture.encode(fixture.page(), limit - 1),
            Err(error) if matches!(
                error.downcast_ref::<otel_arrow_dfe_scraper::database::OtlpMappingError>(),
                Some(otel_arrow_dfe_scraper::database::OtlpMappingError::OversizedFirstRow { .. })
            )
        ));
    }
}
