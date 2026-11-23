// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Isolated SQL page heap profiling, separate from throughput timing.

#[path = "support/sql_page.rs"]
mod sql_page;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sql_page::run(true)
}
