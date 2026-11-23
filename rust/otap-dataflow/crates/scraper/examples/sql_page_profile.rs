// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded SQL page timing with the platform default allocator.

#[path = "support/sql_page.rs"]
mod sql_page;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sql_page::run(false)
}
