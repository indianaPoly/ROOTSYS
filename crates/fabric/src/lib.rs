use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use common::IntegrationRecord;

/// Merge IntegrationRecord JSONL files into a single JSONL stream.
///
/// When `dedupe` is true, records are deduplicated by
/// `(source, interface.name, interface.version, record_id)`.
pub fn merge_jsonl_files(
    inputs: &[impl AsRef<Path>],
    output: impl AsRef<Path>,
    dedupe: bool,
) -> Result<MergeStats, Box<dyn std::error::Error>> {
    let file = File::create(output)?;
    let mut writer = BufWriter::new(file);
    let mut seen = HashSet::new();

    let mut stats = MergeStats::default();

    for input in inputs {
        let file = File::open(input.as_ref())?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let record: IntegrationRecord = serde_json::from_str(&line)?;
            stats.total += 1;

            if dedupe {
                let key = record_key(&record);
                if !seen.insert(key) {
                    stats.duplicates += 1;
                    continue;
                }
            }

            let payload = serde_json::to_string(&record)?;
            writeln!(writer, "{}", payload)?;
            stats.written += 1;
        }
    }

    Ok(stats)
}

/// Summary statistics for a merge run.
#[derive(Debug, Default, Clone, Copy)]
pub struct MergeStats {
    pub total: usize,
    pub written: usize,
    pub duplicates: usize,
}

fn record_key(record: &IntegrationRecord) -> String {
    format!(
        "{}|{}|{}|{}",
        record.source, record.interface.name, record.interface.version, record.record_id
    )
}
