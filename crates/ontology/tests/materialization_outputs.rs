use std::fs;
use std::path::PathBuf;

use common::IntegrationRecord;
use ontology::{BasicOntologyMaterializer, OntologyMaterializer};
use serde_json::Value;

#[test]
fn materialization_output_matches_expected_fixture() {
    let materializer = BasicOntologyMaterializer;

    let actual: Vec<Value> = load_input_records()
        .iter()
        .flat_map(|record| materializer.materialize_jsonl_lines(record))
        .map(|line| serde_json::from_str::<Value>(&line).expect("materialized line should be JSON"))
        .collect();

    let expected = load_expected_output();

    assert_eq!(actual, expected);
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/ontology")
}

fn load_input_records() -> Vec<IntegrationRecord> {
    let input_path = fixtures_dir().join("materialization.input.jsonl");
    let content = fs::read_to_string(&input_path)
        .unwrap_or_else(|_| panic!("failed to read fixture input: {}", input_path.display()));

    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<IntegrationRecord>(line)
                .expect("fixture input line must parse as IntegrationRecord")
        })
        .collect()
}

fn load_expected_output() -> Vec<Value> {
    let expected_path = fixtures_dir().join("materialization.expected.jsonl");
    let content = fs::read_to_string(&expected_path).unwrap_or_else(|_| {
        panic!(
            "failed to read expected fixture: {}",
            expected_path.display()
        )
    });

    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<Value>(line).expect("expected line should be valid JSON")
        })
        .collect()
}
