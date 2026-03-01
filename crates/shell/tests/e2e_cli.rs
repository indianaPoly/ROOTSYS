use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn db_interface_run_emits_output_and_metrics_log() {
    let temp_dir = temp_test_dir("db_run");
    let output_path = temp_dir.join("mes.output.jsonl");

    let output = run_shell(&vec![
        "--interface".to_string(),
        path_to_string(&repo_root().join("tests/fixtures/interfaces/mes.db.json")),
        "--contract-registry".to_string(),
        path_to_string(&repo_root().join("system/contracts/reference/allowlist.json")),
        "--output".to_string(),
        path_to_string(&output_path),
    ]);

    assert!(
        output.status.success(),
        "shell command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let content = fs::read_to_string(&output_path)
        .unwrap_or_else(|_| panic!("expected output file: {}", output_path.display()));
    assert!(
        !content.trim().is_empty(),
        "integration output should not be empty"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pipeline_metrics"),
        "stdout should include structured metrics event"
    );
}

#[test]
fn replay_from_file_dlq_recovers_records_with_permissive_interface() {
    let temp_dir = temp_test_dir("replay_flow");
    let input_path = temp_dir.join("input.jsonl");
    let strict_interface_path = temp_dir.join("strict.interface.json");
    let permissive_interface_path = temp_dir.join("permissive.interface.json");
    let strict_output_path = temp_dir.join("strict.output.jsonl");
    let strict_dlq_path = temp_dir.join("strict.output.dlq.jsonl");
    let replay_output_path = temp_dir.join("replay.output.jsonl");

    fs::write(&input_path, "{\"foo\":\"bar\"}\n").expect("failed to write input fixture");

    let strict_interface = serde_json::json!({
        "name": "mes",
        "version": "v1",
        "driver": {
            "kind": "jsonl"
        },
        "payload_format": "json",
        "record_id_policy": "strict",
        "record_id_paths": ["/defect_id"],
        "required_paths": ["/defect_id"]
    });
    fs::write(
        &strict_interface_path,
        serde_json::to_string_pretty(&strict_interface).expect("strict interface json"),
    )
    .expect("failed to write strict interface");

    let permissive_interface = serde_json::json!({
        "name": "mes",
        "version": "v1",
        "driver": {
            "kind": "jsonl"
        },
        "payload_format": "json",
        "record_id_policy": "hash_fallback",
        "record_id_paths": []
    });
    fs::write(
        &permissive_interface_path,
        serde_json::to_string_pretty(&permissive_interface).expect("permissive interface json"),
    )
    .expect("failed to write permissive interface");

    let first_run = run_shell(&vec![
        "--interface".to_string(),
        path_to_string(&strict_interface_path),
        "--input".to_string(),
        path_to_string(&input_path),
        "--contract-registry".to_string(),
        path_to_string(&repo_root().join("system/contracts/reference/allowlist.json")),
        "--output".to_string(),
        path_to_string(&strict_output_path),
    ]);
    assert!(
        first_run.status.success(),
        "strict run failed: {}",
        String::from_utf8_lossy(&first_run.stderr)
    );

    let dlq_content = fs::read_to_string(&strict_dlq_path)
        .unwrap_or_else(|_| panic!("expected DLQ file: {}", strict_dlq_path.display()));
    assert!(
        !dlq_content.trim().is_empty(),
        "DLQ should contain rejected rows"
    );

    let replay_run = run_shell(&vec![
        "--interface".to_string(),
        path_to_string(&permissive_interface_path),
        "--contract-registry".to_string(),
        path_to_string(&repo_root().join("system/contracts/reference/allowlist.json")),
        "--output".to_string(),
        path_to_string(&replay_output_path),
        "--replay-dlq".to_string(),
        path_to_string(&strict_dlq_path),
        "--replay-dlq-source".to_string(),
        "file".to_string(),
    ]);
    assert!(
        replay_run.status.success(),
        "replay run failed: {}",
        String::from_utf8_lossy(&replay_run.stderr)
    );

    let replay_content = fs::read_to_string(&replay_output_path)
        .unwrap_or_else(|_| panic!("expected replay output: {}", replay_output_path.display()));
    assert!(
        !replay_content.trim().is_empty(),
        "replay output should contain recovered records"
    );
}

#[test]
fn local_mvp_fixture_scenario_outputs_expected_records_and_merge() {
    let temp_dir = temp_test_dir("local_mvp_flow");
    let mes_output = temp_dir.join("mes.output.jsonl");
    let qms_output = temp_dir.join("qms.output.jsonl");
    let stream_output = temp_dir.join("stream.output.jsonl");
    let merged_output = temp_dir.join("merged.output.jsonl");

    let mes_run = run_shell(&[
        "--interface".to_string(),
        path_to_string(&repo_root().join("tests/fixtures/interfaces/mes.db.json")),
        "--contract-registry".to_string(),
        path_to_string(&repo_root().join("system/contracts/reference/allowlist.json")),
        "--output".to_string(),
        path_to_string(&mes_output),
    ]);
    assert!(
        mes_run.status.success(),
        "mes shell run failed: {}",
        String::from_utf8_lossy(&mes_run.stderr)
    );

    let qms_run = run_shell(&[
        "--interface".to_string(),
        path_to_string(&repo_root().join("tests/fixtures/interfaces/qms.db.json")),
        "--contract-registry".to_string(),
        path_to_string(&repo_root().join("system/contracts/reference/allowlist.json")),
        "--output".to_string(),
        path_to_string(&qms_output),
    ]);
    assert!(
        qms_run.status.success(),
        "qms shell run failed: {}",
        String::from_utf8_lossy(&qms_run.stderr)
    );

    let stream_run = run_shell(&[
        "--interface".to_string(),
        path_to_string(&repo_root().join("tests/fixtures/interfaces/stream.kafka.sample.json")),
        "--contract-registry".to_string(),
        path_to_string(&repo_root().join("system/contracts/reference/allowlist.json")),
        "--output".to_string(),
        path_to_string(&stream_output),
    ]);
    assert!(
        stream_run.status.success(),
        "stream shell run failed: {}",
        String::from_utf8_lossy(&stream_run.stderr)
    );

    assert_output_ids(&mes_output, &["DEF-1001|LOT-77", "DEF-1002|LOT-78"], "mes");
    assert_output_ids(&qms_output, &["CLAIM-9001", "CLAIM-9002"], "qms");
    assert_output_ids(&stream_output, &["evt-1", "evt-2"], "mes-stream");

    let merge_run = run_fabric(&[
        "--inputs".to_string(),
        path_to_string(&mes_output),
        "--inputs".to_string(),
        path_to_string(&qms_output),
        "--output".to_string(),
        path_to_string(&merged_output),
        "--dedupe".to_string(),
    ]);
    assert!(
        merge_run.status.success(),
        "fabric merge failed: {}",
        String::from_utf8_lossy(&merge_run.stderr)
    );

    assert_output_ids(
        &merged_output,
        &[
            "DEF-1001|LOT-77",
            "DEF-1002|LOT-78",
            "CLAIM-9001",
            "CLAIM-9002",
        ],
        "",
    );
}

fn run_shell(args: &[String]) -> Output {
    if let Ok(binary_path) = std::env::var("CARGO_BIN_EXE_shell") {
        return Command::new(binary_path)
            .args(args)
            .output()
            .expect("failed to execute shell binary");
    }

    Command::new("cargo")
        .current_dir(repo_root())
        .arg("run")
        .arg("-p")
        .arg("shell")
        .arg("--")
        .args(args)
        .output()
        .expect("failed to execute shell via cargo run")
}

fn run_fabric(args: &[String]) -> Output {
    if let Ok(binary_path) = std::env::var("CARGO_BIN_EXE_fabric") {
        return Command::new(binary_path)
            .args(args)
            .output()
            .expect("failed to execute fabric binary");
    }

    Command::new("cargo")
        .current_dir(repo_root())
        .arg("run")
        .arg("-p")
        .arg("fabric")
        .arg("--")
        .args(args)
        .output()
        .expect("failed to execute fabric via cargo run")
}

fn assert_output_ids(path: &Path, expected_ids: &[&str], expected_source: &str) {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("expected output file: {}", path.display()));
    let mut ids = Vec::new();

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("output line should be valid json");
        let record_id = parsed["record_id"]
            .as_str()
            .expect("record_id should be a string");
        ids.push(record_id.to_string());

        if !expected_source.is_empty() {
            assert_eq!(
                parsed["source"].as_str(),
                Some(expected_source),
                "source should match expected fixture source"
            );
        }
    }

    let expected: Vec<String> = expected_ids.iter().map(|id| (*id).to_string()).collect();
    assert_eq!(
        ids, expected,
        "record IDs should exactly match expected values"
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to resolve repo root")
}

fn temp_test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rootsys-shell-e2e-{}-{}-{}",
        name,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("failed to create temp test dir");
    dir
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}
