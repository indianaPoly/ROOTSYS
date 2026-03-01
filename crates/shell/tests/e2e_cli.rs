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
