use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use kernel::{
    ActionActor, ActionAuditApiService, ActionCommand, ActionRequest, AddEvidenceToLinkCommand,
    AuditQuery, BasicActionHandler, CandidateLinkState, ConfirmLinkCommand, RejectLinkCommand,
    SqliteAuditLogStore, SqliteCandidateStateStore,
};
use linkage::{LinkSeed, PrefixSimilarityProbabilisticGenerator, ProbabilisticLinkGenerator};

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

#[test]
fn whitepaper_vertical_slice_supports_confirm_reject_and_evidence_paths() {
    let temp_dir = temp_test_dir("whitepaper_vertical_slice");
    let interface_path = temp_dir.join("slice.interface.json");
    let input_path = temp_dir.join("slice.input.jsonl");
    let output_path = temp_dir.join("slice.output.jsonl");
    let product_output_dir = temp_dir.join("slice.product");

    fs::write(
        &input_path,
        [
            serde_json::json!({
                "event_id": "evt-defect-1",
                "defect_id": "D-100",
                "lot_id": "LOT-1",
                "line": "L1",
                "equipment_id": "EQ-1"
            })
            .to_string(),
            serde_json::json!({
                "event_id": "evt-cause-1",
                "defect_id": "D-100",
                "cause_id": "C-100",
                "cause": "temperature_drift",
                "confidence": 0.82,
                "lot_id": "LOT-1",
                "line": "L1",
                "equipment_id": "EQ-1"
            })
            .to_string(),
            serde_json::json!({
                "event_id": "evt-evidence-1",
                "defect_id": "D-100",
                "evidence_id": "E-100",
                "evidence_type": "log",
                "lot_id": "LOT-1",
                "line": "L1",
                "equipment_id": "EQ-1"
            })
            .to_string(),
        ]
        .join("\n")
            + "\n",
    )
    .expect("failed to write vertical-slice input fixture");

    let interface = serde_json::json!({
        "name": "mes-stream",
        "version": "v1",
        "driver": {
            "kind": "jsonl"
        },
        "payload_format": "json",
        "record_id_policy": "hash_fallback",
        "record_id_paths": ["/event_id"]
    });
    fs::write(
        &interface_path,
        serde_json::to_string_pretty(&interface).expect("slice interface json"),
    )
    .expect("failed to write vertical-slice interface");

    let run = run_shell(&[
        "--interface".to_string(),
        path_to_string(&interface_path),
        "--input".to_string(),
        path_to_string(&input_path),
        "--contract-registry".to_string(),
        path_to_string(&repo_root().join("system/contracts/reference/allowlist.json")),
        "--output".to_string(),
        path_to_string(&output_path),
        "--enable-product-flow".to_string(),
        "--product-output-dir".to_string(),
        path_to_string(&product_output_dir),
    ]);
    assert!(
        run.status.success(),
        "vertical-slice shell run failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    let deterministic_links = read_jsonl(&product_output_dir.join("links.r1.jsonl"));
    let candidate_links = read_jsonl(&product_output_dir.join("links.r2.jsonl"));
    let ontology_objects = read_jsonl(&product_output_dir.join("ontology.objects.jsonl"));
    let action_results = read_jsonl(&product_output_dir.join("actions.results.jsonl"));

    assert!(
        !deterministic_links.is_empty(),
        "deterministic links should be produced for confirm path"
    );
    assert!(
        action_results
            .iter()
            .any(|line| line["action_kind"].as_str() == Some("ConfirmLink")),
        "product flow should include confirm action result"
    );

    let deterministic_link_id = deterministic_links[0]["link_id"]
        .as_str()
        .expect("deterministic link_id should exist")
        .to_string();
    let candidate_link_id = if let Some(link_id) = candidate_links
        .first()
        .and_then(|line| line["link_id"].as_str())
        .map(ToString::to_string)
    {
        link_id
    } else {
        let left_object_id = ontology_objects
            .first()
            .and_then(|line| line["object_id"].as_str())
            .unwrap_or("defect-obj")
            .to_string();
        let right_object_id = ontology_objects
            .get(1)
            .and_then(|line| line["object_id"].as_str())
            .unwrap_or("cause-obj")
            .to_string();

        let fallback_candidates = PrefixSimilarityProbabilisticGenerator {
            relation: "candidate_of".to_string(),
            min_score: 0.2,
        }
        .generate_candidates(&[LinkSeed {
            left_object_id,
            right_object_id,
            left_record_id: "evt-defect-1".to_string(),
            right_record_id: "evt-defect-2".to_string(),
            left_source: "mes-stream".to_string(),
            right_source: "mes-stream".to_string(),
            left_strong_keys: Default::default(),
            right_strong_keys: Default::default(),
            left_attributes: Default::default(),
            right_attributes: Default::default(),
            left_event_unix_ms: Some(1_706_999_000_000),
            right_event_unix_ms: Some(1_706_999_000_001),
        }]);
        assert!(
            !fallback_candidates.is_empty(),
            "fallback probabilistic candidate should be generated"
        );
        fallback_candidates[0].link_id.clone()
    };

    let audit_store = SqliteAuditLogStore::new(product_output_dir.join("actions.audit.sqlite"))
        .expect("audit store should initialize");
    let candidate_store =
        SqliteCandidateStateStore::new(product_output_dir.join("candidate.state.sqlite"))
            .expect("candidate state store should initialize");
    let service =
        ActionAuditApiService::new(BasicActionHandler::default(), audit_store, candidate_store);

    let add_evidence = service
        .execute(
            ActionRequest {
                actor: ActionActor {
                    actor_id: "operator-1".to_string(),
                    role: "operator".to_string(),
                },
                command: ActionCommand::AddEvidenceToLink(AddEvidenceToLinkCommand {
                    link_id: candidate_link_id.clone(),
                    evidence_id: "E-200".to_string(),
                    description: "operator attached additional evidence".to_string(),
                }),
            },
            1_706_999_000_001,
        )
        .expect("add evidence path should succeed");
    assert_eq!(add_evidence.current_state, CandidateLinkState::InReview);

    let reject = service
        .execute(
            ActionRequest {
                actor: ActionActor {
                    actor_id: "reviewer-1".to_string(),
                    role: "reviewer".to_string(),
                },
                command: ActionCommand::RejectLink(RejectLinkCommand {
                    link_id: candidate_link_id.clone(),
                    reason: "insufficient causal confidence after review".to_string(),
                }),
            },
            1_706_999_000_002,
        )
        .expect("reject path should succeed");
    assert_eq!(reject.current_state, CandidateLinkState::Rejected);

    let confirm = service
        .execute(
            ActionRequest {
                actor: ActionActor {
                    actor_id: "reviewer-2".to_string(),
                    role: "reviewer".to_string(),
                },
                command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                    link_id: deterministic_link_id.clone(),
                    justification: "deterministic strong key match accepted".to_string(),
                }),
            },
            1_706_999_000_003,
        )
        .expect("confirm path should succeed");
    assert_eq!(confirm.current_state, CandidateLinkState::Confirmed);

    let candidate_history = service
        .query_candidate_history(&candidate_link_id, 10)
        .expect("candidate history query should succeed");
    assert!(candidate_history
        .iter()
        .any(|transition| transition.to_state == CandidateLinkState::InReview));
    assert!(candidate_history
        .iter()
        .any(|transition| transition.to_state == CandidateLinkState::Rejected));

    let audit_events = service
        .query_audit(AuditQuery {
            link_id: Some(candidate_link_id),
            limit: 20,
        })
        .expect("audit query should succeed");
    assert!(audit_events
        .iter()
        .any(|event| event.action_kind == kernel::ActionKind::AddEvidenceToLink));
    assert!(audit_events
        .iter()
        .any(|event| event.action_kind == kernel::ActionKind::RejectLink));
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

fn read_jsonl(path: &Path) -> Vec<serde_json::Value> {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("expected output file: {}", path.display()));
    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl line should parse"))
        .collect()
}
