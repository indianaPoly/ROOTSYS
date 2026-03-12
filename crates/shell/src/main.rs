use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use clap::ValueEnum;
use common::{DeadLetter, ExternalRecord, InterfaceRef, PayloadFormat, RecordMetadata};
use drivers::{
    ApiKeyAuthConfig, ApiKeyLocation, BinaryFileDriver,
    CircuitBreakerConfig as DriverCircuitBreakerConfig,
    CursorPaginationConfig as DriverCursorPaginationConfig, DbConfig, DbDriver, DbKind,
    DbRetryConfig as DriverDbRetryConfig, ExternalSystem, InputSource, JsonlDriver,
    KafkaStreamConfig as DriverKafkaStreamConfig, OAuth2ClientCredentialsAuthConfig,
    PagePaginationConfig as DriverPagePaginationConfig, PostgresTlsMode as DriverPostgresTlsMode,
    RestConfig, RestDriver, RestPaginationConfig as DriverRestPaginationConfig,
    RestPaginationKind as DriverRestPaginationKind, RestRetryConfig as DriverRestRetryConfig,
    StreamConfig as DriverStreamConfig, StreamDriver, StreamSourceKind as DriverStreamSourceKind,
    StreamStartOffset as DriverStreamStartOffset, TextLineDriver,
};
use kernel::{
    ActionActor, ActionCommand, ActionHandler, ActionRequest, AddEvidenceToLinkCommand, AuditEvent,
    AuditLogStore, BasicActionHandler, ConfirmLinkCommand, SqliteAuditLogStore,
};
use linkage::{
    DeterministicLinkGenerator, ExactRecordIdDeterministicGenerator, LightweightR2Config,
    LightweightR2ProbabilisticGenerator, LinkSeed, ProbabilisticLinkGenerator,
    StrongKeyDeterministicGenerator,
};
use ontology::{
    BasicOntologyMaterializer, OntologyMaterializer, OntologyObject, OntologyObjectType,
};
use runtime::{
    ApiKeyLocation as RuntimeApiKeyLocation, ContractRegistry, DbKind as RuntimeDbKind, DriverKind,
    ExternalInterface, IntegrationPipeline, PostgresTlsMode as RuntimePostgresTlsMode,
    RestAuthKind, RestPaginationKind as RuntimeRestPaginationKind,
    StreamSourceKind as RuntimeStreamSourceKind, StreamStartOffset as RuntimeStreamStartOffset,
};

#[derive(Debug, Parser)]
#[command(name = "rootsys-shell")]
#[command(about = "Data integration pipeline runner", long_about = None)]
struct Args {
    #[arg(long)]
    interface: PathBuf,
    #[arg(long)]
    input: Option<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    dlq: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = DlqSinkKind::File)]
    dlq_sink: DlqSinkKind,
    #[arg(long, default_value = "dead_letters")]
    dlq_table: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, value_enum, default_value_t = ScheduleMode::Once)]
    schedule_mode: ScheduleMode,
    #[arg(long)]
    interval_seconds: Option<u64>,
    #[arg(long)]
    max_runs: Option<u32>,
    #[arg(long)]
    replay_dlq: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = DlqSinkKind::File)]
    replay_dlq_source: DlqSinkKind,
    #[arg(long, default_value = "dead_letters")]
    replay_dlq_table: String,
    #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
    format: InputFormat,
    #[arg(long, default_value = "system/contracts/reference/allowlist.json")]
    contract_registry: PathBuf,
    #[arg(long, default_value_t = false)]
    enable_product_flow: bool,
    #[arg(long)]
    product_output_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum InputFormat {
    Auto,
    Jsonl,
    Text,
    Binary,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum DlqSinkKind {
    File,
    Sqlite,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum ScheduleMode {
    Once,
    Interval,
}

struct ResolvedSchedule {
    interval_seconds: Option<u64>,
    max_runs: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let schedule = resolve_schedule(&args)?;

    let interface = ExternalInterface::load(&args.interface)?;
    let contract_registry = ContractRegistry::load(&args.contract_registry)?;
    interface.validate_against_registry(&contract_registry)?;
    let source = args
        .source
        .clone()
        .unwrap_or_else(|| interface.name.clone());

    let driver_kind = match args.format {
        InputFormat::Auto => interface.driver.kind,
        InputFormat::Jsonl => DriverKind::Jsonl,
        InputFormat::Text => DriverKind::Text,
        InputFormat::Binary => DriverKind::Binary,
    };

    let dlq_sink = build_dlq_sink(&args);
    let mut total_records = 0usize;
    let mut total_dead_letters = 0usize;
    let mut metrics = PipelineMetrics::default();
    let mut product_metrics = ProductFlowMetrics::default();
    let product_output_paths = if args.enable_product_flow {
        Some(ProductOutputPaths::new(&args)?)
    } else {
        None
    };
    let mut next_audit_event_id = 1i64;

    for run_idx in 0..schedule.max_runs {
        let metadata = metadata_from_interface(&interface);
        let records = if let Some(replay_dlq_path) = &args.replay_dlq {
            load_replay_records(&args, replay_dlq_path)?
        } else {
            let mut driver = build_external_driver(&args, &interface, driver_kind, metadata)?;
            driver.fetch()?
        };

        let input_records = records.len();
        metrics.input_records_total += input_records;

        let pipeline = IntegrationPipeline::new(interface.clone());
        let outcome = pipeline.integrate(&source, records);

        let append = run_idx > 0;
        write_jsonl(&args.output, &outcome.records, append)?;

        if !outcome.dead_letters.is_empty() {
            dlq_sink.write(&outcome.dead_letters, append)?;
        }

        if let Some(paths) = &product_output_paths {
            let report =
                run_product_flow(&outcome.records, paths, append, &mut next_audit_event_id)?;
            product_metrics.ontology_objects_total += report.ontology_objects;
            product_metrics.ontology_relations_total += report.ontology_relations;
            product_metrics.deterministic_links_total += report.deterministic_links;
            product_metrics.candidate_links_total += report.candidate_links;
            product_metrics.actions_total += report.actions;
            product_metrics.audit_events_total += report.audit_events;

            emit_structured_log(
                "product_flow_summary",
                serde_json::json!({
                    "run_index": run_idx + 1,
                    "ontology_objects": report.ontology_objects,
                    "ontology_relations": report.ontology_relations,
                    "deterministic_links": report.deterministic_links,
                    "candidate_links": report.candidate_links,
                    "actions": report.actions,
                    "audit_events": report.audit_events
                }),
            );
        }

        total_records += outcome.records.len();
        total_dead_letters += outcome.dead_letters.len();
        metrics.integration_records_total += outcome.records.len();
        metrics.dlq_records_total += outcome.dead_letters.len();
        metrics.runs_total += 1;

        emit_structured_log(
            "run_summary",
            serde_json::json!({
                "run_index": run_idx + 1,
                "source": source,
                "input_records": input_records,
                "integration_records": outcome.records.len(),
                "dlq_records": outcome.dead_letters.len()
            }),
        );

        println!(
            "run {}: records={} | dead_letters={}",
            run_idx + 1,
            outcome.records.len(),
            outcome.dead_letters.len()
        );

        if let Some(interval_seconds) = schedule.interval_seconds {
            if run_idx + 1 < schedule.max_runs {
                thread::sleep(std::time::Duration::from_secs(interval_seconds));
            }
        }
    }

    println!(
        "total runs: {} | total records: {} | total dead_letters: {}",
        schedule.max_runs, total_records, total_dead_letters
    );

    emit_structured_log(
        "pipeline_metrics",
        serde_json::json!({
            "runs_total": metrics.runs_total,
            "input_records_total": metrics.input_records_total,
            "integration_records_total": metrics.integration_records_total,
            "dlq_records_total": metrics.dlq_records_total
        }),
    );

    if args.enable_product_flow {
        emit_structured_log(
            "product_flow_metrics",
            serde_json::json!({
                "ontology_objects_total": product_metrics.ontology_objects_total,
                "ontology_relations_total": product_metrics.ontology_relations_total,
                "deterministic_links_total": product_metrics.deterministic_links_total,
                "candidate_links_total": product_metrics.candidate_links_total,
                "actions_total": product_metrics.actions_total,
                "audit_events_total": product_metrics.audit_events_total
            }),
        );
    }

    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
struct PipelineMetrics {
    runs_total: usize,
    input_records_total: usize,
    integration_records_total: usize,
    dlq_records_total: usize,
}

#[derive(Debug, Default, Clone, Copy)]
struct ProductFlowMetrics {
    ontology_objects_total: usize,
    ontology_relations_total: usize,
    deterministic_links_total: usize,
    candidate_links_total: usize,
    actions_total: usize,
    audit_events_total: usize,
}

fn emit_structured_log(event: &str, payload: serde_json::Value) {
    let line = serde_json::json!({
        "event": event,
        "ts_unix_ms": now_unix_ms(),
        "payload": payload
    });

    println!("{}", line);
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Debug, Clone)]
struct ProductOutputPaths {
    ontology_output: PathBuf,
    ontology_relations_output: PathBuf,
    deterministic_links_output: PathBuf,
    candidate_links_output: PathBuf,
    actions_output: PathBuf,
    audit_db_path: PathBuf,
}

impl ProductOutputPaths {
    fn new(args: &Args) -> Result<Self, Box<dyn Error>> {
        let output_dir = if let Some(path) = &args.product_output_dir {
            path.clone()
        } else {
            default_product_output_dir(&args.output)
        };

        std::fs::create_dir_all(&output_dir)?;

        Ok(Self {
            ontology_output: output_dir.join("ontology.objects.jsonl"),
            ontology_relations_output: output_dir.join("ontology.relations.jsonl"),
            deterministic_links_output: output_dir.join("links.r1.jsonl"),
            candidate_links_output: output_dir.join("links.r2.jsonl"),
            actions_output: output_dir.join("actions.results.jsonl"),
            audit_db_path: output_dir.join("actions.audit.sqlite"),
        })
    }
}

fn default_product_output_dir(output_path: &PathBuf) -> PathBuf {
    let parent = output_path
        .parent()
        .map(|path| path.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = output_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("output");

    parent.join(format!("{}.product", stem))
}

#[derive(Debug, Clone, Copy, Default)]
struct ProductFlowReport {
    ontology_objects: usize,
    ontology_relations: usize,
    deterministic_links: usize,
    candidate_links: usize,
    actions: usize,
    audit_events: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ProductActionLog {
    event_id: i64,
    actor_id: String,
    actor_role: String,
    action_kind: String,
    link_id: String,
    summary: String,
    created_at_unix_ms: i64,
}

fn run_product_flow(
    records: &[common::IntegrationRecord],
    output_paths: &ProductOutputPaths,
    append: bool,
    next_audit_event_id: &mut i64,
) -> Result<ProductFlowReport, Box<dyn Error>> {
    let materializer = BasicOntologyMaterializer;
    let mut ontology_objects = Vec::new();
    let mut ontology_relations = Vec::new();
    for record in records {
        ontology_objects.extend(materializer.materialize(record));
        ontology_relations.extend(materializer.materialize_relations(record));
    }

    write_jsonl(&output_paths.ontology_output, &ontology_objects, append)?;
    write_jsonl(
        &output_paths.ontology_relations_output,
        &ontology_relations,
        append,
    )?;

    let seeds = build_link_seeds(&ontology_objects);
    let strong_key_generator = StrongKeyDeterministicGenerator::new("related_to");
    let exact_record_generator =
        ExactRecordIdDeterministicGenerator::new("related_to", "r1_exact_record_id");

    let mut deterministic_links = Vec::new();
    for seed in &seeds {
        if let Some(link) = exact_record_generator.generate(seed) {
            deterministic_links.push(link);
            continue;
        }
        if let Some(link) = strong_key_generator.generate(seed) {
            deterministic_links.push(link);
        }
    }

    write_jsonl(
        &output_paths.deterministic_links_output,
        &deterministic_links,
        append,
    )?;

    let candidate_generator =
        LightweightR2ProbabilisticGenerator::new("candidate_of", LightweightR2Config::default());
    let mut candidate_links = candidate_generator.generate_candidates(&seeds);
    candidate_links.retain(|candidate| candidate.validate_schema().is_ok());

    write_jsonl(
        &output_paths.candidate_links_output,
        &candidate_links,
        append,
    )?;

    let action_handler = BasicActionHandler::default();
    let audit_store = SqliteAuditLogStore::new(&output_paths.audit_db_path)?;
    let mut action_logs = Vec::new();

    for link in &deterministic_links {
        let request = ActionRequest {
            actor: ActionActor {
                actor_id: "system-admin".to_string(),
                role: "admin".to_string(),
            },
            command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                link_id: link.link_id.clone(),
                justification: format!("auto-confirmed by {}", link.rule),
            }),
        };
        let result = action_handler
            .handle(request)
            .map_err(|error| format!("action handling failed: {error:?}"))?;
        append_audit_event(
            &audit_store,
            &mut action_logs,
            next_audit_event_id,
            "system-admin",
            "admin",
            &result,
        )?;
    }

    for (idx, candidate) in candidate_links.iter().enumerate().take(5) {
        let request = ActionRequest {
            actor: ActionActor {
                actor_id: "system-operator".to_string(),
                role: "operator".to_string(),
            },
            command: ActionCommand::AddEvidenceToLink(AddEvidenceToLinkCommand {
                link_id: candidate.link_id.clone(),
                evidence_id: format!("candidate-evidence-{}", idx + 1),
                description: "auto-attached during product flow execution".to_string(),
            }),
        };
        let result = action_handler
            .handle(request)
            .map_err(|error| format!("action handling failed: {error:?}"))?;
        append_audit_event(
            &audit_store,
            &mut action_logs,
            next_audit_event_id,
            "system-operator",
            "operator",
            &result,
        )?;
    }

    write_jsonl(&output_paths.actions_output, &action_logs, append)?;

    Ok(ProductFlowReport {
        ontology_objects: ontology_objects.len(),
        ontology_relations: ontology_relations.len(),
        deterministic_links: deterministic_links.len(),
        candidate_links: candidate_links.len(),
        actions: action_logs.len(),
        audit_events: action_logs.len(),
    })
}

fn append_audit_event(
    audit_store: &impl AuditLogStore,
    action_logs: &mut Vec<ProductActionLog>,
    next_audit_event_id: &mut i64,
    actor_id: &str,
    actor_role: &str,
    action_result: &kernel::ActionResult,
) -> Result<(), Box<dyn Error>> {
    let event = AuditEvent {
        event_id: *next_audit_event_id,
        actor_id: actor_id.to_string(),
        actor_role: actor_role.to_string(),
        action_kind: action_result.action_kind,
        link_id: action_result.link_id.clone(),
        summary: action_result.summary.clone(),
        created_at_unix_ms: now_unix_ms(),
    };

    audit_store.append(&event)?;

    action_logs.push(ProductActionLog {
        event_id: event.event_id,
        actor_id: event.actor_id,
        actor_role: event.actor_role,
        action_kind: format!("{:?}", event.action_kind),
        link_id: event.link_id,
        summary: event.summary,
        created_at_unix_ms: event.created_at_unix_ms,
    });

    *next_audit_event_id += 1;
    Ok(())
}

fn build_link_seeds(objects: &[OntologyObject]) -> Vec<LinkSeed> {
    let defect_objects = objects
        .iter()
        .filter(|object| object.object_type == OntologyObjectType::Defect)
        .collect::<Vec<_>>();
    let non_defect_objects = objects
        .iter()
        .filter(|object| object.object_type != OntologyObjectType::Defect)
        .collect::<Vec<_>>();

    let mut seeds = Vec::new();
    for defect in &defect_objects {
        for target in &non_defect_objects {
            let left_attributes = attributes_to_string_map(&defect.attributes);
            let right_attributes = attributes_to_string_map(&target.attributes);

            seeds.push(LinkSeed {
                left_object_id: defect.object_id.clone(),
                right_object_id: target.object_id.clone(),
                left_record_id: defect.lineage.record_id.clone(),
                right_record_id: target.lineage.record_id.clone(),
                left_source: defect.lineage.source.clone(),
                right_source: target.lineage.source.clone(),
                left_strong_keys: select_strong_keys(&left_attributes),
                right_strong_keys: select_strong_keys(&right_attributes),
                left_attributes,
                right_attributes,
                left_event_unix_ms: Some(defect.lineage.ingested_at_unix_ms),
                right_event_unix_ms: Some(target.lineage.ingested_at_unix_ms),
            });
        }
    }

    seeds
}

fn attributes_to_string_map(
    value: &serde_json::Value,
) -> std::collections::BTreeMap<String, String> {
    let mut map = std::collections::BTreeMap::new();
    let Some(object) = value.as_object() else {
        return map;
    };

    for (key, raw_value) in object {
        let value = raw_value
            .as_str()
            .map(ToString::to_string)
            .unwrap_or_else(|| raw_value.to_string());
        map.insert(key.clone(), value);
    }

    map
}

fn select_strong_keys(
    attributes: &std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    let keys = ["defect_id", "lot_id", "equipment_id", "cause_id"];
    let mut selected = std::collections::BTreeMap::new();

    for key in keys {
        if let Some(value) = attributes.get(key) {
            if !value.trim().is_empty() {
                selected.insert(key.to_string(), value.clone());
            }
        }
    }

    selected
}

fn resolve_schedule(args: &Args) -> Result<ResolvedSchedule, Box<dyn Error>> {
    match args.schedule_mode {
        ScheduleMode::Once => {
            if args.interval_seconds.is_some() || args.max_runs.is_some() {
                return Err(
                    "--interval-seconds/--max-runs are only valid with --schedule-mode interval"
                        .into(),
                );
            }

            Ok(ResolvedSchedule {
                interval_seconds: None,
                max_runs: 1,
            })
        }
        ScheduleMode::Interval => {
            let interval_seconds = args
                .interval_seconds
                .filter(|value| *value > 0)
                .ok_or("--interval-seconds must be > 0 when --schedule-mode interval")?;
            let max_runs = args
                .max_runs
                .filter(|value| *value > 0)
                .ok_or("--max-runs must be > 0 when --schedule-mode interval")?;

            Ok(ResolvedSchedule {
                interval_seconds: Some(interval_seconds),
                max_runs,
            })
        }
    }
}

fn build_external_driver(
    args: &Args,
    interface: &ExternalInterface,
    driver_kind: DriverKind,
    metadata: RecordMetadata,
) -> Result<Box<dyn ExternalSystem>, Box<dyn Error>> {
    let driver: Box<dyn ExternalSystem> = match driver_kind {
        DriverKind::Jsonl => {
            let input_source = resolve_input(&args.input, interface)?;
            Box::new(JsonlDriver::new(input_source, metadata))
        }
        DriverKind::Text => {
            let input_source = resolve_input(&args.input, interface)?;
            Box::new(TextLineDriver::new(input_source, metadata))
        }
        DriverKind::Binary => {
            let input_source = resolve_input(&args.input, interface)?;
            Box::new(BinaryFileDriver::new(input_source, metadata))
        }
        DriverKind::Rest => {
            let config = rest_config_from_interface(interface)?;
            Box::new(RestDriver::new(config, metadata))
        }
        DriverKind::Db => {
            let config = db_config_from_interface(interface)?;
            Box::new(DbDriver::new(config, metadata))
        }
        DriverKind::Stream => {
            let config = stream_config_from_interface(interface)?;
            Box::new(StreamDriver::new(config, metadata))
        }
    };
    Ok(driver)
}

trait DlqSink {
    fn write(&self, rows: &[common::DeadLetter], append: bool) -> Result<(), Box<dyn Error>>;
}

struct FileDlqSink {
    path: PathBuf,
}

impl FileDlqSink {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl DlqSink for FileDlqSink {
    fn write(&self, rows: &[common::DeadLetter], append: bool) -> Result<(), Box<dyn Error>> {
        write_jsonl(&self.path, rows, append)?;
        Ok(())
    }
}

struct SqliteDlqSink {
    path: PathBuf,
    table: String,
}

impl SqliteDlqSink {
    fn new(path: PathBuf, table: String) -> Self {
        Self { path, table }
    }

    fn validated_table_name(&self) -> Result<&str, Box<dyn Error>> {
        if is_valid_sqlite_identifier(&self.table) {
            Ok(&self.table)
        } else {
            Err(format!(
                "invalid --dlq-table value '{}': use [A-Za-z_][A-Za-z0-9_]*",
                self.table
            )
            .into())
        }
    }

    fn ensure_table(&self, connection: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
        let table = self.table.as_str();
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_unix_ms INTEGER NOT NULL,
                source TEXT NOT NULL,
                interface_name TEXT NOT NULL,
                interface_version TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                reason_codes_json TEXT NOT NULL,
                lineage_json TEXT,
                errors_json TEXT NOT NULL
            )",
            table
        );
        connection.execute(&sql, [])?;
        Ok(())
    }
}

impl DlqSink for SqliteDlqSink {
    fn write(&self, rows: &[common::DeadLetter], _append: bool) -> Result<(), Box<dyn Error>> {
        let table = self.validated_table_name()?.to_string();
        let mut connection = rusqlite::Connection::open(&self.path)?;
        self.ensure_table(&connection)?;

        let insert_sql = format!(
            "INSERT INTO {} (
                created_at_unix_ms,
                source,
                interface_name,
                interface_version,
                payload_json,
                metadata_json,
                reason_codes_json,
                lineage_json,
                errors_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            table
        );

        let tx = connection.transaction()?;
        {
            let mut statement = tx.prepare(&insert_sql)?;
            for row in rows {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                let payload_json = serde_json::to_string(&row.payload)?;
                let metadata_json = serde_json::to_string(&row.metadata)?;
                let reason_codes_json = serde_json::to_string(&row.reason_codes)?;
                let lineage_json = serde_json::to_string(&row.lineage)?;
                let errors_json = serde_json::to_string(&row.errors)?;

                statement.execute(rusqlite::params![
                    now,
                    &row.source,
                    &row.interface.name,
                    &row.interface.version,
                    payload_json,
                    metadata_json,
                    reason_codes_json,
                    lineage_json,
                    errors_json,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

fn build_dlq_sink(args: &Args) -> Box<dyn DlqSink> {
    match args.dlq_sink {
        DlqSinkKind::File => {
            let path = args
                .dlq
                .clone()
                .unwrap_or_else(|| with_suffix(&args.output, "dlq"));
            Box::new(FileDlqSink::new(path))
        }
        DlqSinkKind::Sqlite => {
            let path = args
                .dlq
                .clone()
                .unwrap_or_else(|| with_suffix(&args.output, "dlq.db"));
            Box::new(SqliteDlqSink::new(path, args.dlq_table.clone()))
        }
    }
}

fn load_replay_records(args: &Args, path: &PathBuf) -> Result<Vec<ExternalRecord>, Box<dyn Error>> {
    let dead_letters = match args.replay_dlq_source {
        DlqSinkKind::File => load_dead_letters_from_file(path)?,
        DlqSinkKind::Sqlite => load_dead_letters_from_sqlite(path, &args.replay_dlq_table)?,
    };

    Ok(dead_letters_to_external_records(dead_letters))
}

fn load_dead_letters_from_file(path: &PathBuf) -> Result<Vec<DeadLetter>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut dead_letters = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let dead_letter: DeadLetter = serde_json::from_str(&line)?;
        dead_letters.push(dead_letter);
    }

    Ok(dead_letters)
}

fn load_dead_letters_from_sqlite(
    path: &PathBuf,
    table: &str,
) -> Result<Vec<DeadLetter>, Box<dyn Error>> {
    if !is_valid_sqlite_identifier(table) {
        return Err(format!(
            "invalid --replay-dlq-table value '{}': use [A-Za-z_][A-Za-z0-9_]*",
            table
        )
        .into());
    }

    let connection = rusqlite::Connection::open(path)?;

    let sql_with_lineage = format!(
        "SELECT source, interface_name, interface_version, payload_json, metadata_json, reason_codes_json, lineage_json, errors_json
         FROM {} ORDER BY id ASC",
        table
    );

    if let Ok(mut statement) = connection.prepare(&sql_with_lineage) {
        let rows = statement.query_map([], |row| {
            let source: String = row.get(0)?;
            let interface_name: String = row.get(1)?;
            let interface_version: String = row.get(2)?;
            let payload_json: String = row.get(3)?;
            let metadata_json: String = row.get(4)?;
            let reason_codes_json: String = row.get(5)?;
            let lineage_json: Option<String> = row.get(6)?;
            let errors_json: String = row.get(7)?;

            let payload = serde_json::from_str(&payload_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    payload_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            let metadata = serde_json::from_str(&metadata_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    metadata_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            let reason_codes = serde_json::from_str(&reason_codes_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    reason_codes_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            let lineage = if let Some(lineage_json) = lineage_json {
                Some(serde_json::from_str(&lineage_json).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        lineage_json.len(),
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?)
            } else {
                None
            };
            let errors: Vec<common::ValidationMessage> = serde_json::from_str(&errors_json)
                .map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        errors_json.len(),
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;

            Ok(DeadLetter {
                source,
                interface: InterfaceRef {
                    name: interface_name,
                    version: interface_version,
                },
                payload,
                metadata,
                reason_codes,
                lineage,
                errors,
            })
        })?;

        let mut dead_letters = Vec::new();
        for row in rows {
            dead_letters.push(row?);
        }
        return Ok(dead_letters);
    }

    let sql_legacy = format!(
        "SELECT source, interface_name, interface_version, payload_json, metadata_json, errors_json
         FROM {} ORDER BY id ASC",
        table
    );
    let mut statement = connection.prepare(&sql_legacy)?;

    let rows = statement.query_map([], |row| {
        let source: String = row.get(0)?;
        let interface_name: String = row.get(1)?;
        let interface_version: String = row.get(2)?;
        let payload_json: String = row.get(3)?;
        let metadata_json: String = row.get(4)?;
        let errors_json: String = row.get(5)?;

        let payload = serde_json::from_str(&payload_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                payload_json.len(),
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?;
        let metadata = serde_json::from_str(&metadata_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                metadata_json.len(),
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?;
        let errors: Vec<common::ValidationMessage> =
            serde_json::from_str(&errors_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    errors_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;

        Ok(DeadLetter {
            source,
            interface: InterfaceRef {
                name: interface_name,
                version: interface_version,
            },
            payload,
            metadata,
            reason_codes: dedupe_reason_codes_from_errors(&errors),
            lineage: None,
            errors,
        })
    })?;

    let mut dead_letters = Vec::new();
    for row in rows {
        dead_letters.push(row?);
    }

    Ok(dead_letters)
}

fn dead_letters_to_external_records(dead_letters: Vec<DeadLetter>) -> Vec<ExternalRecord> {
    dead_letters
        .into_iter()
        .map(|dead_letter| ExternalRecord {
            payload: dead_letter.payload,
            metadata: dead_letter.metadata,
        })
        .collect()
}

fn dedupe_reason_codes_from_errors(errors: &[common::ValidationMessage]) -> Vec<String> {
    let mut codes = Vec::new();
    for error in errors {
        if !codes.iter().any(|code| code == &error.code) {
            codes.push(error.code.clone());
        }
    }
    codes
}

fn is_valid_sqlite_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }

    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

/// Write JSONL output to disk.
fn write_jsonl<T: serde::Serialize>(
    path: &PathBuf,
    rows: &[T],
    append: bool,
) -> Result<(), std::io::Error> {
    let file = if append {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?
    } else {
        File::create(path)?
    };
    let mut writer = BufWriter::new(file);

    for row in rows {
        let line = serde_json::to_string(row).unwrap_or_else(|_| "{}".to_string());
        writeln!(writer, "{}", line)?;
    }

    Ok(())
}

/// Append a suffix before the extension (used for DLQ files).
fn with_suffix(path: &PathBuf, suffix: &str) -> PathBuf {
    let mut new_path = path.clone();
    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        let filename = if ext.is_empty() {
            format!("{}.{}", stem, suffix)
        } else {
            format!("{}.{}.{}", stem, suffix, ext)
        };
        new_path.set_file_name(filename);
    }
    new_path
}

/// Resolve input for file-based drivers (supports "-" for stdin).
fn resolve_input(
    input: &Option<PathBuf>,
    interface: &ExternalInterface,
) -> Result<InputSource, Box<dyn std::error::Error>> {
    if let Some(path) = input {
        return Ok(InputSource::from_path(path.clone()));
    }

    if let Some(path) = &interface.driver.input {
        return Ok(InputSource::from_str(path));
    }

    Err("input path is required (use --input or interface.driver.input)".into())
}

/// Build default metadata from the interface driver configuration.
fn metadata_from_interface(interface: &ExternalInterface) -> common::RecordMetadata {
    common::RecordMetadata {
        content_type: interface.driver.content_type.clone(),
        filename: interface.driver.filename.clone(),
        source_details: None,
    }
}

/// Build REST driver config from the interface definition.
fn rest_config_from_interface(
    interface: &ExternalInterface,
) -> Result<RestConfig, Box<dyn std::error::Error>> {
    let rest = interface
        .driver
        .rest
        .as_ref()
        .ok_or("rest driver config is required")?;

    Ok(RestConfig {
        url: rest.url.clone(),
        method: rest.method.clone(),
        headers: rest.headers.clone(),
        body: rest.body.clone(),
        timeout_ms: rest.timeout_ms,
        response_format: rest.response_format.unwrap_or(PayloadFormat::Unknown),
        items_pointer: rest.items_pointer.clone(),
        api_key_auth: rest.auth.as_ref().and_then(|auth| {
            if auth.kind == RestAuthKind::ApiKey {
                auth.api_key.as_ref().map(|api_key| ApiKeyAuthConfig {
                    location: match api_key.location {
                        RuntimeApiKeyLocation::Header => ApiKeyLocation::Header,
                        RuntimeApiKeyLocation::Query => ApiKeyLocation::Query,
                    },
                    name: api_key.name.clone(),
                    value: api_key.value.clone(),
                })
            } else {
                None
            }
        }),
        oauth2_auth: rest.auth.as_ref().and_then(|auth| {
            if auth.kind == RestAuthKind::OAuth2ClientCredentials {
                auth.oauth2_client_credentials.as_ref().map(|oauth2| {
                    OAuth2ClientCredentialsAuthConfig {
                        token_url: oauth2.token_url.clone(),
                        client_id: oauth2.client_id.clone(),
                        client_secret: oauth2.client_secret.clone(),
                        scope: oauth2.scope.clone(),
                    }
                })
            } else {
                None
            }
        }),
        pagination: rest
            .pagination
            .as_ref()
            .and_then(|pagination| match pagination.kind {
                RuntimeRestPaginationKind::Cursor => {
                    pagination
                        .cursor
                        .as_ref()
                        .map(|cursor| DriverRestPaginationConfig {
                            kind: DriverRestPaginationKind::Cursor,
                            cursor: Some(DriverCursorPaginationConfig {
                                cursor_param: cursor.cursor_param.clone(),
                                cursor_path: cursor.cursor_path.clone(),
                                initial_cursor: cursor.initial_cursor.clone(),
                                max_pages: cursor.max_pages,
                            }),
                            page: None,
                        })
                }
                RuntimeRestPaginationKind::Page => {
                    pagination
                        .page
                        .as_ref()
                        .map(|page| DriverRestPaginationConfig {
                            kind: DriverRestPaginationKind::Page,
                            cursor: None,
                            page: Some(DriverPagePaginationConfig {
                                page_param: page.page_param.clone(),
                                page_size_param: page.page_size_param.clone(),
                                page_size: page.page_size,
                                initial_page: page.initial_page,
                                max_pages: page.max_pages,
                            }),
                        })
                }
            }),
        retry: rest.retry.as_ref().map(|retry| DriverRestRetryConfig {
            max_attempts: retry.max_attempts,
            base_delay_ms: retry.base_delay_ms,
            max_delay_ms: retry.max_delay_ms,
            jitter_percent: retry.jitter_percent,
        }),
        circuit_breaker: rest.circuit_breaker.as_ref().map(|circuit_breaker| {
            DriverCircuitBreakerConfig {
                failure_threshold: circuit_breaker.failure_threshold,
                open_timeout_ms: circuit_breaker.open_timeout_ms,
            }
        }),
    })
}

/// Build DB driver config from the interface definition.
fn db_config_from_interface(
    interface: &ExternalInterface,
) -> Result<DbConfig, Box<dyn std::error::Error>> {
    let db = interface
        .driver
        .db
        .as_ref()
        .ok_or("db driver config is required")?;

    let kind = match db.kind {
        RuntimeDbKind::Sqlite => DbKind::Sqlite,
        RuntimeDbKind::Postgres => DbKind::Postgres,
        RuntimeDbKind::Mysql => DbKind::Mysql,
    };

    Ok(DbConfig {
        kind,
        connection: db.connection.clone(),
        query: db.query.clone(),
        postgres_tls_mode: db.postgres_tls_mode.map(|mode| match mode {
            RuntimePostgresTlsMode::Disable => DriverPostgresTlsMode::Disable,
            RuntimePostgresTlsMode::Require => DriverPostgresTlsMode::Require,
        }),
        pool_min_connections: db.pool.as_ref().and_then(|pool| pool.min_connections),
        pool_max_connections: db.pool.as_ref().and_then(|pool| pool.max_connections),
        retry: db.retry.as_ref().map(|retry| DriverDbRetryConfig {
            max_attempts: retry.max_attempts,
            base_delay_ms: retry.base_delay_ms,
            max_delay_ms: retry.max_delay_ms,
            jitter_percent: retry.jitter_percent,
        }),
        circuit_breaker: db.circuit_breaker.as_ref().map(|circuit_breaker| {
            DriverCircuitBreakerConfig {
                failure_threshold: circuit_breaker.failure_threshold,
                open_timeout_ms: circuit_breaker.open_timeout_ms,
            }
        }),
    })
}

fn stream_config_from_interface(
    interface: &ExternalInterface,
) -> Result<DriverStreamConfig, Box<dyn std::error::Error>> {
    let stream = interface
        .driver
        .stream
        .as_ref()
        .ok_or("stream driver config is required")?;

    let source = match stream.source {
        RuntimeStreamSourceKind::Kafka => DriverStreamSourceKind::Kafka,
    };

    let kafka = stream.kafka.as_ref().map(|kafka| DriverKafkaStreamConfig {
        brokers: kafka.brokers.clone(),
        topic: kafka.topic.clone(),
        group_id: kafka.group_id.clone(),
        format: kafka.format,
        max_batch_records: kafka.max_batch_records,
        poll_timeout_ms: kafka.poll_timeout_ms,
        start_offset: kafka.start_offset.map(|offset| match offset {
            RuntimeStreamStartOffset::Earliest => DriverStreamStartOffset::Earliest,
            RuntimeStreamStartOffset::Latest => DriverStreamStartOffset::Latest,
        }),
        mvp_input: InputSource::from_str(&kafka.mvp_input),
    });

    Ok(DriverStreamConfig { source, kafka })
}

#[cfg(test)]
mod tests {
    use super::{
        build_dlq_sink, dead_letters_to_external_records, is_valid_sqlite_identifier,
        resolve_schedule, run_product_flow, with_suffix, Args, DlqSinkKind, InputFormat,
        ProductOutputPaths, ScheduleMode,
    };
    use common::{
        DeadLetter, DlqLineage, IntegrationRecord, InterfaceRef, Payload, RecordMetadata,
        ValidationMessage,
    };
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn with_suffix_appends_before_extension() {
        let path = PathBuf::from("/tmp/output.jsonl");
        assert_eq!(
            with_suffix(&path, "dlq"),
            PathBuf::from("/tmp/output.dlq.jsonl")
        );
    }

    #[test]
    fn with_suffix_handles_extensionless_path() {
        let path = PathBuf::from("/tmp/output");
        assert_eq!(with_suffix(&path, "dlq"), PathBuf::from("/tmp/output.dlq"));
    }

    #[test]
    fn default_sqlite_dlq_path_uses_db_suffix() {
        let args = Args {
            interface: PathBuf::from("/tmp/interface.json"),
            input: None,
            output: PathBuf::from("/tmp/output.jsonl"),
            dlq: None,
            dlq_sink: DlqSinkKind::Sqlite,
            dlq_table: "dead_letters".to_string(),
            source: None,
            schedule_mode: ScheduleMode::Once,
            interval_seconds: None,
            max_runs: None,
            replay_dlq: None,
            replay_dlq_source: DlqSinkKind::File,
            replay_dlq_table: "dead_letters".to_string(),
            format: InputFormat::Auto,
            contract_registry: PathBuf::from("/tmp/allowlist.json"),
            enable_product_flow: false,
            product_output_dir: None,
        };

        let _ = build_dlq_sink(&args);
        assert_eq!(
            with_suffix(&args.output, "dlq.db"),
            PathBuf::from("/tmp/output.dlq.db.jsonl")
        );
    }

    #[test]
    fn sqlite_identifier_validation_accepts_safe_name() {
        assert!(is_valid_sqlite_identifier("dead_letters"));
        assert!(is_valid_sqlite_identifier("dlq2"));
    }

    #[test]
    fn sqlite_identifier_validation_rejects_unsafe_name() {
        assert!(!is_valid_sqlite_identifier(""));
        assert!(!is_valid_sqlite_identifier("2dead_letters"));
        assert!(!is_valid_sqlite_identifier("dead-letters"));
        assert!(!is_valid_sqlite_identifier("dead letters"));
    }

    #[test]
    fn dead_letter_conversion_preserves_payload_and_metadata() {
        let dead_letters = vec![DeadLetter {
            source: "mes".to_string(),
            interface: InterfaceRef {
                name: "mes".to_string(),
                version: "v1".to_string(),
            },
            payload: Payload::from_text("hello".to_string()),
            metadata: RecordMetadata {
                content_type: Some("text/plain".to_string()),
                filename: Some("in.txt".to_string()),
                source_details: None,
            },
            reason_codes: vec!["TEST_ERROR".to_string()],
            lineage: Some(DlqLineage {
                rejected_at_unix_ms: 1,
                pipeline_stage: "integration".to_string(),
                driver_kind: "jsonl".to_string(),
                record_id_policy: "hash_fallback".to_string(),
                source_type: Some("file".to_string()),
                source_locator: Some("/tmp/input.jsonl".to_string()),
            }),
            errors: vec![ValidationMessage::new(
                "TEST_ERROR",
                Some("/x".to_string()),
                "bad".to_string(),
            )],
        }];

        let records = dead_letters_to_external_records(dead_letters);
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0].payload, Payload::Text(text) if text == "hello"));
        assert_eq!(records[0].metadata.filename.as_deref(), Some("in.txt"));
    }

    #[test]
    fn dedupe_reason_codes_from_errors_returns_unique_codes() {
        let errors = vec![
            ValidationMessage::new("A", Some("/a".to_string()), "a".to_string()),
            ValidationMessage::new("B", Some("/b".to_string()), "b".to_string()),
            ValidationMessage::new("A", Some("/c".to_string()), "c".to_string()),
        ];

        let codes = super::dedupe_reason_codes_from_errors(&errors);
        assert_eq!(codes, vec!["A".to_string(), "B".to_string()]);
    }

    #[test]
    fn resolve_schedule_once_defaults_to_single_run() {
        let args = Args {
            interface: PathBuf::from("/tmp/interface.json"),
            input: None,
            output: PathBuf::from("/tmp/output.jsonl"),
            dlq: None,
            dlq_sink: DlqSinkKind::File,
            dlq_table: "dead_letters".to_string(),
            source: None,
            schedule_mode: ScheduleMode::Once,
            interval_seconds: None,
            max_runs: None,
            replay_dlq: None,
            replay_dlq_source: DlqSinkKind::File,
            replay_dlq_table: "dead_letters".to_string(),
            format: InputFormat::Auto,
            contract_registry: PathBuf::from("/tmp/allowlist.json"),
            enable_product_flow: false,
            product_output_dir: None,
        };

        let schedule = resolve_schedule(&args).unwrap();
        assert_eq!(schedule.max_runs, 1);
        assert_eq!(schedule.interval_seconds, None);
    }

    #[test]
    fn resolve_schedule_interval_requires_interval_and_max_runs() {
        let args = Args {
            interface: PathBuf::from("/tmp/interface.json"),
            input: None,
            output: PathBuf::from("/tmp/output.jsonl"),
            dlq: None,
            dlq_sink: DlqSinkKind::File,
            dlq_table: "dead_letters".to_string(),
            source: None,
            schedule_mode: ScheduleMode::Interval,
            interval_seconds: Some(1),
            max_runs: Some(3),
            replay_dlq: None,
            replay_dlq_source: DlqSinkKind::File,
            replay_dlq_table: "dead_letters".to_string(),
            format: InputFormat::Auto,
            contract_registry: PathBuf::from("/tmp/allowlist.json"),
            enable_product_flow: false,
            product_output_dir: None,
        };

        let schedule = resolve_schedule(&args).unwrap();
        assert_eq!(schedule.max_runs, 3);
        assert_eq!(schedule.interval_seconds, Some(1));
    }

    #[test]
    fn product_flow_executes_ontology_linkage_kernel_chain() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rootsys-shell-product-flow-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be valid")
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).expect("temp dir should be created");

        let output_paths = ProductOutputPaths {
            ontology_output: temp_dir.join("ontology.objects.jsonl"),
            ontology_relations_output: temp_dir.join("ontology.relations.jsonl"),
            deterministic_links_output: temp_dir.join("links.r1.jsonl"),
            candidate_links_output: temp_dir.join("links.r2.jsonl"),
            actions_output: temp_dir.join("actions.results.jsonl"),
            audit_db_path: temp_dir.join("actions.audit.sqlite"),
        };

        let records = vec![
            IntegrationRecord {
                source: "mes".to_string(),
                interface: InterfaceRef {
                    name: "mes".to_string(),
                    version: "v1".to_string(),
                },
                record_id: "rec-defect-1".to_string(),
                ingested_at_unix_ms: 1_706_000_000_001,
                payload: Payload::from_json(serde_json::json!({
                    "defect_id": "D-1",
                    "lot_id": "LOT-10",
                    "line": "L1"
                })),
                metadata: RecordMetadata::default(),
                warnings: Vec::new(),
            },
            IntegrationRecord {
                source: "qms".to_string(),
                interface: InterfaceRef {
                    name: "qms".to_string(),
                    version: "v1".to_string(),
                },
                record_id: "rec-cause-1".to_string(),
                ingested_at_unix_ms: 1_706_000_000_101,
                payload: Payload::from_json(serde_json::json!({
                    "defect_id": "D-1",
                    "cause_id": "C-7",
                    "cause": "temperature_drift"
                })),
                metadata: RecordMetadata::default(),
                warnings: Vec::new(),
            },
        ];

        let mut next_audit_event_id = 1i64;
        let report = run_product_flow(&records, &output_paths, false, &mut next_audit_event_id)
            .expect("product flow should run successfully");

        assert!(report.ontology_objects >= 3);
        assert!(report.ontology_relations >= 1);
        assert!(report.deterministic_links >= 1);
        assert!(report.actions >= 1);
        assert!(output_paths.ontology_output.exists());
        assert!(output_paths.ontology_relations_output.exists());
        assert!(output_paths.deterministic_links_output.exists());
        assert!(output_paths.actions_output.exists());
        assert!(output_paths.audit_db_path.exists());
    }
}
