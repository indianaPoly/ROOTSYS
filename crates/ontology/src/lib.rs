use common::{IntegrationRecord, Payload};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OntologyObjectType {
    Defect,
    Cause,
    CompositeCause,
    Evidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OntologyRelationType {
    HasCause,
    SupportedBy,
    CombinesTo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OntologyLineage {
    pub source: String,
    pub interface_name: String,
    pub interface_version: String,
    pub record_id: String,
    pub ingested_at_unix_ms: i64,
    pub payload_kind: String,
    pub payload_sha256: String,
    pub warning_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OntologyIdentityStrategy {
    DeterministicV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OntologyObject {
    pub object_id: String,
    pub identity_strategy: OntologyIdentityStrategy,
    pub object_type: OntologyObjectType,
    pub lineage: OntologyLineage,
    pub attributes: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OntologyRelation {
    pub relation_id: String,
    pub identity_strategy: OntologyIdentityStrategy,
    pub relation_type: OntologyRelationType,
    pub left_object_id: String,
    pub right_object_id: String,
    pub lineage: OntologyLineage,
    pub attributes: Value,
}

pub trait OntologyMaterializer {
    fn materialize(&self, record: &IntegrationRecord) -> Vec<OntologyObject>;
    fn materialize_jsonl_lines(&self, record: &IntegrationRecord) -> Vec<String>;
    fn materialize_relations(&self, record: &IntegrationRecord) -> Vec<OntologyRelation>;
    fn materialize_relation_jsonl_lines(&self, record: &IntegrationRecord) -> Vec<String>;
}

#[derive(Debug, Clone, Default)]
pub struct BasicOntologyMaterializer;

impl BasicOntologyMaterializer {
    pub fn object_id(record: &IntegrationRecord, object_type: OntologyObjectType) -> String {
        let object_type_token = match object_type {
            OntologyObjectType::Defect => "defect",
            OntologyObjectType::Cause => "cause",
            OntologyObjectType::CompositeCause => "composite_cause",
            OntologyObjectType::Evidence => "evidence",
        };

        let mut hasher = Sha256::new();
        hasher.update(record.source.as_bytes());
        hasher.update([0]);
        hasher.update(record.interface.name.as_bytes());
        hasher.update([0]);
        hasher.update(record.interface.version.as_bytes());
        hasher.update([0]);
        hasher.update(record.record_id.as_bytes());
        hasher.update([0]);
        hasher.update(object_type_token.as_bytes());

        format!("{:x}", hasher.finalize())
    }

    fn lineage(record: &IntegrationRecord) -> OntologyLineage {
        OntologyLineage {
            source: record.source.clone(),
            interface_name: record.interface.name.clone(),
            interface_version: record.interface.version.clone(),
            record_id: record.record_id.clone(),
            ingested_at_unix_ms: record.ingested_at_unix_ms,
            payload_kind: payload_kind(record),
            payload_sha256: payload_sha256(record),
            warning_count: record.warnings.len(),
        }
    }

    fn to_object(record: &IntegrationRecord, object_type: OntologyObjectType) -> OntologyObject {
        OntologyObject {
            object_id: Self::object_id(record, object_type),
            identity_strategy: OntologyIdentityStrategy::DeterministicV1,
            object_type,
            lineage: Self::lineage(record),
            attributes: Self::extract_attributes(record, object_type),
        }
    }

    fn payload_json(record: &IntegrationRecord) -> Option<&Map<String, Value>> {
        match &record.payload {
            Payload::Json(Value::Object(object)) => Some(object),
            _ => None,
        }
    }

    fn detect_types(record: &IntegrationRecord) -> Vec<OntologyObjectType> {
        let mut object_types = Vec::new();

        if let Some(payload) = Self::payload_json(record) {
            if has_any_key(payload, DEFECT_KEYS) {
                object_types.push(OntologyObjectType::Defect);
            }
            if has_any_key(payload, CAUSE_KEYS) {
                object_types.push(OntologyObjectType::Cause);
            }
            if has_any_key(payload, COMPOSITE_CAUSE_KEYS) {
                object_types.push(OntologyObjectType::CompositeCause);
            }
            if has_any_key(payload, EVIDENCE_KEYS) {
                object_types.push(OntologyObjectType::Evidence);
            }
        }

        if object_types.is_empty() {
            object_types.push(OntologyObjectType::Defect);
        }

        object_types
    }

    fn extract_attributes(record: &IntegrationRecord, object_type: OntologyObjectType) -> Value {
        let mut attributes = Map::new();
        attributes.insert(
            "source_record_id".to_string(),
            Value::String(record.record_id.clone()),
        );
        attributes.insert(
            "source_interface".to_string(),
            Value::String(format!(
                "{}:{}",
                record.interface.name, record.interface.version
            )),
        );

        if let Some(payload) = Self::payload_json(record) {
            let selected_keys = match object_type {
                OntologyObjectType::Defect => DEFECT_KEYS,
                OntologyObjectType::Cause => CAUSE_KEYS,
                OntologyObjectType::CompositeCause => COMPOSITE_CAUSE_KEYS,
                OntologyObjectType::Evidence => EVIDENCE_KEYS,
            };

            for key in selected_keys {
                if let Some(value) = payload.get(*key) {
                    attributes.insert((*key).to_string(), value.clone());
                }
            }
        }

        Value::Object(attributes)
    }

    fn relation_id(
        record: &IntegrationRecord,
        relation_type: OntologyRelationType,
        left_object_id: &str,
        right_object_id: &str,
    ) -> String {
        let relation_type_token = match relation_type {
            OntologyRelationType::HasCause => "has_cause",
            OntologyRelationType::SupportedBy => "supported_by",
            OntologyRelationType::CombinesTo => "combines_to",
        };

        let mut hasher = Sha256::new();
        hasher.update(record.source.as_bytes());
        hasher.update([0]);
        hasher.update(record.interface.name.as_bytes());
        hasher.update([0]);
        hasher.update(record.interface.version.as_bytes());
        hasher.update([0]);
        hasher.update(record.record_id.as_bytes());
        hasher.update([0]);
        hasher.update(relation_type_token.as_bytes());
        hasher.update([0]);
        hasher.update(left_object_id.as_bytes());
        hasher.update([0]);
        hasher.update(right_object_id.as_bytes());

        format!("{:x}", hasher.finalize())
    }

    fn relation(
        record: &IntegrationRecord,
        relation_type: OntologyRelationType,
        left_object_id: &str,
        right_object_id: &str,
    ) -> OntologyRelation {
        let mut attributes = Map::new();
        attributes.insert(
            "source_record_id".to_string(),
            Value::String(record.record_id.clone()),
        );

        OntologyRelation {
            relation_id: Self::relation_id(record, relation_type, left_object_id, right_object_id),
            identity_strategy: OntologyIdentityStrategy::DeterministicV1,
            relation_type,
            left_object_id: left_object_id.to_string(),
            right_object_id: right_object_id.to_string(),
            lineage: Self::lineage(record),
            attributes: Value::Object(attributes),
        }
    }

    fn first_object_id(
        objects: &[OntologyObject],
        object_type: OntologyObjectType,
    ) -> Option<&str> {
        objects
            .iter()
            .find(|object| object.object_type == object_type)
            .map(|object| object.object_id.as_str())
    }
}

impl OntologyMaterializer for BasicOntologyMaterializer {
    fn materialize(&self, record: &IntegrationRecord) -> Vec<OntologyObject> {
        Self::detect_types(record)
            .into_iter()
            .map(|object_type| Self::to_object(record, object_type))
            .collect()
    }

    fn materialize_jsonl_lines(&self, record: &IntegrationRecord) -> Vec<String> {
        self.materialize(record)
            .into_iter()
            .filter_map(|object| serde_json::to_string(&object).ok())
            .collect()
    }

    fn materialize_relations(&self, record: &IntegrationRecord) -> Vec<OntologyRelation> {
        let objects = self.materialize(record);
        let defect_id = Self::first_object_id(&objects, OntologyObjectType::Defect);
        let cause_id = Self::first_object_id(&objects, OntologyObjectType::Cause);
        let composite_cause_id =
            Self::first_object_id(&objects, OntologyObjectType::CompositeCause);
        let evidence_id = Self::first_object_id(&objects, OntologyObjectType::Evidence);

        let mut relations = Vec::new();

        if let (Some(defect_id), Some(cause_id)) = (defect_id, cause_id) {
            relations.push(Self::relation(
                record,
                OntologyRelationType::HasCause,
                defect_id,
                cause_id,
            ));
        }

        if let (Some(defect_id), Some(composite_cause_id)) = (defect_id, composite_cause_id) {
            relations.push(Self::relation(
                record,
                OntologyRelationType::HasCause,
                defect_id,
                composite_cause_id,
            ));
        }

        if let (Some(cause_id), Some(evidence_id)) = (cause_id, evidence_id) {
            relations.push(Self::relation(
                record,
                OntologyRelationType::SupportedBy,
                cause_id,
                evidence_id,
            ));
        }

        if let (Some(composite_cause_id), Some(evidence_id)) = (composite_cause_id, evidence_id) {
            relations.push(Self::relation(
                record,
                OntologyRelationType::SupportedBy,
                composite_cause_id,
                evidence_id,
            ));
        }

        if let (Some(cause_id), Some(composite_cause_id)) = (cause_id, composite_cause_id) {
            relations.push(Self::relation(
                record,
                OntologyRelationType::CombinesTo,
                cause_id,
                composite_cause_id,
            ));
        }

        relations
    }

    fn materialize_relation_jsonl_lines(&self, record: &IntegrationRecord) -> Vec<String> {
        self.materialize_relations(record)
            .into_iter()
            .filter_map(|relation| serde_json::to_string(&relation).ok())
            .collect()
    }
}

const DEFECT_KEYS: &[&str] = &["defect_id", "defect_code", "lot_id", "line", "equipment_id"];
const CAUSE_KEYS: &[&str] = &["cause_id", "cause_code", "cause", "category", "confidence"];
const COMPOSITE_CAUSE_KEYS: &[&str] = &[
    "composite_cause_id",
    "composite_cause_code",
    "composite_cause",
    "component_cause_ids",
];
const EVIDENCE_KEYS: &[&str] = &[
    "evidence_id",
    "evidence_type",
    "evidence_url",
    "evidence_path",
    "captured_at",
];

fn has_any_key(object: &Map<String, Value>, keys: &[&str]) -> bool {
    keys.iter().any(|key| object.contains_key(*key))
}

fn payload_kind(record: &IntegrationRecord) -> String {
    match &record.payload {
        Payload::Json(_) => "json".to_string(),
        Payload::Text(_) => "text".to_string(),
        Payload::Binary { .. } => "binary".to_string(),
    }
}

fn payload_sha256(record: &IntegrationRecord) -> String {
    let mut hasher = Sha256::new();
    hasher.update(record.payload.to_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use common::{IntegrationRecord, InterfaceRef, Payload, RecordMetadata};

    use super::{
        BasicOntologyMaterializer, OntologyIdentityStrategy, OntologyMaterializer,
        OntologyObjectType, OntologyRelationType,
    };

    fn sample_record() -> IntegrationRecord {
        IntegrationRecord {
            source: "mes".to_string(),
            interface: InterfaceRef {
                name: "mes".to_string(),
                version: "v1".to_string(),
            },
            record_id: "defect-001".to_string(),
            ingested_at_unix_ms: 1_706_000_000_000,
            payload: Payload::from_text("payload".to_string()),
            metadata: RecordMetadata::default(),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn object_id_is_deterministic_for_same_input() {
        let record = sample_record();
        let first = BasicOntologyMaterializer::object_id(&record, OntologyObjectType::Defect);
        let second = BasicOntologyMaterializer::object_id(&record, OntologyObjectType::Defect);

        assert_eq!(first, second);
    }

    #[test]
    fn object_id_changes_by_object_type() {
        let record = sample_record();
        let defect = BasicOntologyMaterializer::object_id(&record, OntologyObjectType::Defect);
        let cause = BasicOntologyMaterializer::object_id(&record, OntologyObjectType::Cause);

        assert_ne!(defect, cause);
    }

    #[test]
    fn materialize_returns_single_defect_object_with_lineage() {
        let materializer = BasicOntologyMaterializer;
        let record = sample_record();

        let objects = materializer.materialize(&record);
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].object_type, OntologyObjectType::Defect);
        assert_eq!(
            objects[0].identity_strategy,
            OntologyIdentityStrategy::DeterministicV1
        );
        assert_eq!(objects[0].lineage.source, "mes");
        assert_eq!(objects[0].lineage.record_id, "defect-001");
        assert_eq!(objects[0].lineage.payload_kind, "text");
        assert_eq!(objects[0].lineage.warning_count, 0);
    }

    #[test]
    fn materialization_is_idempotent_for_same_input() {
        let materializer = BasicOntologyMaterializer;
        let record = sample_record();

        let first = materializer.materialize(&record);
        let second = materializer.materialize(&record);

        assert_eq!(first, second);
    }

    #[test]
    fn lineage_contains_stable_payload_hash() {
        let materializer = BasicOntologyMaterializer;
        let record = sample_record();

        let first_hash = materializer.materialize(&record)[0]
            .lineage
            .payload_sha256
            .clone();
        let second_hash = materializer.materialize(&record)[0]
            .lineage
            .payload_sha256
            .clone();

        assert_eq!(first_hash, second_hash);
        assert!(!first_hash.is_empty());
    }

    #[test]
    fn fixture_records_materialize_to_expected_type_distribution() {
        let materializer = BasicOntologyMaterializer;
        let records = load_fixture_records("materialization.input.jsonl");

        let mut defect_count = 0usize;
        let mut cause_count = 0usize;
        let mut composite_cause_count = 0usize;
        let mut evidence_count = 0usize;

        for record in &records {
            for object in materializer.materialize(record) {
                match object.object_type {
                    OntologyObjectType::Defect => defect_count += 1,
                    OntologyObjectType::Cause => cause_count += 1,
                    OntologyObjectType::CompositeCause => composite_cause_count += 1,
                    OntologyObjectType::Evidence => evidence_count += 1,
                }
            }
        }

        assert_eq!(defect_count, 3);
        assert_eq!(cause_count, 1);
        assert_eq!(composite_cause_count, 0);
        assert_eq!(evidence_count, 1);
    }

    #[test]
    fn materialized_jsonl_lines_are_valid_and_include_attributes() {
        let materializer = BasicOntologyMaterializer;
        let records = load_fixture_records("materialization.input.jsonl");

        let lines = materializer.materialize_jsonl_lines(&records[1]);
        assert_eq!(lines.len(), 2);

        for line in lines {
            let parsed: serde_json::Value =
                serde_json::from_str(&line).expect("materialized JSONL line should parse");
            assert!(parsed.get("object_id").is_some());
            assert!(parsed.get("object_type").is_some());
            assert!(parsed.get("lineage").is_some());
            assert!(parsed.get("attributes").is_some());
        }
    }

    #[test]
    fn materialize_includes_composite_cause_when_payload_has_keys() {
        let materializer = BasicOntologyMaterializer;
        let record = IntegrationRecord {
            source: "mes".to_string(),
            interface: InterfaceRef {
                name: "mes".to_string(),
                version: "v1".to_string(),
            },
            record_id: "defect-010".to_string(),
            ingested_at_unix_ms: 1_706_000_000_100,
            payload: Payload::from_json(serde_json::json!({
                "defect_id": "D-10",
                "cause_id": "C-1",
                "composite_cause_id": "CC-1"
            })),
            metadata: RecordMetadata::default(),
            warnings: Vec::new(),
        };

        let objects = materializer.materialize(&record);
        assert!(objects
            .iter()
            .any(|object| object.object_type == OntologyObjectType::CompositeCause));
    }

    #[test]
    fn materialize_relations_emits_canonical_relation_types() {
        let materializer = BasicOntologyMaterializer;
        let record = IntegrationRecord {
            source: "mes".to_string(),
            interface: InterfaceRef {
                name: "mes".to_string(),
                version: "v1".to_string(),
            },
            record_id: "defect-011".to_string(),
            ingested_at_unix_ms: 1_706_000_000_101,
            payload: Payload::from_json(serde_json::json!({
                "defect_id": "D-11",
                "cause_id": "C-11",
                "composite_cause_id": "CC-11",
                "evidence_id": "E-11"
            })),
            metadata: RecordMetadata::default(),
            warnings: Vec::new(),
        };

        let relations = materializer.materialize_relations(&record);
        assert!(relations
            .iter()
            .any(|relation| relation.relation_type == OntologyRelationType::HasCause));
        assert!(relations
            .iter()
            .any(|relation| relation.relation_type == OntologyRelationType::SupportedBy));
        assert!(relations
            .iter()
            .any(|relation| relation.relation_type == OntologyRelationType::CombinesTo));
    }

    #[test]
    fn materialize_relation_jsonl_lines_are_valid_json() {
        let materializer = BasicOntologyMaterializer;
        let record = IntegrationRecord {
            source: "mes".to_string(),
            interface: InterfaceRef {
                name: "mes".to_string(),
                version: "v1".to_string(),
            },
            record_id: "defect-012".to_string(),
            ingested_at_unix_ms: 1_706_000_000_102,
            payload: Payload::from_json(serde_json::json!({
                "defect_id": "D-12",
                "cause_id": "C-12"
            })),
            metadata: RecordMetadata::default(),
            warnings: Vec::new(),
        };

        let lines = materializer.materialize_relation_jsonl_lines(&record);
        assert_eq!(lines.len(), 1);

        let parsed: serde_json::Value =
            serde_json::from_str(&lines[0]).expect("relation JSONL line should parse");
        assert!(parsed.get("relation_id").is_some());
        assert!(parsed.get("relation_type").is_some());
        assert!(parsed.get("left_object_id").is_some());
        assert!(parsed.get("right_object_id").is_some());
    }

    fn load_fixture_records(file_name: &str) -> Vec<IntegrationRecord> {
        let fixture_path = fixture_file_path(file_name);
        let content = fs::read_to_string(&fixture_path)
            .unwrap_or_else(|_| panic!("failed to read fixture file: {}", fixture_path.display()));

        content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<IntegrationRecord>(line)
                    .expect("fixture line must parse as IntegrationRecord")
            })
            .collect()
    }

    fn fixture_file_path(file_name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/ontology")
            .join(file_name)
    }
}
