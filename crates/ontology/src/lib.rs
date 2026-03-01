use common::IntegrationRecord;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OntologyObjectType {
    Defect,
    Cause,
    Evidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OntologyLineage {
    pub source: String,
    pub interface_name: String,
    pub interface_version: String,
    pub record_id: String,
    pub ingested_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OntologyObject {
    pub object_id: String,
    pub object_type: OntologyObjectType,
    pub lineage: OntologyLineage,
}

pub trait OntologyMaterializer {
    fn materialize(&self, record: &IntegrationRecord) -> Vec<OntologyObject>;
}

#[derive(Debug, Clone, Default)]
pub struct BasicOntologyMaterializer;

impl BasicOntologyMaterializer {
    pub fn object_id(record: &IntegrationRecord, object_type: OntologyObjectType) -> String {
        let object_type_token = match object_type {
            OntologyObjectType::Defect => "defect",
            OntologyObjectType::Cause => "cause",
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
        }
    }

    fn to_object(record: &IntegrationRecord, object_type: OntologyObjectType) -> OntologyObject {
        OntologyObject {
            object_id: Self::object_id(record, object_type),
            object_type,
            lineage: Self::lineage(record),
        }
    }
}

impl OntologyMaterializer for BasicOntologyMaterializer {
    fn materialize(&self, record: &IntegrationRecord) -> Vec<OntologyObject> {
        vec![Self::to_object(record, OntologyObjectType::Defect)]
    }
}

#[cfg(test)]
mod tests {
    use common::{IntegrationRecord, InterfaceRef, Payload, RecordMetadata};

    use super::{BasicOntologyMaterializer, OntologyMaterializer, OntologyObjectType};

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
        assert_eq!(objects[0].lineage.source, "mes");
        assert_eq!(objects[0].lineage.record_id, "defect-001");
    }
}
