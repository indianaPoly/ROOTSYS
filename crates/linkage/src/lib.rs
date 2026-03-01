use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkSeed {
    pub left_object_id: String,
    pub right_object_id: String,
    pub left_record_id: String,
    pub right_record_id: String,
    pub left_source: String,
    pub right_source: String,
    #[serde(default)]
    pub left_strong_keys: BTreeMap<String, String>,
    #[serde(default)]
    pub right_strong_keys: BTreeMap<String, String>,
    #[serde(default)]
    pub left_attributes: BTreeMap<String, String>,
    #[serde(default)]
    pub right_attributes: BTreeMap<String, String>,
    #[serde(default)]
    pub left_event_unix_ms: Option<i64>,
    #[serde(default)]
    pub right_event_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkLineage {
    pub left_record_id: String,
    pub right_record_id: String,
    pub left_source: String,
    pub right_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicLink {
    pub link_id: String,
    pub relation: String,
    pub rule: String,
    pub lineage: LinkLineage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateLink {
    pub link_id: String,
    pub relation: String,
    pub confidence: f32,
    pub reasons: Vec<String>,
    pub lineage: LinkLineage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateSchemaError {
    MissingLinkId,
    MissingRelation,
    InvalidConfidence,
    EmptyReasons,
    MissingLineageRecordIds,
}

impl CandidateLink {
    pub fn validate_schema(&self) -> Result<(), CandidateSchemaError> {
        if self.link_id.trim().is_empty() {
            return Err(CandidateSchemaError::MissingLinkId);
        }
        if self.relation.trim().is_empty() {
            return Err(CandidateSchemaError::MissingRelation);
        }
        if !(0.0..=1.0).contains(&self.confidence) {
            return Err(CandidateSchemaError::InvalidConfidence);
        }
        if self.reasons.is_empty() {
            return Err(CandidateSchemaError::EmptyReasons);
        }
        if self.lineage.left_record_id.trim().is_empty()
            || self.lineage.right_record_id.trim().is_empty()
        {
            return Err(CandidateSchemaError::MissingLineageRecordIds);
        }
        Ok(())
    }
}

pub trait DeterministicLinkGenerator {
    fn generate(&self, seed: &LinkSeed) -> Option<DeterministicLink>;
}

pub trait ProbabilisticLinkGenerator {
    fn generate_candidates(&self, seeds: &[LinkSeed]) -> Vec<CandidateLink>;
}

#[derive(Debug, Clone)]
pub struct ExactRecordIdDeterministicGenerator {
    pub relation: String,
    pub rule: String,
}

#[derive(Debug, Clone)]
pub struct StrongKeyDeterministicGenerator {
    pub relation: String,
    pub rule_prefix: String,
}

impl StrongKeyDeterministicGenerator {
    pub fn new(relation: impl Into<String>) -> Self {
        Self {
            relation: relation.into(),
            rule_prefix: "r1_strong_keys".to_string(),
        }
    }

    fn matched_strong_keys(&self, seed: &LinkSeed) -> Vec<String> {
        let mut matched = Vec::new();
        for key in STRONG_KEY_PRIORITY {
            let left = seed.left_strong_keys.get(*key).map(String::as_str);
            let right = seed.right_strong_keys.get(*key).map(String::as_str);

            if let (Some(left), Some(right)) = (left, right) {
                if !left.trim().is_empty() && left == right {
                    matched.push((*key).to_string());
                }
            }
        }

        matched
    }
}

impl DeterministicLinkGenerator for StrongKeyDeterministicGenerator {
    fn generate(&self, seed: &LinkSeed) -> Option<DeterministicLink> {
        let matched_keys = self.matched_strong_keys(seed);
        if matched_keys.is_empty() {
            return None;
        }

        let rule = format!("{}:{}", self.rule_prefix, matched_keys.join("+"));

        Some(DeterministicLink {
            link_id: deterministic_link_id(
                &seed.left_object_id,
                &seed.right_object_id,
                &self.relation,
                &rule,
            ),
            relation: self.relation.clone(),
            rule,
            lineage: LinkLineage {
                left_record_id: seed.left_record_id.clone(),
                right_record_id: seed.right_record_id.clone(),
                left_source: seed.left_source.clone(),
                right_source: seed.right_source.clone(),
            },
        })
    }
}

impl ExactRecordIdDeterministicGenerator {
    pub fn new(relation: impl Into<String>, rule: impl Into<String>) -> Self {
        Self {
            relation: relation.into(),
            rule: rule.into(),
        }
    }
}

impl DeterministicLinkGenerator for ExactRecordIdDeterministicGenerator {
    fn generate(&self, seed: &LinkSeed) -> Option<DeterministicLink> {
        if seed.left_record_id != seed.right_record_id {
            return None;
        }

        Some(DeterministicLink {
            link_id: deterministic_link_id(
                &seed.left_object_id,
                &seed.right_object_id,
                &self.relation,
                &self.rule,
            ),
            relation: self.relation.clone(),
            rule: self.rule.clone(),
            lineage: LinkLineage {
                left_record_id: seed.left_record_id.clone(),
                right_record_id: seed.right_record_id.clone(),
                left_source: seed.left_source.clone(),
                right_source: seed.right_source.clone(),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct PrefixSimilarityProbabilisticGenerator {
    pub relation: String,
    pub min_score: f32,
}

#[derive(Debug, Clone)]
pub struct LightweightR2Config {
    pub max_time_diff_ms: i64,
    pub min_shared_attributes: usize,
    pub min_score: f32,
    pub attribute_keys: Vec<String>,
    pub time_weight: f32,
    pub attribute_weight: f32,
}

impl Default for LightweightR2Config {
    fn default() -> Self {
        Self {
            max_time_diff_ms: 300_000,
            min_shared_attributes: 1,
            min_score: 0.4,
            attribute_keys: vec![
                "lot_id".to_string(),
                "line".to_string(),
                "equipment_id".to_string(),
            ],
            time_weight: 0.4,
            attribute_weight: 0.6,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LightweightR2ProbabilisticGenerator {
    pub relation: String,
    pub config: LightweightR2Config,
}

impl LightweightR2ProbabilisticGenerator {
    pub fn new(relation: impl Into<String>, config: LightweightR2Config) -> Self {
        Self {
            relation: relation.into(),
            config,
        }
    }
}

impl PrefixSimilarityProbabilisticGenerator {
    pub fn new(relation: impl Into<String>, min_score: f32) -> Self {
        Self {
            relation: relation.into(),
            min_score,
        }
    }
}

impl ProbabilisticLinkGenerator for PrefixSimilarityProbabilisticGenerator {
    fn generate_candidates(&self, seeds: &[LinkSeed]) -> Vec<CandidateLink> {
        seeds
            .iter()
            .filter_map(|seed| {
                let score = prefix_similarity(&seed.left_record_id, &seed.right_record_id);
                if score < self.min_score {
                    return None;
                }

                let reason = format!(
                    "prefix_similarity(left={}, right={})={:.3}",
                    seed.left_record_id, seed.right_record_id, score
                );

                Some(CandidateLink {
                    link_id: candidate_link_id(
                        &seed.left_object_id,
                        &seed.right_object_id,
                        &self.relation,
                        score,
                    ),
                    relation: self.relation.clone(),
                    confidence: score,
                    reasons: vec![reason],
                    lineage: LinkLineage {
                        left_record_id: seed.left_record_id.clone(),
                        right_record_id: seed.right_record_id.clone(),
                        left_source: seed.left_source.clone(),
                        right_source: seed.right_source.clone(),
                    },
                })
            })
            .collect()
    }
}

impl ProbabilisticLinkGenerator for LightweightR2ProbabilisticGenerator {
    fn generate_candidates(&self, seeds: &[LinkSeed]) -> Vec<CandidateLink> {
        seeds
            .iter()
            .filter_map(|seed| {
                let (left_ts, right_ts) = match (seed.left_event_unix_ms, seed.right_event_unix_ms)
                {
                    (Some(left), Some(right)) => (left, right),
                    _ => return None,
                };

                let time_diff_ms = (left_ts - right_ts).abs();
                if time_diff_ms > self.config.max_time_diff_ms {
                    return None;
                }

                let mut matched_keys = Vec::new();
                let mut inspected_count = 0usize;

                for key in &self.config.attribute_keys {
                    let left = seed.left_attributes.get(key).map(String::as_str);
                    let right = seed.right_attributes.get(key).map(String::as_str);

                    if let (Some(left), Some(right)) = (left, right) {
                        inspected_count += 1;
                        if !left.trim().is_empty() && left == right {
                            matched_keys.push(key.clone());
                        }
                    }
                }

                if matched_keys.len() < self.config.min_shared_attributes {
                    return None;
                }

                if inspected_count == 0 {
                    return None;
                }

                let time_score = 1.0 - (time_diff_ms as f32 / self.config.max_time_diff_ms as f32);
                let attribute_score = matched_keys.len() as f32 / inspected_count as f32;
                let score = (self.config.time_weight * time_score)
                    + (self.config.attribute_weight * attribute_score);

                if score < self.config.min_score {
                    return None;
                }

                let reason = format!(
                    "r2_lightweight(time_diff_ms={}, matched_keys={})",
                    time_diff_ms,
                    matched_keys.join("+")
                );

                Some(CandidateLink {
                    link_id: candidate_link_id(
                        &seed.left_object_id,
                        &seed.right_object_id,
                        &self.relation,
                        score,
                    ),
                    relation: self.relation.clone(),
                    confidence: score,
                    reasons: vec![reason],
                    lineage: LinkLineage {
                        left_record_id: seed.left_record_id.clone(),
                        right_record_id: seed.right_record_id.clone(),
                        left_source: seed.left_source.clone(),
                        right_source: seed.right_source.clone(),
                    },
                })
            })
            .collect()
    }
}

fn deterministic_link_id(
    left_object_id: &str,
    right_object_id: &str,
    relation: &str,
    rule: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(left_object_id.as_bytes());
    hasher.update([0]);
    hasher.update(right_object_id.as_bytes());
    hasher.update([0]);
    hasher.update(relation.as_bytes());
    hasher.update([0]);
    hasher.update(rule.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn candidate_link_id(
    left_object_id: &str,
    right_object_id: &str,
    relation: &str,
    score: f32,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(left_object_id.as_bytes());
    hasher.update([0]);
    hasher.update(right_object_id.as_bytes());
    hasher.update([0]);
    hasher.update(relation.as_bytes());
    hasher.update([0]);
    hasher.update(format!("{:.6}", score).as_bytes());
    format!("{:x}", hasher.finalize())
}

fn prefix_similarity(left: &str, right: &str) -> f32 {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();

    if left_chars.is_empty() || right_chars.is_empty() {
        return 0.0;
    }

    let max_len = left_chars.len().max(right_chars.len()) as f32;
    let mut shared_prefix_len = 0usize;

    for (a, b) in left_chars.iter().zip(right_chars.iter()) {
        if a == b {
            shared_prefix_len += 1;
        } else {
            break;
        }
    }

    shared_prefix_len as f32 / max_len
}

const STRONG_KEY_PRIORITY: &[&str] = &["defect_id", "lot_id", "equipment_id", "cause_id"];

#[cfg(test)]
mod tests {
    use super::{
        CandidateLink, CandidateSchemaError, DeterministicLinkGenerator,
        ExactRecordIdDeterministicGenerator, LightweightR2Config,
        LightweightR2ProbabilisticGenerator, LinkLineage, LinkSeed,
        PrefixSimilarityProbabilisticGenerator, ProbabilisticLinkGenerator,
        StrongKeyDeterministicGenerator,
    };

    fn seed(left_record_id: &str, right_record_id: &str) -> LinkSeed {
        LinkSeed {
            left_object_id: "defect:obj:1".to_string(),
            right_object_id: "cause:obj:7".to_string(),
            left_record_id: left_record_id.to_string(),
            right_record_id: right_record_id.to_string(),
            left_source: "mes".to_string(),
            right_source: "qms".to_string(),
            left_strong_keys: std::collections::BTreeMap::new(),
            right_strong_keys: std::collections::BTreeMap::new(),
            left_attributes: std::collections::BTreeMap::new(),
            right_attributes: std::collections::BTreeMap::new(),
            left_event_unix_ms: None,
            right_event_unix_ms: None,
        }
    }

    fn seed_with_strong_key(key: &str, value: &str) -> LinkSeed {
        let mut seed = seed("left-record", "right-record");
        seed.left_strong_keys
            .insert(key.to_string(), value.to_string());
        seed.right_strong_keys
            .insert(key.to_string(), value.to_string());
        seed
    }

    #[test]
    fn deterministic_generator_emits_link_for_exact_record_id_match() {
        let generator = ExactRecordIdDeterministicGenerator::new("has_cause", "r1_exact_record_id");
        let result = generator.generate(&seed("defect-001", "defect-001"));

        assert!(result.is_some());
        let link = result.expect("deterministic link should be emitted");
        assert_eq!(link.relation, "has_cause");
        assert_eq!(link.rule, "r1_exact_record_id");
        assert_eq!(link.lineage.left_source, "mes");
        assert_eq!(link.lineage.right_source, "qms");
    }

    #[test]
    fn deterministic_generator_skips_link_for_mismatch() {
        let generator = ExactRecordIdDeterministicGenerator::new("has_cause", "r1_exact_record_id");
        let result = generator.generate(&seed("defect-001", "defect-002"));

        assert!(result.is_none());
    }

    #[test]
    fn probabilistic_generator_emits_candidates_above_threshold() {
        let generator = PrefixSimilarityProbabilisticGenerator::new("candidate_of", 0.6);
        let seeds = vec![seed("defect-100", "defect-199"), seed("abc", "xyz")];

        let candidates = generator.generate_candidates(&seeds);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].relation, "candidate_of");
        assert!(candidates[0].confidence >= 0.6);
        assert_eq!(candidates[0].lineage.left_record_id, "defect-100");
        assert_eq!(candidates[0].lineage.right_record_id, "defect-199");
    }

    #[test]
    fn probabilistic_generator_filters_out_low_similarity() {
        let generator = PrefixSimilarityProbabilisticGenerator::new("candidate_of", 0.9);
        let seeds = vec![seed("defect-100", "defect-199")];

        let candidates = generator.generate_candidates(&seeds);
        assert!(candidates.is_empty());
    }

    #[test]
    fn strong_key_generator_emits_link_when_defect_id_matches() {
        let generator = StrongKeyDeterministicGenerator::new("has_cause");
        let result = generator.generate(&seed_with_strong_key("defect_id", "D-100"));

        assert!(result.is_some());
        let link = result.expect("link should be emitted");
        assert_eq!(link.relation, "has_cause");
        assert_eq!(link.rule, "r1_strong_keys:defect_id");
    }

    #[test]
    fn strong_key_generator_uses_multiple_matched_keys_in_rule() {
        let generator = StrongKeyDeterministicGenerator::new("has_cause");
        let mut seed = seed("left-record", "right-record");
        seed.left_strong_keys
            .insert("defect_id".to_string(), "D-100".to_string());
        seed.right_strong_keys
            .insert("defect_id".to_string(), "D-100".to_string());
        seed.left_strong_keys
            .insert("lot_id".to_string(), "LOT-10".to_string());
        seed.right_strong_keys
            .insert("lot_id".to_string(), "LOT-10".to_string());

        let result = generator.generate(&seed);
        assert!(result.is_some());
        let link = result.expect("link should be emitted");
        assert_eq!(link.rule, "r1_strong_keys:defect_id+lot_id");
    }

    #[test]
    fn strong_key_generator_skips_when_keys_do_not_match() {
        let generator = StrongKeyDeterministicGenerator::new("has_cause");
        let mut seed = seed("left-record", "right-record");
        seed.left_strong_keys
            .insert("defect_id".to_string(), "D-100".to_string());
        seed.right_strong_keys
            .insert("defect_id".to_string(), "D-101".to_string());

        let result = generator.generate(&seed);
        assert!(result.is_none());
    }

    #[test]
    fn lightweight_r2_emits_candidate_within_time_window_and_shared_attributes() {
        let config = LightweightR2Config {
            max_time_diff_ms: 120_000,
            min_shared_attributes: 2,
            min_score: 0.5,
            attribute_keys: vec![
                "lot_id".to_string(),
                "line".to_string(),
                "equipment_id".to_string(),
            ],
            time_weight: 0.4,
            attribute_weight: 0.6,
        };
        let generator = LightweightR2ProbabilisticGenerator::new("candidate_of", config);

        let mut candidate_seed = seed("left-record", "right-record");
        candidate_seed.left_event_unix_ms = Some(1_706_000_000_000);
        candidate_seed.right_event_unix_ms = Some(1_706_000_030_000);
        candidate_seed
            .left_attributes
            .insert("lot_id".to_string(), "LOT-10".to_string());
        candidate_seed
            .right_attributes
            .insert("lot_id".to_string(), "LOT-10".to_string());
        candidate_seed
            .left_attributes
            .insert("line".to_string(), "L1".to_string());
        candidate_seed
            .right_attributes
            .insert("line".to_string(), "L1".to_string());
        candidate_seed
            .left_attributes
            .insert("equipment_id".to_string(), "EQ-1".to_string());
        candidate_seed
            .right_attributes
            .insert("equipment_id".to_string(), "EQ-9".to_string());

        let candidates = generator.generate_candidates(&[candidate_seed]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].relation, "candidate_of");
        assert!(candidates[0].confidence >= 0.5);
        assert!(candidates[0].reasons[0].contains("matched_keys=lot_id+line"));
    }

    #[test]
    fn lightweight_r2_skips_candidate_when_time_window_exceeded() {
        let generator = LightweightR2ProbabilisticGenerator::new(
            "candidate_of",
            LightweightR2Config::default(),
        );

        let mut candidate_seed = seed("left-record", "right-record");
        candidate_seed.left_event_unix_ms = Some(1_706_000_000_000);
        candidate_seed.right_event_unix_ms = Some(1_706_000_500_001);
        candidate_seed
            .left_attributes
            .insert("lot_id".to_string(), "LOT-10".to_string());
        candidate_seed
            .right_attributes
            .insert("lot_id".to_string(), "LOT-10".to_string());

        let candidates = generator.generate_candidates(&[candidate_seed]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn lightweight_r2_skips_candidate_when_shared_attributes_below_threshold() {
        let config = LightweightR2Config {
            min_shared_attributes: 2,
            ..LightweightR2Config::default()
        };
        let generator = LightweightR2ProbabilisticGenerator::new("candidate_of", config);

        let mut candidate_seed = seed("left-record", "right-record");
        candidate_seed.left_event_unix_ms = Some(1_706_000_000_000);
        candidate_seed.right_event_unix_ms = Some(1_706_000_010_000);
        candidate_seed
            .left_attributes
            .insert("lot_id".to_string(), "LOT-10".to_string());
        candidate_seed
            .right_attributes
            .insert("lot_id".to_string(), "LOT-10".to_string());

        let candidates = generator.generate_candidates(&[candidate_seed]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn candidate_schema_validation_accepts_required_fields() {
        let candidate = CandidateLink {
            link_id: "candidate-link-1".to_string(),
            relation: "candidate_of".to_string(),
            confidence: 0.77,
            reasons: vec!["matched lot_id".to_string()],
            lineage: LinkLineage {
                left_record_id: "left-1".to_string(),
                right_record_id: "right-1".to_string(),
                left_source: "mes".to_string(),
                right_source: "qms".to_string(),
            },
        };

        assert_eq!(candidate.validate_schema(), Ok(()));
    }

    #[test]
    fn candidate_schema_validation_rejects_invalid_confidence() {
        let candidate = CandidateLink {
            link_id: "candidate-link-2".to_string(),
            relation: "candidate_of".to_string(),
            confidence: 1.2,
            reasons: vec!["matched lot_id".to_string()],
            lineage: LinkLineage {
                left_record_id: "left-1".to_string(),
                right_record_id: "right-1".to_string(),
                left_source: "mes".to_string(),
                right_source: "qms".to_string(),
            },
        };

        assert_eq!(
            candidate.validate_schema(),
            Err(CandidateSchemaError::InvalidConfidence)
        );
    }
}
