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
    pub score: f32,
    pub reasons: Vec<String>,
    pub lineage: LinkLineage,
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
                    score,
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

#[cfg(test)]
mod tests {
    use super::{
        DeterministicLinkGenerator, ExactRecordIdDeterministicGenerator, LinkSeed,
        PrefixSimilarityProbabilisticGenerator, ProbabilisticLinkGenerator,
    };

    fn seed(left_record_id: &str, right_record_id: &str) -> LinkSeed {
        LinkSeed {
            left_object_id: "defect:obj:1".to_string(),
            right_object_id: "cause:obj:7".to_string(),
            left_record_id: left_record_id.to_string(),
            right_record_id: right_record_id.to_string(),
            left_source: "mes".to_string(),
            right_source: "qms".to_string(),
        }
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
        assert!(candidates[0].score >= 0.6);
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
}
