import { promises as fs } from "node:fs";
import path from "node:path";

export type CandidateStatus = "candidate" | "in_review" | "confirmed" | "rejected";

export type AnalysisCause = {
  id: string;
  title: string;
  description: string;
  confidence: number;
  status: CandidateStatus;
  rule: string;
  evidenceIds: string[];
  updatedAtUnixMs: number;
};

export type AnalysisEvidence = {
  id: string;
  title: string;
  evidenceType: "log" | "report" | "note" | "image" | "unknown";
  source: string;
  ingestedAtUnixMs: number;
};

export type AnalysisWorkspaceData = {
  defectId: string;
  source: string;
  severity: string;
  lotId: string;
  line: string;
  updatedAtUnixMs: number;
  causes: AnalysisCause[];
  evidence: AnalysisEvidence[];
};

export type AnalysisDefectSummary = {
  defectId: string;
  source: string;
  candidateCount: number;
  evidenceCount: number;
  updatedAtUnixMs: number;
};

type OntologyObject = {
  object_id: string;
  object_type: string;
  lineage?: {
    record_id?: string;
    source?: string;
    ingested_at_unix_ms?: number;
  };
  attributes?: Record<string, unknown>;
};

type CandidateLink = {
  link_id?: string;
  left_object_id?: string;
  right_object_id?: string;
  confidence?: number;
  rule?: string;
  status?: string;
  reason_codes?: string[];
};

const integrationOutputs = [
  "/tmp/rootsys-smoke/rest.output.jsonl",
  "/tmp/rootsys-smoke/postgres.output.jsonl",
  "/tmp/rootsys-smoke/mysql.output.jsonl",
  "/tmp/rootsys-smoke/merged.db.output.jsonl",
  "/tmp/rootsys-complex/stream.interval.output.jsonl",
  "/tmp/rootsys-complex/replay/replay.output.jsonl",
  "/tmp/rootsys-complex/complex.merged.output.jsonl"
];

const fallbackDefectId = "D-LOCAL-DEMO";

export async function listDefectSummaries(): Promise<AnalysisDefectSummary[]> {
  const map = await loadWorkspaceMap();
  const summaries = [...map.values()]
    .map((workspace) => ({
      defectId: workspace.defectId,
      source: workspace.source,
      candidateCount: workspace.causes.length,
      evidenceCount: workspace.evidence.length,
      updatedAtUnixMs: workspace.updatedAtUnixMs
    }))
    .sort((a, b) => b.updatedAtUnixMs - a.updatedAtUnixMs);

  if (summaries.length > 0) {
    return summaries;
  }

  return [
    {
      defectId: fallbackDefectId,
      source: "local-demo",
      candidateCount: 2,
      evidenceCount: 2,
      updatedAtUnixMs: Date.now()
    }
  ];
}

export async function getAnalysisWorkspace(defectId: string): Promise<AnalysisWorkspaceData> {
  const map = await loadWorkspaceMap();
  const workspace = map.get(defectId);
  if (workspace) {
    return workspace;
  }

  return {
    defectId,
    source: "local-demo",
    severity: "medium",
    lotId: "LOT-DEMO-01",
    line: "L1",
    updatedAtUnixMs: Date.now(),
    causes: [
      {
        id: "cand-demo-1",
        title: "Temperature drift near end-of-batch",
        description: "Shared lot and timing window suggest thermal instability around lot transition.",
        confidence: 0.81,
        status: "candidate",
        rule: "R2_time_window_shared_attributes",
        evidenceIds: ["ev-demo-1"],
        updatedAtUnixMs: Date.now()
      },
      {
        id: "cand-demo-2",
        title: "Equipment calibration lag",
        description: "Calibration snapshot indicates possible offset compared with previous reference state.",
        confidence: 0.64,
        status: "in_review",
        rule: "R1_strong_key_equipment",
        evidenceIds: ["ev-demo-2"],
        updatedAtUnixMs: Date.now()
      }
    ],
    evidence: [
      {
        id: "ev-demo-1",
        title: "Batch process log excerpt",
        evidenceType: "log",
        source: "local-demo",
        ingestedAtUnixMs: Date.now() - 20_000
      },
      {
        id: "ev-demo-2",
        title: "Calibration inspection report",
        evidenceType: "report",
        source: "local-demo",
        ingestedAtUnixMs: Date.now() - 10_000
      }
    ]
  };
}

async function loadWorkspaceMap(): Promise<Map<string, AnalysisWorkspaceData>> {
  const productDirs = integrationOutputs.map(deriveProductDir);
  const objectRowsNested = await Promise.all(
    productDirs.map((dir) => readJsonl<OntologyObject>(path.join(dir, "ontology.objects.jsonl")))
  );
  const candidateRowsNested = await Promise.all(
    productDirs.map((dir) => readJsonl<CandidateLink>(path.join(dir, "links.r2.jsonl")))
  );

  const objectRows = objectRowsNested.flat();
  const candidateRows = candidateRowsNested.flat();

  const workspaceMap = new Map<string, AnalysisWorkspaceData>();
  const objectToDefect = new Map<string, string>();

  for (const object of objectRows) {
    const attributes = object.attributes ?? {};
    const objectType = stringValue(object.object_type);
    const defectIdFromAttributes = stringValue(attributes.defect_id);
    const defectId = defectIdFromAttributes || (objectType === "defect" ? stringValue(attributes.defect_code) || object.object_id : "");
    if (!defectId) {
      continue;
    }

    const workspace = ensureWorkspace(workspaceMap, defectId, object);
    objectToDefect.set(object.object_id, defectId);

    if (objectType === "evidence") {
      workspace.evidence.push({
        id: object.object_id,
        title: stringValue(attributes.evidence_title) || stringValue(attributes.evidence_id) || "Evidence",
        evidenceType: asEvidenceType(stringValue(attributes.evidence_type)),
        source: stringValue(object.lineage?.source) || workspace.source,
        ingestedAtUnixMs: timestampValue(object.lineage?.ingested_at_unix_ms)
      });
      continue;
    }

    if (objectType === "cause" || objectType === "composite_cause") {
      const causeId = object.object_id;
      workspace.causes.push({
        id: causeId,
        title:
          stringValue(attributes.cause) ||
          stringValue(attributes.composite_cause) ||
          stringValue(attributes.cause_id) ||
          stringValue(attributes.composite_cause_id) ||
          "Cause candidate",
        description:
          stringValue(attributes.description) ||
          stringValue(attributes.category) ||
          (objectType === "composite_cause"
            ? "Composite cause materialized from integration record"
            : "Cause materialized from integration record"),
        confidence: clampConfidence(numberValue(attributes.confidence) || 0.5),
        status: "candidate",
        rule: objectType === "composite_cause" ? "R2_composite" : "R2_candidate",
        evidenceIds: [],
        updatedAtUnixMs: timestampValue(object.lineage?.ingested_at_unix_ms)
      });
    }
  }

  for (const link of candidateRows) {
    const leftId = stringValue(link.left_object_id);
    const rightId = stringValue(link.right_object_id);
    const defectId = objectToDefect.get(leftId) ?? objectToDefect.get(rightId);
    if (!defectId) {
      continue;
    }

    const workspace = workspaceMap.get(defectId);
    if (!workspace) {
      continue;
    }

    const matchedCause = workspace.causes.find((cause) => cause.id === rightId || cause.id === leftId);
    if (!matchedCause) {
      continue;
    }

    matchedCause.confidence = clampConfidence(numberValue(link.confidence) || matchedCause.confidence);
    matchedCause.status = asCandidateStatus(stringValue(link.status));
    matchedCause.rule = stringValue(link.rule) || matchedCause.rule;
  }

  for (const workspace of workspaceMap.values()) {
    const evidenceIds = workspace.evidence.map((item) => item.id);
    for (const cause of workspace.causes) {
      if (cause.evidenceIds.length === 0) {
        cause.evidenceIds = evidenceIds.slice(0, 2);
      }
    }

    workspace.causes.sort((a, b) => b.confidence - a.confidence);
    workspace.evidence.sort((a, b) => b.ingestedAtUnixMs - a.ingestedAtUnixMs);
    workspace.updatedAtUnixMs = Math.max(
      workspace.updatedAtUnixMs,
      ...workspace.causes.map((cause) => cause.updatedAtUnixMs),
      ...workspace.evidence.map((item) => item.ingestedAtUnixMs)
    );
  }

  return workspaceMap;
}

function ensureWorkspace(
  workspaceMap: Map<string, AnalysisWorkspaceData>,
  defectId: string,
  object: OntologyObject
): AnalysisWorkspaceData {
  const existing = workspaceMap.get(defectId);
  if (existing) {
    return existing;
  }

  const attributes = object.attributes ?? {};
  const workspace: AnalysisWorkspaceData = {
    defectId,
    source: stringValue(object.lineage?.source) || "unknown",
    severity: stringValue(attributes.severity) || "unknown",
    lotId: stringValue(attributes.lot_id) || "-",
    line: stringValue(attributes.line) || "-",
    updatedAtUnixMs: timestampValue(object.lineage?.ingested_at_unix_ms),
    causes: [],
    evidence: []
  };
  workspaceMap.set(defectId, workspace);
  return workspace;
}

function deriveProductDir(outputPath: string): string {
  if (outputPath.endsWith(".jsonl")) {
    return outputPath.slice(0, -6) + ".product";
  }
  return `${outputPath}.product`;
}

async function readJsonl<T>(filePath: string): Promise<T[]> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return raw
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .flatMap((line) => {
        try {
          return [JSON.parse(line) as T];
        } catch {
          return [];
        }
      });
  } catch {
    return [];
  }
}

function asEvidenceType(value: string): AnalysisEvidence["evidenceType"] {
  if (value === "log" || value === "report" || value === "note" || value === "image") {
    return value;
  }
  return "unknown";
}

function asCandidateStatus(value: string): CandidateStatus {
  if (value === "candidate" || value === "in_review" || value === "confirmed" || value === "rejected") {
    return value;
  }
  return "candidate";
}

function stringValue(input: unknown): string {
  return typeof input === "string" ? input.trim() : "";
}

function numberValue(input: unknown): number {
  return typeof input === "number" && Number.isFinite(input) ? input : 0;
}

function timestampValue(input: unknown): number {
  return typeof input === "number" && Number.isFinite(input) ? input : Date.now();
}

function clampConfidence(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  if (value < 0) {
    return 0;
  }
  if (value > 1) {
    return 1;
  }
  return value;
}
