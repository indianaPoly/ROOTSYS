import { promises as fs } from "node:fs";
import path from "node:path";

type RunArtifact = {
  source: string;
  record_id: string;
  ingested_at_unix_ms: number;
};

type ArtifactState = "ready" | "missing" | "invalid";

export type ArtifactSummary = {
  name: string;
  filePath: string;
  state: ArtifactState;
  totalRecords: number;
  uniqueRecordIds: number;
  sources: string[];
  sampleRecordIds: string[];
  error?: string;
};

export type DashboardData = {
  generatedAt: string;
  artifacts: ArtifactSummary[];
  totals: {
    readyCount: number;
    missingCount: number;
    invalidCount: number;
    totalRecords: number;
  };
};

const trackedArtifacts = [
  { name: "Service Smoke REST", filePath: "/tmp/rootsys-smoke/rest.output.jsonl" },
  { name: "Service Smoke Postgres", filePath: "/tmp/rootsys-smoke/postgres.output.jsonl" },
  { name: "Service Smoke MySQL", filePath: "/tmp/rootsys-smoke/mysql.output.jsonl" },
  { name: "Service Smoke Merge", filePath: "/tmp/rootsys-smoke/merged.db.output.jsonl" },
  {
    name: "Complex Interval Stream",
    filePath: "/tmp/rootsys-complex/stream.interval.output.jsonl"
  },
  { name: "Complex Replay", filePath: "/tmp/rootsys-complex/replay/replay.output.jsonl" },
  { name: "Complex Merge", filePath: "/tmp/rootsys-complex/complex.merged.output.jsonl" }
];

export async function getDashboardData(): Promise<DashboardData> {
  const artifacts = await Promise.all(trackedArtifacts.map(loadArtifact));
  const readyCount = artifacts.filter((artifact) => artifact.state === "ready").length;
  const missingCount = artifacts.filter((artifact) => artifact.state === "missing").length;
  const invalidCount = artifacts.filter((artifact) => artifact.state === "invalid").length;
  const totalRecords = artifacts.reduce((acc, artifact) => acc + artifact.totalRecords, 0);

  return {
    generatedAt: new Date().toISOString(),
    artifacts,
    totals: {
      readyCount,
      missingCount,
      invalidCount,
      totalRecords
    }
  };
}

async function loadArtifact(input: { name: string; filePath: string }): Promise<ArtifactSummary> {
  const normalizedPath = path.resolve(input.filePath);

  try {
    const raw = await fs.readFile(normalizedPath, "utf8");
    const lines = raw
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);

    const records: RunArtifact[] = [];
    for (const line of lines) {
      const parsed = JSON.parse(line) as Record<string, unknown>;
      const source = stringValue(parsed.source);
      const recordId = stringValue(parsed.record_id);
      const ingestedAt = numberValue(parsed.ingested_at_unix_ms);
      records.push({ source, record_id: recordId, ingested_at_unix_ms: ingestedAt });
    }

    const sourceSet = new Set(records.map((record) => record.source));
    const idSet = new Set(records.map((record) => record.record_id));

    return {
      name: input.name,
      filePath: normalizedPath,
      state: "ready",
      totalRecords: records.length,
      uniqueRecordIds: idSet.size,
      sources: [...sourceSet].sort(),
      sampleRecordIds: [...idSet].sort().slice(0, 6)
    };
  } catch (error) {
    if (isMissing(error)) {
      return {
        name: input.name,
        filePath: normalizedPath,
        state: "missing",
        totalRecords: 0,
        uniqueRecordIds: 0,
        sources: [],
        sampleRecordIds: [],
        error: "Artifact file not found. Run the related smoke script first."
      };
    }

    return {
      name: input.name,
      filePath: normalizedPath,
      state: "invalid",
      totalRecords: 0,
      uniqueRecordIds: 0,
      sources: [],
      sampleRecordIds: [],
      error: error instanceof Error ? error.message : "Unknown parse error"
    };
  }
}

function stringValue(input: unknown): string {
  if (typeof input !== "string" || input.length === 0) {
    throw new Error("Required string field is missing in artifact record");
  }
  return input;
}

function numberValue(input: unknown): number {
  if (typeof input !== "number" || Number.isNaN(input)) {
    throw new Error("Required number field is missing in artifact record");
  }
  return input;
}

function isMissing(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as { code: string }).code === "ENOENT";
}
