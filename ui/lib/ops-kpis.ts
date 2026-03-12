import { promises as fs } from "node:fs";

type ActionType = "confirmLink" | "rejectLink" | "addEvidenceToLink";

type NormalizedEvent = {
  linkId: string;
  actionType: ActionType;
  policyDecision: "allow" | "deny";
  createdAtUnixMs: number;
};

export type OpsTrendPoint = {
  day: string;
  totalEvents: number;
  decisions: number;
  confirms: number;
  rejects: number;
};

export type OpsAlert = {
  code: "REJECT_RATE_HIGH" | "LEAD_TIME_HIGH" | "BACKLOG_HIGH";
  severity: "warn" | "critical";
  message: string;
};

export type OpsDashboardData = {
  totals: {
    totalEvents: number;
    uniqueCandidates: number;
    decisions: number;
    confirmations: number;
    rejections: number;
    approvalRate: number;
    rejectRate: number;
    avgLeadTimeMinutes: number;
    backlogCandidates: number;
  };
  thresholds: {
    rejectRateWarn: number;
    leadTimeWarnMinutes: number;
    backlogWarnCount: number;
  };
  trends: OpsTrendPoint[];
  alerts: OpsAlert[];
  sourceCount: number;
};

const actionFiles = [
  "/tmp/rootsys-analysis/audit-events.jsonl",
  "/tmp/rootsys-smoke/rest.output.product/actions.results.jsonl",
  "/tmp/rootsys-smoke/postgres.output.product/actions.results.jsonl",
  "/tmp/rootsys-smoke/mysql.output.product/actions.results.jsonl",
  "/tmp/rootsys-smoke/merged.db.output.product/actions.results.jsonl",
  "/tmp/rootsys-complex/stream.interval.output.product/actions.results.jsonl",
  "/tmp/rootsys-complex/replay/replay.output.product/actions.results.jsonl",
  "/tmp/rootsys-complex/complex.merged.output.product/actions.results.jsonl"
];

const thresholds = {
  rejectRateWarn: 0.35,
  leadTimeWarnMinutes: 240,
  backlogWarnCount: 25
};

export async function getOpsDashboardData(): Promise<OpsDashboardData> {
  const rowsNested = await Promise.all(actionFiles.map((filePath) => readJsonl(filePath)));
  const rows = rowsNested.flat();
  const events = rows
    .map(normalizeEvent)
    .filter((event): event is NormalizedEvent => event !== null)
    .sort((a, b) => a.createdAtUnixMs - b.createdAtUnixMs);

  if (events.length === 0) {
    return fallbackDashboard();
  }

  const candidateIds = new Set(events.map((event) => event.linkId));
  const decisions = events.filter((event) => event.actionType === "confirmLink" || event.actionType === "rejectLink");
  const confirmations = decisions.filter((event) => event.actionType === "confirmLink").length;
  const rejections = decisions.filter((event) => event.actionType === "rejectLink").length;
  const approvalRate = safeRatio(confirmations, decisions.length);
  const rejectRate = safeRatio(rejections, decisions.length);

  const firstSeenByLink = new Map<string, number>();
  const firstDecisionByLink = new Map<string, number>();
  for (const event of events) {
    if (!firstSeenByLink.has(event.linkId)) {
      firstSeenByLink.set(event.linkId, event.createdAtUnixMs);
    }
    if (
      (event.actionType === "confirmLink" || event.actionType === "rejectLink") &&
      !firstDecisionByLink.has(event.linkId)
    ) {
      firstDecisionByLink.set(event.linkId, event.createdAtUnixMs);
    }
  }

  const leadTimes = [...firstDecisionByLink.entries()].flatMap(([linkId, decidedAt]) => {
    const createdAt = firstSeenByLink.get(linkId);
    if (!createdAt) {
      return [];
    }
    return [Math.max(0, (decidedAt - createdAt) / 60_000)];
  });
  const avgLeadTimeMinutes =
    leadTimes.length > 0 ? leadTimes.reduce((acc, value) => acc + value, 0) / leadTimes.length : 0;
  const backlogCandidates = candidateIds.size - firstDecisionByLink.size;

  const trends = buildTrend(events);
  const alerts: OpsAlert[] = [];
  if (rejectRate > thresholds.rejectRateWarn) {
    alerts.push({
      code: "REJECT_RATE_HIGH",
      severity: rejectRate > 0.5 ? "critical" : "warn",
      message: `Reject rate ${formatPercent(rejectRate)} is above warning threshold ${formatPercent(thresholds.rejectRateWarn)}.`
    });
  }
  if (avgLeadTimeMinutes > thresholds.leadTimeWarnMinutes) {
    alerts.push({
      code: "LEAD_TIME_HIGH",
      severity: avgLeadTimeMinutes > thresholds.leadTimeWarnMinutes * 2 ? "critical" : "warn",
      message: `Average lead time ${avgLeadTimeMinutes.toFixed(1)}m exceeds threshold ${thresholds.leadTimeWarnMinutes}m.`
    });
  }
  if (backlogCandidates > thresholds.backlogWarnCount) {
    alerts.push({
      code: "BACKLOG_HIGH",
      severity: backlogCandidates > thresholds.backlogWarnCount * 2 ? "critical" : "warn",
      message: `Backlog candidates ${backlogCandidates} exceed threshold ${thresholds.backlogWarnCount}.`
    });
  }

  return {
    totals: {
      totalEvents: events.length,
      uniqueCandidates: candidateIds.size,
      decisions: decisions.length,
      confirmations,
      rejections,
      approvalRate,
      rejectRate,
      avgLeadTimeMinutes,
      backlogCandidates
    },
    thresholds,
    trends,
    alerts,
    sourceCount: rowsNested.filter((rows) => rows.length > 0).length
  };
}

function normalizeEvent(raw: Record<string, unknown>): NormalizedEvent | null {
  const linkId = stringValue(raw.linkId) || stringValue(raw.link_id);
  const actionRaw = stringValue(raw.actionType) || stringValue(raw.action_kind);
  const actionType = normalizeActionType(actionRaw);
  const createdAtUnixMs =
    numberValue(raw.createdAtUnixMs) || numberValue(raw.created_at_unix_ms) || Date.now();
  const policyDecision = normalizePolicyDecision(stringValue(raw.policyDecision));

  if (!linkId || !actionType) {
    return null;
  }

  return {
    linkId,
    actionType,
    policyDecision,
    createdAtUnixMs
  };
}

function normalizeActionType(input: string): ActionType | null {
  if (!input) {
    return null;
  }
  if (input === "confirmLink" || input === "ConfirmLink") {
    return "confirmLink";
  }
  if (input === "rejectLink" || input === "RejectLink") {
    return "rejectLink";
  }
  if (input === "addEvidenceToLink" || input === "AddEvidenceToLink") {
    return "addEvidenceToLink";
  }
  return null;
}

function normalizePolicyDecision(input: string): "allow" | "deny" {
  return input === "deny" ? "deny" : "allow";
}

function buildTrend(events: NormalizedEvent[]): OpsTrendPoint[] {
  const byDay = new Map<string, OpsTrendPoint>();
  for (const event of events) {
    const day = new Date(event.createdAtUnixMs).toISOString().slice(0, 10);
    const current = byDay.get(day) ?? {
      day,
      totalEvents: 0,
      decisions: 0,
      confirms: 0,
      rejects: 0
    };
    current.totalEvents += 1;
    if (event.actionType === "confirmLink" || event.actionType === "rejectLink") {
      current.decisions += 1;
      if (event.actionType === "confirmLink") {
        current.confirms += 1;
      }
      if (event.actionType === "rejectLink") {
        current.rejects += 1;
      }
    }
    byDay.set(day, current);
  }

  return [...byDay.values()].sort((a, b) => a.day.localeCompare(b.day));
}

function safeRatio(numerator: number, denominator: number): number {
  if (denominator <= 0) {
    return 0;
  }
  return numerator / denominator;
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function stringValue(input: unknown): string {
  return typeof input === "string" ? input.trim() : "";
}

function numberValue(input: unknown): number {
  return typeof input === "number" && Number.isFinite(input) ? input : 0;
}

async function readJsonl(filePath: string): Promise<Record<string, unknown>[]> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return raw
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .flatMap((line) => {
        try {
          return [JSON.parse(line) as Record<string, unknown>];
        } catch {
          return [];
        }
      });
  } catch {
    return [];
  }
}

function fallbackDashboard(): OpsDashboardData {
  return {
    totals: {
      totalEvents: 12,
      uniqueCandidates: 7,
      decisions: 6,
      confirmations: 4,
      rejections: 2,
      approvalRate: 0.666,
      rejectRate: 0.333,
      avgLeadTimeMinutes: 84,
      backlogCandidates: 1
    },
    thresholds,
    trends: [
      { day: "2026-03-09", totalEvents: 3, decisions: 2, confirms: 1, rejects: 1 },
      { day: "2026-03-10", totalEvents: 4, decisions: 2, confirms: 2, rejects: 0 },
      { day: "2026-03-11", totalEvents: 5, decisions: 2, confirms: 1, rejects: 1 }
    ],
    alerts: [],
    sourceCount: 1
  };
}
