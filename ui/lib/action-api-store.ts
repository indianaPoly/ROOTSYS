import { promises as fs } from "node:fs";
import path from "node:path";

type CandidateState = "candidate" | "in_review" | "confirmed" | "rejected";
type ActionType = "confirmLink" | "rejectLink" | "addEvidenceToLink";

type ConfirmPayload = {
  linkId: string;
  justification: string;
  confidenceOverride?: number;
};

type RejectPayload = {
  linkId: string;
  reason: string;
  reasonCode?: string;
};

type AddEvidencePayload = {
  linkId: string;
  evidenceId: string;
  description?: string;
};

type ActionRequest = {
  actorId: string;
  actorRole: string;
  actionType: ActionType;
  payload: ConfirmPayload | RejectPayload | AddEvidencePayload;
};

type CandidateTransition = {
  linkId: string;
  actionType: ActionType;
  fromState: CandidateState;
  toState: CandidateState;
  actorId: string;
  actorRole: string;
  transitionedAtUnixMs: number;
};

type AuditEvent = {
  eventId: number;
  actionId: string;
  actorId: string;
  actorRole: string;
  actionType: ActionType;
  linkId: string;
  summary: string;
  policyDecision: "allow" | "deny";
  createdAtUnixMs: number;
};

const storeDir = "/tmp/rootsys-analysis";
const statePath = path.join(storeDir, "candidate-state.json");
const historyPath = path.join(storeDir, "candidate-history.jsonl");
const auditPath = path.join(storeDir, "audit-events.jsonl");

type ApiErrorCode =
  | "POLICY_PERMISSION_DENIED"
  | "ACTION_INVALID_LINK_STATE"
  | "ACTION_VALIDATION_FAILED"
  | "ACTION_UNKNOWN"
  | "AUDIT_QUERY_INVALID";

export class ApiError extends Error {
  readonly code: ApiErrorCode;
  readonly status: number;

  constructor(code: ApiErrorCode, message: string, status: number) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

export async function executeAction(request: ActionRequest): Promise<{
  actionId: string;
  actionType: ActionType;
  linkId: string;
  previousState: CandidateState;
  currentState: CandidateState;
  auditEventId: number;
  policyDecision: "allow";
}> {
  validatePermission(request.actorRole, request.actionType);
  validatePayload(request.actionType, request.payload);

  const linkId = getLinkId(request.payload);
  const currentStates = await readCandidateStates();
  const previousState = currentStates[linkId] ?? "candidate";
  const currentState = nextState(previousState, request.actionType);
  const now = Date.now();

  const auditEvents = await readJsonl<AuditEvent>(auditPath);
  const auditEventId = (auditEvents[0]?.eventId ?? 0) + 1;
  const actionId = `act-${auditEventId}`;

  currentStates[linkId] = currentState;
  await writeJson(statePath, currentStates);

  const transition: CandidateTransition = {
    linkId,
    actionType: request.actionType,
    fromState: previousState,
    toState: currentState,
    actorId: request.actorId,
    actorRole: request.actorRole,
    transitionedAtUnixMs: now
  };
  await appendJsonl(historyPath, transition);

  const auditEvent: AuditEvent = {
    eventId: auditEventId,
    actionId,
    actorId: request.actorId,
    actorRole: request.actorRole,
    actionType: request.actionType,
    linkId,
    summary: summarizeAction(request.actionType, request.payload),
    policyDecision: "allow",
    createdAtUnixMs: now
  };
  await appendJsonl(auditPath, auditEvent);

  return {
    actionId,
    actionType: request.actionType,
    linkId,
    previousState,
    currentState,
    auditEventId,
    policyDecision: "allow"
  };
}

export async function queryAudit(params: { linkId?: string; limit?: number }): Promise<AuditEvent[]> {
  const limit = params.limit ?? 50;
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new ApiError("AUDIT_QUERY_INVALID", "limit must be a positive integer", 400);
  }

  const rows = await readJsonl<AuditEvent>(auditPath);
  const filtered = params.linkId ? rows.filter((row) => row.linkId === params.linkId) : rows;
  return filtered.slice(0, limit);
}

export async function queryCandidateState(linkId: string): Promise<{ linkId: string; state: CandidateState }> {
  const normalized = normalizeNonEmpty(linkId, "linkId");
  const states = await readCandidateStates();
  return {
    linkId: normalized,
    state: states[normalized] ?? "candidate"
  };
}

export async function queryCandidateHistory(params: {
  linkId: string;
  limit?: number;
}): Promise<CandidateTransition[]> {
  const normalizedLinkId = normalizeNonEmpty(params.linkId, "linkId");
  const limit = params.limit ?? 50;
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new ApiError("AUDIT_QUERY_INVALID", "limit must be a positive integer", 400);
  }

  const rows = await readJsonl<CandidateTransition>(historyPath);
  return rows.filter((row) => row.linkId === normalizedLinkId).slice(0, limit);
}

function validatePermission(actorRole: string, actionType: ActionType): void {
  const role = normalizeNonEmpty(actorRole, "actorRole");
  if (role === "admin") {
    return;
  }

  const permissionMap: Record<string, ActionType[]> = {
    reviewer: ["confirmLink", "rejectLink"],
    operator: ["addEvidenceToLink"]
  };

  const allowed = permissionMap[role] ?? [];
  if (!allowed.includes(actionType)) {
    throw new ApiError(
      "POLICY_PERMISSION_DENIED",
      `role '${role}' is not allowed to execute '${actionType}'`,
      403
    );
  }
}

function validatePayload(actionType: ActionType, payload: ActionRequest["payload"]): void {
  if (actionType === "confirmLink") {
    const body = payload as ConfirmPayload;
    normalizeNonEmpty(body.linkId, "payload.linkId");
    if ((body.justification ?? "").trim().length < 10) {
      throw new ApiError(
        "ACTION_VALIDATION_FAILED",
        "Please provide at least 10 characters for justification.",
        400
      );
    }
    if (
      body.confidenceOverride !== undefined &&
      (typeof body.confidenceOverride !== "number" || body.confidenceOverride < 0 || body.confidenceOverride > 1)
    ) {
      throw new ApiError(
        "ACTION_VALIDATION_FAILED",
        "Confidence override must be between 0.0 and 1.0.",
        400
      );
    }
    return;
  }

  if (actionType === "rejectLink") {
    const body = payload as RejectPayload;
    normalizeNonEmpty(body.linkId, "payload.linkId");
    if ((body.reason ?? "").trim().length < 10) {
      throw new ApiError("ACTION_VALIDATION_FAILED", "Please provide at least 10 characters for reason.", 400);
    }
    return;
  }

  if (actionType === "addEvidenceToLink") {
    const body = payload as AddEvidencePayload;
    normalizeNonEmpty(body.linkId, "payload.linkId");
    normalizeNonEmpty(body.evidenceId, "payload.evidenceId");
    return;
  }

  throw new ApiError("ACTION_UNKNOWN", "Unsupported action type", 400);
}

function summarizeAction(actionType: ActionType, payload: ActionRequest["payload"]): string {
  if (actionType === "confirmLink") {
    return `Confirmed link ${getLinkId(payload)} with justification`;
  }
  if (actionType === "rejectLink") {
    return `Rejected link ${getLinkId(payload)} with reason`;
  }
  return `Attached evidence ${(payload as AddEvidencePayload).evidenceId} to link ${getLinkId(payload)}`;
}

function getLinkId(payload: ActionRequest["payload"]): string {
  if ("linkId" in payload) {
    return normalizeNonEmpty(payload.linkId, "payload.linkId");
  }
  throw new ApiError("ACTION_VALIDATION_FAILED", "payload.linkId is required", 400);
}

function nextState(current: CandidateState, actionType: ActionType): CandidateState {
  if (actionType === "confirmLink") {
    if (current === "candidate" || current === "in_review") {
      return "confirmed";
    }
    throw new ApiError(
      "ACTION_INVALID_LINK_STATE",
      `Cannot confirm from '${current}' state. Refresh candidate status and retry.`,
      409
    );
  }

  if (actionType === "rejectLink") {
    if (current === "candidate" || current === "in_review") {
      return "rejected";
    }
    throw new ApiError(
      "ACTION_INVALID_LINK_STATE",
      `Cannot reject from '${current}' state. Refresh candidate status and retry.`,
      409
    );
  }

  if (current === "candidate" || current === "in_review") {
    return "in_review";
  }
  throw new ApiError(
    "ACTION_INVALID_LINK_STATE",
    `Cannot attach evidence from '${current}' state. Refresh candidate status and retry.`,
    409
  );
}

function normalizeNonEmpty(input: unknown, field: string): string {
  if (typeof input !== "string" || input.trim().length === 0) {
    throw new ApiError("ACTION_VALIDATION_FAILED", `${field} must be a non-empty string`, 400);
  }
  return input.trim();
}

async function readCandidateStates(): Promise<Record<string, CandidateState>> {
  try {
    const raw = await fs.readFile(statePath, "utf8");
    const parsed = JSON.parse(raw) as Record<string, CandidateState>;
    return parsed;
  } catch {
    return {};
  }
}

async function writeJson(filePath: string, value: unknown): Promise<void> {
  await ensureDir();
  await fs.writeFile(filePath, JSON.stringify(value, null, 2), "utf8");
}

async function appendJsonl(filePath: string, value: unknown): Promise<void> {
  await ensureDir();
  await fs.appendFile(filePath, `${JSON.stringify(value)}\n`, "utf8");
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
      })
      .reverse();
  } catch {
    return [];
  }
}

async function ensureDir(): Promise<void> {
  await fs.mkdir(storeDir, { recursive: true });
}
