"use client";

import { useEffect, useMemo, useState } from "react";

import type { AnalysisCause, AnalysisWorkspaceData, CandidateStatus } from "@/lib/analysis-data";

type Props = {
  data: AnalysisWorkspaceData;
};

type ActionType = "confirmLink" | "rejectLink" | "addEvidenceToLink";
type RowUiState = "idle" | "submitting" | "success" | "error";

type RowFeedback = {
  state: RowUiState;
  errorCode?: string;
  message?: string;
  policyDecision?: "allow" | "deny";
};

type HistoryRow = {
  linkId: string;
  actionType: ActionType;
  fromState: CandidateStatus;
  toState: CandidateStatus;
  actorRole: string;
  transitionedAtUnixMs: number;
};

type ActionResponse = {
  actionId: string;
  actionType: ActionType;
  linkId: string;
  previousState: CandidateStatus;
  currentState: CandidateStatus;
  auditEventId: number;
  policyDecision: "allow";
};

const statusOptions: CandidateStatus[] = ["candidate", "in_review", "confirmed", "rejected"];
const evidenceTypeOptions = ["all", "log", "report", "note", "image", "unknown"] as const;
const timeWindowOptions = ["all", "1h", "24h", "7d"] as const;
const rejectReasonCodeOptions = [
  "TIME_WINDOW_MISMATCH",
  "EQUIPMENT_CONTEXT_MISMATCH",
  "INSUFFICIENT_EVIDENCE",
  "DUPLICATE_CANDIDATE",
  "OTHER"
] as const;

export function AnalysisWorkspace({ data }: Props) {
  const [causes, setCauses] = useState<AnalysisCause[]>(data.causes);
  const [selectedCauseId, setSelectedCauseId] = useState<string>(data.causes[0]?.id ?? "");
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<CandidateStatus | "all">("all");
  const [confidenceFilter, setConfidenceFilter] = useState<number>(0);
  const [evidenceTypeFilter, setEvidenceTypeFilter] = useState<(typeof evidenceTypeOptions)[number]>("all");
  const [timeWindowFilter, setTimeWindowFilter] = useState<(typeof timeWindowOptions)[number]>("all");
  const [rowFeedback, setRowFeedback] = useState<Record<string, RowFeedback>>({});
  const [actionMode, setActionMode] = useState<ActionType>("confirmLink");
  const [actorRole, setActorRole] = useState<"reviewer" | "operator" | "admin">("reviewer");

  const [confirmJustification, setConfirmJustification] = useState("");
  const [confidenceOverride, setConfidenceOverride] = useState("");
  const [rejectReason, setRejectReason] = useState("");
  const [rejectReasonCode, setRejectReasonCode] = useState<(typeof rejectReasonCodeOptions)[number]>(
    "TIME_WINDOW_MISMATCH"
  );
  const [evidenceId, setEvidenceId] = useState("");
  const [evidenceDescription, setEvidenceDescription] = useState("");
  const [formErrorMessage, setFormErrorMessage] = useState<string>("");

  const [historyRows, setHistoryRows] = useState<HistoryRow[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  const now = Date.now();
  const filteredCauses = useMemo(() => {
    return causes.filter((cause) => {
      if (statusFilter !== "all" && cause.status !== statusFilter) {
        return false;
      }
      if (cause.confidence < confidenceFilter) {
        return false;
      }

      if (timeWindowFilter !== "all") {
        const windowMs =
          timeWindowFilter === "1h" ? 3_600_000 : timeWindowFilter === "24h" ? 86_400_000 : 604_800_000;
        if (cause.updatedAtUnixMs < now - windowMs) {
          return false;
        }
      }

      if (search.trim().length > 0) {
        const needle = search.trim().toLowerCase();
        const haystack = `${cause.title} ${cause.description} ${cause.id}`.toLowerCase();
        if (!haystack.includes(needle)) {
          return false;
        }
      }

      if (evidenceTypeFilter !== "all") {
        const evidenceForCause = data.evidence.filter((item) => cause.evidenceIds.includes(item.id));
        if (!evidenceForCause.some((item) => item.evidenceType === evidenceTypeFilter)) {
          return false;
        }
      }

      return true;
    });
  }, [
    causes,
    confidenceFilter,
    data.evidence,
    evidenceTypeFilter,
    now,
    search,
    statusFilter,
    timeWindowFilter
  ]);

  const selectedCause =
    filteredCauses.find((cause) => cause.id === selectedCauseId) ?? filteredCauses[0] ?? causes[0] ?? null;
  const highlightedEvidence = selectedCause
    ? data.evidence.filter((item) => selectedCause.evidenceIds.includes(item.id))
    : [];

  const selectedCauseKey = selectedCause?.id ?? "";

  useEffect(() => {
    if (!selectedCauseKey) {
      setHistoryRows([]);
      return;
    }

    const controller = new AbortController();
    const loadHistory = async () => {
      setHistoryLoading(true);
      try {
        const response = await fetch(
          `/api/candidates/${encodeURIComponent(selectedCauseKey)}/history?limit=8`,
          {
            signal: controller.signal
          }
        );
        const body = (await response.json()) as { rows?: HistoryRow[] };
        setHistoryRows(body.rows ?? []);
      } catch {
        if (!controller.signal.aborted) {
          setHistoryRows([]);
        }
      } finally {
        if (!controller.signal.aborted) {
          setHistoryLoading(false);
        }
      }
    };

    void loadHistory();
    return () => controller.abort();
  }, [selectedCauseKey]);

  async function submitAction(): Promise<void> {
    if (!selectedCause) {
      setFormErrorMessage("Select a candidate before executing an action.");
      return;
    }

    let payload: Record<string, unknown> | null = null;
    if (actionMode === "confirmLink") {
      if (confirmJustification.trim().length < 10) {
        setFormErrorMessage("Please provide at least 10 characters for justification.");
        return;
      }

      if (confidenceOverride.trim().length > 0) {
        const parsed = Number(confidenceOverride);
        if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
          setFormErrorMessage("Confidence override must be between 0.0 and 1.0.");
          return;
        }
        payload = {
          linkId: selectedCause.id,
          justification: confirmJustification.trim(),
          confidenceOverride: parsed
        };
      } else {
        payload = {
          linkId: selectedCause.id,
          justification: confirmJustification.trim()
        };
      }
    }

    if (actionMode === "rejectLink") {
      if (rejectReason.trim().length < 10) {
        setFormErrorMessage("Please provide at least 10 characters for reason.");
        return;
      }

      payload = {
        linkId: selectedCause.id,
        reason: rejectReason.trim(),
        reasonCode: rejectReasonCode
      };
    }

    if (actionMode === "addEvidenceToLink") {
      if (evidenceId.trim().length === 0) {
        setFormErrorMessage("Please select an evidence item.");
        return;
      }
      payload = {
        linkId: selectedCause.id,
        evidenceId: evidenceId.trim(),
        description: evidenceDescription.trim().length > 0 ? evidenceDescription.trim() : undefined
      };
    }

    if (!payload) {
      setFormErrorMessage("Unsupported action type.");
      return;
    }

    setFormErrorMessage("");
    setRowFeedback((prev) => ({
      ...prev,
      [selectedCause.id]: {
        state: "submitting"
      }
    }));

    const requestBody: {
      actorId: string;
      actorRole: string;
      scope: {
        linkIds: string[];
      };
      actionType: ActionType;
      payload: Record<string, unknown>;
    } = {
      actorId: `ui-${actorRole}-1`,
      actorRole,
      scope: {
        linkIds: [selectedCause.id]
      },
      actionType: actionMode,
      payload
    };

    const authToken = process.env.NEXT_PUBLIC_ROOTSYS_ACTION_API_TOKEN;
    const headers: Record<string, string> = {
      "content-type": "application/json"
    };
    if (authToken) {
      headers["x-rootsys-auth-token"] = authToken;
    }

    try {
      const response = await fetch("/api/actions", {
        method: "POST",
        headers,
        body: JSON.stringify(requestBody)
      });
      const responseBody = (await response.json()) as
        | ActionResponse
        | {
            errorCode?: string;
            message?: string;
          };

      if (!response.ok) {
        const errorCode =
          "errorCode" in responseBody && typeof responseBody.errorCode === "string"
            ? responseBody.errorCode
            : "ACTION_UNKNOWN";
        const message =
          "message" in responseBody && typeof responseBody.message === "string"
            ? responseBody.message
            : "Action failed";
        setRowFeedback((prev) => ({
          ...prev,
          [selectedCause.id]: {
            state: "error",
            errorCode,
            message,
            policyDecision: errorCode?.startsWith("POLICY_") ? "deny" : undefined
          }
        }));
        return;
      }

      const successBody = responseBody as ActionResponse;
      setCauses((prev) =>
        prev.map((cause) =>
          cause.id === successBody.linkId
            ? {
                ...cause,
                status: successBody.currentState,
                updatedAtUnixMs: Date.now()
              }
            : cause
        )
      );
      setRowFeedback((prev) => ({
        ...prev,
        [selectedCause.id]: {
          state: "success",
          message: `action_id=${successBody.actionId}, audit_event=${successBody.auditEventId}`,
          policyDecision: successBody.policyDecision
        }
      }));

      const historyResponse = await fetch(
        `/api/candidates/${encodeURIComponent(selectedCause.id)}/history?limit=8`
      );
      const historyBody = (await historyResponse.json()) as { rows?: HistoryRow[] };
      setHistoryRows(historyBody.rows ?? []);
    } catch {
      setRowFeedback((prev) => ({
        ...prev,
        [selectedCause.id]: {
          state: "error",
          errorCode: "ACTION_UNKNOWN",
          message: "Network or server error while executing action"
        }
      }));
    }
  }

  const selectedFeedback = selectedCause ? rowFeedback[selectedCause.id] : undefined;
  const isSubmitting = selectedFeedback?.state === "submitting";

  return (
    <main id="content" className="page analysisPage">
      <section className="hero analysisHero" aria-label="Defect context">
        <p className="eyebrow">ROOTSYS CAPA Analysis</p>
        <h1>{data.defectId}</h1>
        <p className="lede">Single-screen analysis workspace synchronized by selected defect context.</p>
        <div className="analysisMetaRow" role="list" aria-label="Defect metadata">
          <span role="listitem">Source: {data.source}</span>
          <span role="listitem">Severity: {data.severity}</span>
          <span role="listitem">Line: {data.line}</span>
          <span role="listitem">Lot: {data.lotId}</span>
        </div>
      </section>

      <section className="panel section" aria-label="Analysis filters">
        <div className="analysisFilters">
          <label>
            Search
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="defect id, cause title, evidence keyword"
            />
          </label>
          <label>
            Cause status
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value as CandidateStatus | "all")}> 
              <option value="all">all</option>
              {statusOptions.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </select>
          </label>
          <label>
            Confidence &gt;=
            <input
              type="number"
              min={0}
              max={1}
              step={0.05}
              value={confidenceFilter}
              onChange={(event) => setConfidenceFilter(Number(event.target.value) || 0)}
            />
          </label>
          <label>
            Evidence type
            <select
              value={evidenceTypeFilter}
              onChange={(event) =>
                setEvidenceTypeFilter(event.target.value as (typeof evidenceTypeOptions)[number])
              }
            >
              {evidenceTypeOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <label>
            Time window
            <select
              value={timeWindowFilter}
              onChange={(event) => setTimeWindowFilter(event.target.value as (typeof timeWindowOptions)[number])}
            >
              {timeWindowOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <section className="analysisGrid" aria-label="Single-screen analysis panes">
        <article className="analysisPane" aria-label="Cause graph pane">
          <h2>Cause Graph Pane</h2>
          <p className="paneHint">Candidate, confirmed, and rejected causes are shown with confidence and rule origin.</p>
          <div className="causeList" role="list">
            {filteredCauses.map((cause) => {
              const selected = selectedCause?.id === cause.id;
              const feedback = rowFeedback[cause.id];
              return (
                <div key={cause.id} role="listitem" className={`causeCard ${selected ? "causeCardSelected" : ""}`}>
                  <button type="button" className="causeSelectButton" onClick={() => setSelectedCauseId(cause.id)}>
                    <strong>{cause.title}</strong>
                    <span className="causeMeta">{cause.status}</span>
                    <span className="causeMeta">confidence {cause.confidence.toFixed(2)}</span>
                    <span className="causeMeta monoCell">{cause.rule}</span>
                  </button>

                  <div className="rowActionButtons">
                    <button
                      type="button"
                      disabled={feedback?.state === "submitting"}
                      onClick={() => {
                        setSelectedCauseId(cause.id);
                        setActionMode("confirmLink");
                      }}
                    >
                      Confirm
                    </button>
                    <button
                      type="button"
                      disabled={feedback?.state === "submitting"}
                      onClick={() => {
                        setSelectedCauseId(cause.id);
                        setActionMode("rejectLink");
                      }}
                    >
                      Reject
                    </button>
                    <button
                      type="button"
                      disabled={feedback?.state === "submitting"}
                      onClick={() => {
                        setSelectedCauseId(cause.id);
                        setActionMode("addEvidenceToLink");
                      }}
                    >
                      Add Evidence
                    </button>
                  </div>
                  {feedback?.state === "submitting" ? <p className="paneHint">Submitting...</p> : null}
                  {feedback?.state === "error" ? (
                    <div className="errorPanel" role="status" aria-live="polite">
                      <strong>{feedback.errorCode ?? "ACTION_UNKNOWN"}</strong>
                      <span>{feedback.message ?? "Action failed"}</span>
                    </div>
                  ) : null}
                  {feedback?.state === "success" ? (
                    <div className="successPanel" role="status" aria-live="polite">
                      <strong>allow</strong>
                      <span>{feedback.message}</span>
                    </div>
                  ) : null}
                </div>
              );
            })}
            {filteredCauses.length === 0 ? <p className="paneHint">No candidates matched the selected filters.</p> : null}
          </div>
        </article>

        <article className="analysisPane" aria-label="Evidence pane">
          <h2>Evidence Pane</h2>
          <p className="paneHint">Selecting a cause highlights supporting evidence and preserves defect context.</p>
          <div className="evidenceList" role="list">
            {highlightedEvidence.map((item) => (
              <div key={item.id} role="listitem" className="evidenceCard">
                <strong>{item.title}</strong>
                <span className="causeMeta">{item.evidenceType}</span>
                <span className="causeMeta monoCell">{item.source}</span>
              </div>
            ))}
            {highlightedEvidence.length === 0 ? (
              <p className="paneHint">No evidence is linked to the selected cause yet.</p>
            ) : null}
          </div>
        </article>

        <article className="analysisPane" aria-label="Action pane">
          <h2>Action Pane</h2>
          <p className="paneHint">Primary actions are scoped to the selected cause candidate.</p>
          <div className="actionCard">
            <p>
              Selected candidate: <code>{selectedCause?.id ?? "none"}</code>
            </p>
            <p>Status: {selectedCause?.status ?? "-"}</p>

            <label className="actionField">
              Actor role
              <select value={actorRole} onChange={(event) => setActorRole(event.target.value as "reviewer" | "operator" | "admin")}> 
                <option value="reviewer">reviewer</option>
                <option value="operator">operator</option>
                <option value="admin">admin</option>
              </select>
            </label>

            <label className="actionField">
              Action type
              <select value={actionMode} onChange={(event) => setActionMode(event.target.value as ActionType)}>
                <option value="confirmLink">confirmLink</option>
                <option value="rejectLink">rejectLink</option>
                <option value="addEvidenceToLink">addEvidenceToLink</option>
              </select>
            </label>

            {actionMode === "confirmLink" ? (
              <>
                <label className="actionField">
                  Justification (min 10 chars)
                  <textarea
                    value={confirmJustification}
                    onChange={(event) => setConfirmJustification(event.target.value)}
                    placeholder="Explain why this link should be confirmed"
                  />
                </label>
                <label className="actionField">
                  Confidence override (optional 0.0..1.0)
                  <input
                    type="number"
                    min={0}
                    max={1}
                    step={0.01}
                    value={confidenceOverride}
                    onChange={(event) => setConfidenceOverride(event.target.value)}
                  />
                </label>
              </>
            ) : null}

            {actionMode === "rejectLink" ? (
              <>
                <label className="actionField">
                  Reason (min 10 chars)
                  <textarea
                    value={rejectReason}
                    onChange={(event) => setRejectReason(event.target.value)}
                    placeholder="Explain why this candidate should be rejected"
                  />
                </label>
                <label className="actionField">
                  Reason code
                  <select
                    value={rejectReasonCode}
                    onChange={(event) =>
                      setRejectReasonCode(event.target.value as (typeof rejectReasonCodeOptions)[number])
                    }
                  >
                    {rejectReasonCodeOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </label>
              </>
            ) : null}

            {actionMode === "addEvidenceToLink" ? (
              <>
                <label className="actionField">
                  Evidence ID
                  <input
                    value={evidenceId}
                    onChange={(event) => setEvidenceId(event.target.value)}
                    placeholder="Select or enter evidence id"
                  />
                </label>
                <label className="actionField">
                  Description (optional)
                  <textarea
                    value={evidenceDescription}
                    onChange={(event) => setEvidenceDescription(event.target.value)}
                    placeholder="Optional note for evidence attachment"
                  />
                </label>
              </>
            ) : null}

            {formErrorMessage ? (
              <div className="errorPanel" role="status" aria-live="polite">
                <strong>ACTION_VALIDATION_FAILED</strong>
                <span>{formErrorMessage}</span>
              </div>
            ) : null}

            {selectedFeedback?.state === "error" ? (
              <div className="errorPanel" role="status" aria-live="polite">
                <strong>{selectedFeedback.errorCode ?? "ACTION_UNKNOWN"}</strong>
                <span>{selectedFeedback.message ?? "Action failed"}</span>
              </div>
            ) : null}

            {selectedFeedback?.state === "success" ? (
              <div className="successPanel" role="status" aria-live="polite">
                <strong>{selectedFeedback.policyDecision ?? "allow"}</strong>
                <span>{selectedFeedback.message}</span>
              </div>
            ) : null}

            <div className="actionButtons">
              <button type="button" disabled={isSubmitting || !selectedCause} onClick={() => void submitAction()}>
                {isSubmitting ? "Submitting..." : "Submit action"}
              </button>
            </div>

            <p className="paneHint">Errors surface machine-readable codes (policy_code/error_code) from action API responses.</p>
          </div>
        </article>

        <article className="analysisPane" aria-label="Defect context pane">
          <h2>Defect Context Pane</h2>
          <p className="paneHint">Timeline and context remain anchored to one defect route.</p>
          <ul className="contextList">
            <li>
              Defect ID: <code>{data.defectId}</code>
            </li>
            <li>
              Updated: <code>{new Date(data.updatedAtUnixMs).toLocaleString()}</code>
            </li>
            <li>
              Candidate count: <code>{filteredCauses.length}</code>
            </li>
            <li>
              Evidence count: <code>{highlightedEvidence.length}</code>
            </li>
          </ul>

          <h3 className="historyTitle">Candidate Transition History</h3>
          {historyLoading ? <p className="paneHint">Loading history...</p> : null}
          {!historyLoading && historyRows.length === 0 ? (
            <p className="paneHint">No state transitions recorded for the selected candidate.</p>
          ) : null}
          <div className="historyList" role="list">
            {historyRows.map((row, index) => (
              <div key={`${row.linkId}-${row.transitionedAtUnixMs}-${index}`} className="historyRow" role="listitem">
                <strong>{row.actionType}</strong>
                <span>
                  {row.fromState} -&gt; {row.toState}
                </span>
                <span className="monoCell">{row.actorRole}</span>
                <span>{new Date(row.transitionedAtUnixMs).toLocaleString()}</span>
              </div>
            ))}
          </div>
        </article>
      </section>
    </main>
  );
}
