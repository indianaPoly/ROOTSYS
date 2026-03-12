import Link from "next/link";

import { getOpsDashboardData } from "@/lib/ops-kpis";

export const dynamic = "force-dynamic";

export default async function AnalysisOpsDashboardPage() {
  const dashboard = await getOpsDashboardData();

  return (
    <main id="content" className="page">
      <section className="hero">
        <p className="eyebrow">ROOTSYS Operations</p>
        <h1>Candidate Throughput and Lead-Time Dashboard</h1>
        <p className="lede">
          This dashboard tracks decision throughput and analysis lead time from action/audit artifacts.
        </p>
        <p className="paneHint">
          <Link className="inlineLink" href="/analysis">
            Back to analysis index
          </Link>
        </p>
      </section>

      <section className="opsKpiGrid" aria-label="Operations KPIs">
        <article className="opsKpiCard">
          <h2>Total Events</h2>
          <p>{dashboard.totals.totalEvents}</p>
        </article>
        <article className="opsKpiCard">
          <h2>Unique Candidates</h2>
          <p>{dashboard.totals.uniqueCandidates}</p>
        </article>
        <article className="opsKpiCard">
          <h2>Decision Volume</h2>
          <p>{dashboard.totals.decisions}</p>
        </article>
        <article className="opsKpiCard">
          <h2>Approval Rate</h2>
          <p>{(dashboard.totals.approvalRate * 100).toFixed(1)}%</p>
        </article>
        <article className="opsKpiCard">
          <h2>Reject Rate</h2>
          <p>{(dashboard.totals.rejectRate * 100).toFixed(1)}%</p>
        </article>
        <article className="opsKpiCard">
          <h2>Avg Lead Time</h2>
          <p>{dashboard.totals.avgLeadTimeMinutes.toFixed(1)}m</p>
        </article>
        <article className="opsKpiCard">
          <h2>Backlog Candidates</h2>
          <p>{dashboard.totals.backlogCandidates}</p>
        </article>
        <article className="opsKpiCard">
          <h2>Data Sources</h2>
          <p>{dashboard.sourceCount}</p>
        </article>
      </section>

      <section className="panel section" aria-label="Trend table">
        <h2 className="sectionTitle">Daily Throughput Trend</h2>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>Day</th>
                <th>Total Events</th>
                <th>Decisions</th>
                <th>Confirmations</th>
                <th>Rejections</th>
              </tr>
            </thead>
            <tbody>
              {dashboard.trends.map((point) => (
                <tr key={point.day}>
                  <td>{point.day}</td>
                  <td>{point.totalEvents}</td>
                  <td>{point.decisions}</td>
                  <td>{point.confirms}</td>
                  <td>{point.rejects}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel section" aria-label="Threshold and alerts">
        <h2 className="sectionTitle">Alert Thresholds</h2>
        <ul className="contextList">
          <li>
            Reject-rate warning threshold: <code>{(dashboard.thresholds.rejectRateWarn * 100).toFixed(1)}%</code>
          </li>
          <li>
            Lead-time warning threshold: <code>{dashboard.thresholds.leadTimeWarnMinutes} minutes</code>
          </li>
          <li>
            Backlog warning threshold: <code>{dashboard.thresholds.backlogWarnCount} candidates</code>
          </li>
        </ul>

        <h3 className="historyTitle">Active Alerts</h3>
        {dashboard.alerts.length === 0 ? (
          <p className="paneHint">No active alert from current artifacts.</p>
        ) : (
          <div className="historyList" role="list">
            {dashboard.alerts.map((alert) => (
              <div key={alert.code} className={`historyRow ${alert.severity === "critical" ? "opsAlertCritical" : "opsAlertWarn"}`} role="listitem">
                <strong>{alert.code}</strong>
                <span>{alert.message}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
