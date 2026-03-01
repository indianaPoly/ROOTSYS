import { getDashboardData } from "@/lib/artifacts";

const runCommands = [
  "bash scripts/run_service_smoke_tests.sh",
  "bash scripts/run_complex_pipeline_checks.sh"
];

export default async function HomePage() {
  const data = await getDashboardData();

  return (
    <main className="page">
      <section className="hero">
        <p className="eyebrow">ROOTSYS Runtime Console</p>
        <h1>Validated Execution Artifacts</h1>
        <p className="lede">
          This dashboard reads actual JSONL outputs produced by smoke and complex pipeline checks.
          It is intended to confirm real run status before wiring frontend-driven workflows.
        </p>
        <div className="commandList" role="list" aria-label="Run commands">
          {runCommands.map((command) => (
            <code key={command} role="listitem">
              {command}
            </code>
          ))}
        </div>
      </section>

      <section className="statsGrid" aria-label="Summary metrics">
        <MetricCard label="Ready Artifacts" value={String(data.totals.readyCount)} tone="ready" />
        <MetricCard label="Missing Artifacts" value={String(data.totals.missingCount)} tone="missing" />
        <MetricCard label="Invalid Artifacts" value={String(data.totals.invalidCount)} tone="invalid" />
        <MetricCard label="Total Records" value={String(data.totals.totalRecords)} tone="neutral" />
      </section>

      <section className="tableWrap" aria-label="Artifact table">
        <table>
          <thead>
            <tr>
              <th>Artifact</th>
              <th>Status</th>
              <th>Records</th>
              <th>Unique IDs</th>
              <th>Sources</th>
              <th>Sample IDs</th>
            </tr>
          </thead>
          <tbody>
            {data.artifacts.map((artifact) => (
              <tr key={artifact.filePath}>
                <td>
                  <strong>{artifact.name}</strong>
                  <span className="path">{artifact.filePath}</span>
                </td>
                <td>
                  <StatusPill state={artifact.state} />
                </td>
                <td>{artifact.totalRecords}</td>
                <td>{artifact.uniqueRecordIds}</td>
                <td className="monoCell">{artifact.sources.join(", ") || "-"}</td>
                <td className="monoCell">{artifact.sampleRecordIds.join(", ") || artifact.error || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <footer className="footer">Generated at {new Date(data.generatedAt).toLocaleString()}</footer>
    </main>
  );
}

function MetricCard({ label, value, tone }: { label: string; value: string; tone: "ready" | "missing" | "invalid" | "neutral" }) {
  return (
    <article className={`metric metric-${tone}`}>
      <p>{label}</p>
      <h2>{value}</h2>
    </article>
  );
}

function StatusPill({ state }: { state: "ready" | "missing" | "invalid" }) {
  return <span className={`pill pill-${state}`}>{state.toUpperCase()}</span>;
}
