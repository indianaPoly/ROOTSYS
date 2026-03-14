import Link from "next/link";

import { Reveal } from "@/components/reveal";
import { getDashboardData } from "@/lib/artifacts";

export const dynamic = "force-dynamic";

const runCommands = [
  "bash scripts/run_service_smoke_tests.sh",
  "bash scripts/run_complex_pipeline_checks.sh",
  "bash scripts/run_all_checks_and_prepare_ui.sh"
];

export default async function LandingPage() {
  const data = await getDashboardData();

  return (
    <main id="content" className="page landing">
      <Reveal>
        <section className="hero landingHero">
          <div className="ambientGlow" aria-hidden="true" />
          <div className="heroTop">
            <p className="eyebrow">ROOTSYS Data Integration</p>
            <div className="heroCtas" aria-label="Primary actions">
              <Link className="button buttonPrimary" href="/console">
                Open Runtime Console
              </Link>
              <Link className="button buttonGhost" href="#getting-started">
                Quickstart
              </Link>
            </div>
          </div>
          <h1>Normalize unknown external schemas into a stable record stream.</h1>
          <p className="lede">
            ROOTSYS turns inconsistent external payloads into a predictable integration stream. It validates minimal contract rules and emits
            inspectable JSONL artifacts from the same smoke and complex checks already tracked in this repository.
          </p>
          <div className="tagRow" role="list" aria-label="Capabilities">
            <span className="tag" role="listitem">
              External interface JSON
            </span>
            <span className="tag" role="listitem">
              IntegrationRecord + DeadLetter
            </span>
            <span className="tag" role="listitem">
              Merge + optional dedupe
            </span>
            <span className="tag" role="listitem">
              DLQ replay flow
            </span>
          </div>
        </section>
      </Reveal>

      <Reveal delay={0.06}>
        <section className="statsGrid" aria-label="Local artifact snapshot">
          <MetricCard label="Ready Artifacts" value={String(data.totals.readyCount)} tone="ready" />
          <MetricCard label="Missing Artifacts" value={String(data.totals.missingCount)} tone="missing" />
          <MetricCard label="Invalid Artifacts" value={String(data.totals.invalidCount)} tone="invalid" />
          <MetricCard label="Total Records" value={String(data.totals.totalRecords)} tone="neutral" />
        </section>
      </Reveal>

      <Reveal delay={0.09}>
        <section className="panel section" aria-label="How ROOTSYS works">
          <h2 className="sectionTitle">How it works (as exercised by the repo tests)</h2>
          <div className="stepGrid">
            <article className="step">
              <p className="stepKicker">1</p>
              <h3>Define an external interface</h3>
              <p>
                Declare what the source is (REST / DB / file / stream MVP), how to parse it, and the minimal contract to accept records.
              </p>
            </article>
            <article className="step">
              <p className="stepKicker">2</p>
              <h3>Run the pipeline</h3>
              <p>
                Accepted payloads become <code>IntegrationRecord</code> lines; rejected payloads become <code>DeadLetter</code> lines with structured
                reason codes.
              </p>
            </article>
            <article className="step">
              <p className="stepKicker">3</p>
              <h3>Merge and replay when needed</h3>
              <p>
                Combine multiple outputs into a single dataset (optionally deduped). Replay DLQ rows with a more permissive interface to recover
                records.
              </p>
            </article>
          </div>
        </section>
      </Reveal>

      <Reveal delay={0.12}>
        <section className="panel section" aria-label="Test-backed capabilities">
          <h2 className="sectionTitle">What the current tests cover</h2>
          <div className="featureGrid">
            <article className="featureCard">
              <h3>Drivers + fixtures</h3>
              <p>DB interfaces (SQLite fixtures), REST smoke flows, file JSONL runs, and a stream.kafka MVP fixture-backed input.</p>
            </article>
            <article className="featureCard">
              <h3>Interface schema validation</h3>
              <p>Valid interface fixtures satisfy the external interface JSON schema, and negative fixtures fail as expected.</p>
            </article>
            <article className="featureCard">
              <h3>DLQ + replay flow</h3>
              <p>Strict record ID rules emit DLQ rows; replay with hash fallback can recover records into a new output.</p>
            </article>
            <article className="featureCard">
              <h3>Merge with optional dedupe</h3>
              <p>Merge multiple integration outputs into one dataset and dedupe by stable identity fields when enabled.</p>
            </article>
            <article className="featureCard">
              <h3>Ontology materialization</h3>
              <p>Materialize <code>IntegrationRecord</code> inputs into typed ontology JSON outputs that match the expected fixture.</p>
            </article>
            <article className="featureCard">
              <h3>Console reads real artifacts</h3>
              <p>
                The UI reads JSONL outputs under <code>/tmp/rootsys-smoke</code> and <code>/tmp/rootsys-complex</code> and surfaces readiness and samples.
              </p>
            </article>
          </div>
        </section>
      </Reveal>

      <Reveal delay={0.15}>
        <section id="getting-started" className="panel section" aria-label="Local quickstart">
          <div className="sectionTop">
            <h2 className="sectionTitle">Local quickstart</h2>
            <Link className="inlineLink" href="/console">
              Go to console
            </Link>
          </div>
          <p className="sectionText">
            These commands produce the JSONL artifacts the console visualizes. The page intentionally reflects what the repo currently validates
            in smoke and complex checks.
          </p>
          <div className="commandList" role="list" aria-label="Run commands">
            {runCommands.map((command) => (
              <code key={command} role="listitem">
                {command}
              </code>
            ))}
          </div>
          <div className="hintRow" aria-label="Artifact locations">
            <p>
              Artifact paths: <code>/tmp/rootsys-smoke</code>, <code>/tmp/rootsys-complex</code>
            </p>
          </div>
        </section>
      </Reveal>

      <footer className="footer">
        Snapshot generated at {new Date(data.generatedAt).toLocaleString()} |{" "}
        <Link className="inlineLink" href="/console">
          Runtime Console
        </Link>
      </footer>
    </main>
  );
}

function MetricCard({
  label,
  value,
  tone
}: {
  label: string;
  value: string;
  tone: "ready" | "missing" | "invalid" | "neutral";
}) {
  return (
    <article className={`metric metric-${tone}`}>
      <p>{label}</p>
      <h2>{value}</h2>
    </article>
  );
}
