import Link from "next/link";

import { listDefectSummaries } from "@/lib/analysis-data";

export const dynamic = "force-dynamic";

export default async function AnalysisIndexPage() {
  const summaries = await listDefectSummaries();

  return (
    <main id="content" className="page">
      <section className="hero">
        <p className="eyebrow">ROOTSYS CAPA Analysis</p>
        <h1>Defect-Centric Review Workspace</h1>
        <p className="lede">Open one defect route to inspect causes, evidence, and action decisions on a single screen.</p>
      </section>

      <section className="panel section" aria-label="Defect routes">
        <h2 className="sectionTitle">Available Defect Contexts</h2>
        <div className="defectGrid">
          {summaries.map((summary) => (
            <article key={summary.defectId} className="defectCard">
              <h3>{summary.defectId}</h3>
              <p>Source: {summary.source}</p>
              <p>Candidates: {summary.candidateCount}</p>
              <p>Evidence: {summary.evidenceCount}</p>
              <Link className="inlineLink" href={`/analysis/${encodeURIComponent(summary.defectId)}`}>
                Open analysis route
              </Link>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
