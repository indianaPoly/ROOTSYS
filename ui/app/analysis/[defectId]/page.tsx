import Link from "next/link";

import { AnalysisWorkspace } from "@/components/analysis-workspace";
import { getAnalysisWorkspace } from "@/lib/analysis-data";

export const dynamic = "force-dynamic";

type Params = {
  params: Promise<{
    defectId: string;
  }>;
};

export default async function AnalysisDefectPage({ params }: Params) {
  const { defectId } = await params;
  const data = await getAnalysisWorkspace(defectId);

  return (
    <>
      <div className="page" style={{ marginBottom: 0 }}>
        <p className="paneHint">
          <Link className="inlineLink" href="/analysis">
            Back to analysis index
          </Link>
        </p>
      </div>
      <AnalysisWorkspace data={data} />
    </>
  );
}
