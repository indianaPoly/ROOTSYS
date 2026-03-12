import { NextResponse } from "next/server";

import { ApiError, queryCandidateHistory } from "@/lib/action-api-store";

type Params = {
  params: Promise<{
    linkId: string;
  }>;
};

export async function GET(request: Request, context: Params) {
  try {
    const { linkId } = await context.params;
    const { searchParams } = new URL(request.url);
    const limitRaw = searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    const rows = await queryCandidateHistory({ linkId, limit });
    return NextResponse.json({ rows }, { status: 200 });
  } catch (error) {
    if (error instanceof ApiError) {
      return NextResponse.json(
        {
          errorCode: error.code,
          message: error.message
        },
        { status: error.status }
      );
    }

    return NextResponse.json(
      {
        errorCode: "ACTION_UNKNOWN",
        message: "Unexpected candidate history query failure"
      },
      { status: 500 }
    );
  }
}
