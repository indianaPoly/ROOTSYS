import { NextResponse } from "next/server";

import { ApiError, queryCandidateState } from "@/lib/action-api-store";

type Params = {
  params: Promise<{
    linkId: string;
  }>;
};

export async function GET(_request: Request, context: Params) {
  try {
    const { linkId } = await context.params;
    const row = await queryCandidateState(linkId);
    return NextResponse.json(row, { status: 200 });
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
        message: "Unexpected candidate state query failure"
      },
      { status: 500 }
    );
  }
}
