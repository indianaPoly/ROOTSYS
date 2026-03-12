import { NextResponse } from "next/server";

import { ApiError, queryAudit } from "@/lib/action-api-store";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const linkId = searchParams.get("linkId") ?? undefined;
    const limitRaw = searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;

    const rows = await queryAudit({ linkId, limit });
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
        message: "Unexpected audit query failure"
      },
      { status: 500 }
    );
  }
}
