import { NextResponse } from "next/server";

import { ApiError, executeAction } from "@/lib/action-api-store";

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      actorId?: string;
      actorRole?: string;
      actionType?: "confirmLink" | "rejectLink" | "addEvidenceToLink";
      payload?: Record<string, unknown>;
    };

    const result = await executeAction({
      actorId: body.actorId ?? "anonymous",
      actorRole: body.actorRole ?? "reviewer",
      actionType: body.actionType ?? "confirmLink",
      payload: (body.payload ?? {}) as never
    });

    return NextResponse.json(result, { status: 200 });
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
        message: "Unexpected action execution failure"
      },
      { status: 500 }
    );
  }
}
