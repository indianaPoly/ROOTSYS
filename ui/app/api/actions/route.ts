import { NextResponse } from "next/server";

import { ApiError, appendDeniedAuditEvent, executeAction } from "@/lib/action-api-store";

export async function POST(request: Request) {
  try {
    const expectedToken = process.env.ROOTSYS_ACTION_API_TOKEN;
    if (expectedToken) {
      const receivedToken = request.headers.get("x-rootsys-auth-token");
      if (receivedToken !== expectedToken) {
        await appendDeniedAuditEvent({
          actorId: "unknown",
          actorRole: "unknown",
          actionType: "confirmLink",
          linkId: "unknown",
          errorCode: "AUTH_TOKEN_INVALID",
          message: "Invalid or missing x-rootsys-auth-token"
        });
        throw new ApiError("AUTH_TOKEN_INVALID", "Invalid or missing x-rootsys-auth-token", 401);
      }
    }

    const body = (await request.json()) as {
      actorId?: string;
      actorRole?: string;
      scope?: {
        linkIds?: string[];
      };
      actionType?: "confirmLink" | "rejectLink" | "addEvidenceToLink";
      payload?: Record<string, unknown>;
    };

    const result = await executeAction({
      actorId: body.actorId ?? "anonymous",
      actorRole: body.actorRole ?? "reviewer",
      scope: {
        linkIds: body.scope?.linkIds ?? []
      },
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
