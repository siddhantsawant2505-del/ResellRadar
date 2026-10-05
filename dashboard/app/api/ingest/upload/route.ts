import { BACKEND } from "@/lib/backend";

export const dynamic = "force-dynamic";
export const revalidate = 0;

// Multipart bodies cannot go through the generic JSON proxy (re-encoding the
// FormData as text would drop the boundary) — forward the raw bytes as-is.
export async function POST(request: Request) {
  try {
    const res = await fetch(`${BACKEND}/api/ingest/upload`, {
      method: "POST",
      headers: {
        "content-type": request.headers.get("content-type") ?? "multipart/form-data",
      },
      body: await request.arrayBuffer(),
      cache: "no-store",
    });
    const text = await res.text();
    return new Response(text, {
      status: res.status,
      headers: { "content-type": res.headers.get("content-type") ?? "application/json" },
    });
  } catch (err) {
    return Response.json(
      {
        error: "pipeline backend unreachable",
        detail: err instanceof Error ? err.message : String(err),
        backend: BACKEND,
      },
      { status: 502 },
    );
  }
}
