// Bridge between the dashboard UI (localhost:3000) and the FastAPI pipeline
// controller (server.py on 127.0.0.1:8000). Route handlers under app/api/*
// forward every request (method, query string, JSON body) to the backend and
// stream the response back, so the browser keeps calling same-origin /api/*.
const BACKEND = process.env.PIPELINE_API_URL ?? "http://127.0.0.1:8000";

export async function proxy(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const backendUrl = `${BACKEND}${url.pathname}${url.search}`;

  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  const body = hasBody ? await request.text() : undefined;

  try {
    const res = await fetch(backendUrl, {
      method: request.method,
      headers: { "content-type": "application/json" },
      body,
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
