import { proxy } from "@/lib/backend";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function POST(request: Request) {
  return proxy(request);
}
