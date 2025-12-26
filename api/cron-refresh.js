// /api/cron-refresh.js
export default async function handler(req, res) {
  // Simple protection (optional)
  const secret = process.env.CRON_SECRET;
  const provided = req.headers["x-cron-secret"];
  if (secret && provided !== secret) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const base = "https://insiderscope-backend.vercel.app/api/insider-buys";
  const url = `${base}?wrap=1&days=30&pageSize=50&page=1`;

  try {
    const r = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
    const j = await r.json().catch(() => null);
    return res.status(200).json({
      ok: r.ok,
      status: r.status,
      warmed: true,
      sample: j?.meta ?? null,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
}
