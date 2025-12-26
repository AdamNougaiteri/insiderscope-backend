// api/insider-buys.js
import { fetchRecentInsiderBuys } from "../lib/secDataServices.js";

export default async function handler(req, res) {
  try {
    const limit = Number(req.query.limit ?? 25);
    const minValue = Number(req.query.minValue ?? 0);
    const affiliation = String(req.query.affiliation ?? "any"); // any | Officer | Director | 10% Owner | Other

    const debug = String(req.query.debug ?? "0") === "1";
    const nocache = String(req.query.nocache ?? "0") === "1";

    const result = await fetchRecentInsiderBuys({
      limit: Number.isFinite(limit) ? Math.max(1, Math.min(100, limit)) : 25,
      minValue: Number.isFinite(minValue) ? Math.max(0, minValue) : 0,
      affiliation,
      debug,
      nocache,
    });

    return res.status(200).json(result);
  } catch (err) {
    return res.status(500).json({
      error: "INTERNAL_SERVER_ERROR",
      message: err?.message ?? String(err),
    });
  }
}
