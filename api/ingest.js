import { Pool } from "pg";

const pool =
  globalThis.__INSIDERSCOPE_POOL__ ||
  new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 1,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 10_000,
  });

globalThis.__INSIDERSCOPE_POOL__ = pool;

function nowIso() {
  return new Date().toISOString();
}
function num(v, def) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}
function bool(v, def = false) {
  if (v === undefined || v === null) return def;
  const s = String(v).toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "on";
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function getBaseUrl(req) {
  const proto = (req.headers["x-forwarded-proto"] || "https").toString();
  const host = (req.headers["x-forwarded-host"] || req.headers.host).toString();
  return `${proto}://${host}`;
}

async function getState(client, id) {
  const r = await client.query(`SELECT value FROM ingestion_state WHERE id=$1`, [id]);
  return r.rows?.[0]?.value ?? null;
}

async function setState(client, id, value) {
  await client.query(
    `
    INSERT INTO ingestion_state(id, value, updated_at)
    VALUES($1, $2::jsonb, now())
    ON CONFLICT (id) DO UPDATE
      SET value = EXCLUDED.value,
          updated_at = now()
    `,
    [id, JSON.stringify(value)]
  );
}

async function statusResponse(client) {
  const tables = await client.query(
    `SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;`
  );

  const counts = {};
  for (const t of tables.rows.map((r) => r.table_name)) {
    try {
      const c = await client.query(`SELECT COUNT(*)::int AS n FROM ${t};`);
      counts[t] = c.rows?.[0]?.n ?? 0;
    } catch {
      counts[t] = null;
    }
  }

  return {
    ok: true,
    tables: tables.rows.map((r) => r.table_name),
    counts,
    cursor: await getState(client, "insider_buys_cursor"),
    lastRun: await getState(client, "ingest_last_run"),
    ts: nowIso(),
  };
}

async function fetchInsiderBuysFromSelf(
  req,
  { pageSize, page, days, mode, includeGroups, debug } = {}
) {
  const base = getBaseUrl(req);
  const url =
    `${base}/api/insider-buys?wrap=1` +
    `&pageSize=${encodeURIComponent(pageSize)}` +
    `&page=${encodeURIComponent(page)}` +
    `&days=${encodeURIComponent(days)}` +
    (mode === "seed" ? `&mode=seed` : ``) +
    (includeGroups ? `&includeGroups=1` : ``) +
    (debug ? `&debug=1` : ``);

  const r = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`insider-buys HTTP ${r.status} ${txt}`.slice(0, 500));
  }

  const payload = await r.json();
  const data = Array.isArray(payload) ? payload : payload?.data;
  const meta = Array.isArray(payload) ? null : payload?.meta;

  return { url, data: Array.isArray(data) ? data : [], meta };
}

async function upsertBatch(client, rows) {
  let inserted = 0;

  for (const item of rows) {
    const purchasedTicker = item.purchasedTicker ? String(item.purchasedTicker) : "";
    const employerTicker = item.employerTicker ? String(item.employerTicker) : "";
    const insiderName = String(item.insiderName ?? "");
    const insiderTitle = String(item.insiderTitle ?? "");
    const transactionDate = String(item.transactionDate ?? "");
    const shares = Number(item.shares ?? 0);
    const pricePerShare = Number(item.pricePerShare ?? 0);

    // Deterministic id (matches migrate.js backfill)
    const idSeed = `${purchasedTicker}|${employerTicker}|${insiderName}|${insiderTitle}|${transactionDate}|${shares}|${pricePerShare}`;
    const id = await (async () => {
      // compute md5 in Postgres so it matches DB behavior
      const r = await client.query(`SELECT md5($1) AS id`, [idSeed]);
      return r.rows?.[0]?.id;
    })();

    await client.query(
      `
      INSERT INTO insider_transactions(
        id, cik,
        employer_ticker, employer_company,
        purchased_ticker, purchased_company,
        insider_name, insider_title,
        shares, price_per_share, total_value,
        transaction_date,
        signal_score, purchase_type,
        raw, updated_at
      )
      VALUES(
        $1,$2,$3,$4,$5,$6,$7,$8,
        $9,$10,$11,
        NULLIF($12,'')::date,
        $13,$14,
        $15::jsonb, now()
      )
      ON CONFLICT (id) DO UPDATE SET
        cik = COALESCE(EXCLUDED.cik, insider_transactions.cik),
        employer_ticker = COALESCE(EXCLUDED.employer_ticker, insider_transactions.employer_ticker),
        employer_company = COALESCE(EXCLUDED.employer_company, insider_transactions.employer_company),
        purchased_ticker = COALESCE(EXCLUDED.purchased_ticker, insider_transactions.purchased_ticker),
        purchased_company = COALESCE(EXCLUDED.purchased_company, insider_transactions.purchased_company),
        insider_name = COALESCE(EXCLUDED.insider_name, insider_transactions.insider_name),
        insider_title = COALESCE(EXCLUDED.insider_title, insider_transactions.insider_title),
        shares = COALESCE(EXCLUDED.shares, insider_transactions.shares),
        price_per_share = COALESCE(EXCLUDED.price_per_share, insider_transactions.price_per_share),
        total_value = COALESCE(EXCLUDED.total_value, insider_transactions.total_value),
        transaction_date = COALESCE(EXCLUDED.transaction_date, insider_transactions.transaction_date),
        signal_score = COALESCE(EXCLUDED.signal_score, insider_transactions.signal_score),
        purchase_type = COALESCE(EXCLUDED.purchase_type, insider_transactions.purchase_type),
        raw = COALESCE(EXCLUDED.raw, insider_transactions.raw),
        updated_at = now()
      `,
      [
        id,
        item.cik ? String(item.cik) : null,
        item.employerTicker ? String(item.employerTicker) : null,
        item.employerCompany ? String(item.employerCompany) : null,
        item.purchasedTicker ? String(item.purchasedTicker) : null,
        item.purchasedCompany ? String(item.purchasedCompany) : null,
        insiderName,
        insiderTitle,
        shares,
        pricePerShare,
        Number(item.totalValue ?? 0),
        transactionDate,
        Number(item.signalScore ?? 0),
        String(item.purchaseType ?? ""),
        JSON.stringify(item),
      ]
    );

    inserted += 1;
  }

  return inserted;
}

export default async function handler(req, res) {
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }
  if (!process.env.DATABASE_URL) {
    return res.status(500).json({ error: "DATABASE_URL missing" });
  }

  const mode = String(req.query.mode || "status"); // status | run | backfill
  const dryRun = bool(req.query.dryRun, false);

  const pageSize = num(req.query.pageSize, 50);
  const maxPages = num(req.query.maxPages, 3);
  const throttleMs = num(req.query.throttleMs, 700);
  const days = num(req.query.days, 30);
  const dataMode = String(req.query.dataMode || "live"); // live | seed
  const includeGroups = bool(req.query.includeGroups, true);

  const client = await pool.connect();

  try {
    if (mode === "status") {
      return res.status(200).json(await statusResponse(client));
    }

    if (mode !== "run" && mode !== "backfill") {
      return res.status(400).json({ error: "Unknown mode", allowed: ["status", "run", "backfill"] });
    }

    const cursorKey = mode === "run" ? "insider_buys_cursor" : "insider_buys_backfill_cursor";
    const existing = (await getState(client, cursorKey)) || {};
    const startPage = num(req.query.page, existing.page || 1);

    let page = startPage;
    let totalFetched = 0;
    let totalInserted = 0;
    let lastUrl = null;
    const startedAt = Date.now();

    for (let i = 0; i < maxPages; i++) {
      const { url, data, meta } = await fetchInsiderBuysFromSelf(req, {
        pageSize,
        page,
        days,
        mode: dataMode,
        includeGroups,
        debug: false,
      });

      lastUrl = url;
      totalFetched += data.length;

      if (!dryRun && data.length) {
        totalInserted += await upsertBatch(client, data);
      }

      await setState(client, cursorKey, {
        page: page + 1,
        pageSize,
        days,
        dataMode,
        updatedAt: nowIso(),
        lastMeta: meta || null,
      });

      if (data.length === 0) break;
      page += 1;

      if (throttleMs > 0) await sleep(throttleMs);
      if (Date.now() - startedAt > 22_000) break; // serverless guardrail
    }

    const out = {
      ok: true,
      mode,
      dryRun,
      fetched: totalFetched,
      inserted: totalInserted,
      cursor: await getState(client, cursorKey),
      lastUrl,
      ts: nowIso(),
    };

    await setState(client, "ingest_last_run", out);
    return res.status(200).json(out);
  } catch (err) {
    const msg = err?.message || String(err);
    try {
      await setState(client, "ingest_last_run", { ok: false, mode, error: msg, ts: nowIso() });
    } catch {}
    return res.status(500).json({ error: "Ingest error", details: msg });
  } finally {
    client.release();
  }
}
