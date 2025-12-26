// /api/ingest.js
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function num(v, def) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}
function bool(v, def = false) {
  if (v === undefined || v === null) return def;
  const s = String(v).toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "on";
}
function nowIso() {
  return new Date().toISOString();
}
function getBaseUrl(req) {
  const proto = (req.headers["x-forwarded-proto"] || "https").toString();
  const host = (req.headers["x-forwarded-host"] || req.headers.host).toString();
  return `${proto}://${host}`;
}

async function upsertState(client, { id, cursor, status, lastError, value }) {
  await client.query(
    `
    INSERT INTO ingestion_state(id, cursor, last_run_at, status, last_error, value, updated_at)
    VALUES($1, $2, now(), $3, $4, $5::jsonb, now())
    ON CONFLICT (id) DO UPDATE SET
      cursor = EXCLUDED.cursor,
      last_run_at = EXCLUDED.last_run_at,
      status = EXCLUDED.status,
      last_error = EXCLUDED.last_error,
      value = EXCLUDED.value,
      updated_at = now()
    `,
    [id, cursor || null, status || null, lastError || null, JSON.stringify(value || null)]
  );
}

async function getState(client, id) {
  const r = await client.query(`SELECT * FROM ingestion_state WHERE id=$1`, [id]);
  return r.rows?.[0] || null;
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
  const state = await getState(client, "insider_buys");
  return { ok: true, tables: tables.rows.map((r) => r.table_name), counts, state, ts: nowIso() };
}

async function fetchInsiderBuys(req, { pageSize, page, days, includeGroups }) {
  const base = getBaseUrl(req);
  const url =
    `${base}/api/insider-buys?wrap=1` +
    `&pageSize=${encodeURIComponent(pageSize)}` +
    `&page=${encodeURIComponent(page)}` +
    `&days=${encodeURIComponent(days)}` +
    (includeGroups ? `&includeGroups=1` : ``);

  const r = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`insider-buys HTTP ${r.status} ${txt}`.slice(0, 500));
  }
  const payload = await r.json();
  const data = Array.isArray(payload) ? payload : payload?.data;
  return { url, data: Array.isArray(data) ? data : [] };
}

async function upsertBatch(client, rows) {
  let inserted = 0;
  let skippedMissing = 0;

  for (const item of rows) {
    const transactionId = String(item.id || "").trim();
    const cik = String(item.cik || "").trim(); // now provided by insider-buys.js

    if (!transactionId || !cik) {
      skippedMissing++;
      continue;
    }

    const purchasedTicker = item.purchasedTicker ? String(item.purchasedTicker) : null;
    const purchasedCompany = item.purchasedCompany ? String(item.purchasedCompany) : null;
    const employerTicker = item.employerTicker ? String(item.employerTicker) : null;
    const employerCompany = item.employerCompany ? String(item.employerCompany) : null;

    await client.query(
      `
      INSERT INTO insider_transactions(
        transaction_id, cik,
        ticker, company_name,
        insider_name, insider_title,
        transaction_date, filing_date,
        shares, price_per_share, total_value,
        transaction_code, is_purchase, is_exercise, is_10b5_1,
        accession_no, source_url,
        employer_ticker, employer_company,
        purchased_ticker, purchased_company,
        signal_score, purchase_type,
        raw, updated_at
      )
      VALUES(
        $1,$2,
        $3,$4,
        $5,$6,
        NULLIF($7,'')::date, NULLIF($8,'')::date,
        $9,$10,$11,
        $12,$13,$14,$15,
        $16,$17,
        $18,$19,
        $20,$21,
        $22,$23,
        $24::jsonb, now()
      )
      ON CONFLICT (transaction_id) DO UPDATE SET
        cik = EXCLUDED.cik,
        ticker = COALESCE(EXCLUDED.ticker, insider_transactions.ticker),
        company_name = COALESCE(EXCLUDED.company_name, insider_transactions.company_name),
        insider_name = COALESCE(EXCLUDED.insider_name, insider_transactions.insider_name),
        insider_title = COALESCE(EXCLUDED.insider_title, insider_transactions.insider_title),
        transaction_date = COALESCE(EXCLUDED.transaction_date, insider_transactions.transaction_date),
        filing_date = COALESCE(EXCLUDED.filing_date, insider_transactions.filing_date),
        shares = COALESCE(EXCLUDED.shares, insider_transactions.shares),
        price_per_share = COALESCE(EXCLUDED.price_per_share, insider_transactions.price_per_share),
        total_value = COALESCE(EXCLUDED.total_value, insider_transactions.total_value),
        accession_no = COALESCE(EXCLUDED.accession_no, insider_transactions.accession_no),
        source_url = COALESCE(EXCLUDED.source_url, insider_transactions.source_url),
        employer_ticker = COALESCE(EXCLUDED.employer_ticker, insider_transactions.employer_ticker),
        employer_company = COALESCE(EXCLUDED.employer_company, insider_transactions.employer_company),
        purchased_ticker = COALESCE(EXCLUDED.purchased_ticker, insider_transactions.purchased_ticker),
        purchased_company = COALESCE(EXCLUDED.purchased_company, insider_transactions.purchased_company),
        signal_score = COALESCE(EXCLUDED.signal_score, insider_transactions.signal_score),
        purchase_type = COALESCE(EXCLUDED.purchase_type, insider_transactions.purchase_type),
        raw = COALESCE(EXCLUDED.raw, insider_transactions.raw),
        updated_at = now()
      `,
      [
        transactionId,
        cik,
        purchasedTicker || employerTicker,
        purchasedCompany || employerCompany,
        String(item.insiderName || ""),
        String(item.insiderTitle || ""),
        String(item.transactionDate || ""),
        String(item.filingDate || ""),
        Number(item.shares || 0),
        Number(item.pricePerShare || 0),
        Number(item.totalValue || 0),
        "P",
        true,
        false,
        null,
        String(item.accessionNo || ""),
        String(item.sourceUrl || ""),
        employerTicker,
        employerCompany,
        purchasedTicker,
        purchasedCompany,
        Number(item.signalScore || 0),
        String(item.purchaseType || ""),
        JSON.stringify(item),
      ]
    );

    // companies upsert (optional)
    await client.query(
      `
      INSERT INTO companies(cik, ticker, company_name, updated_at)
      VALUES($1,$2,$3, now())
      ON CONFLICT (cik) DO UPDATE SET
        ticker = COALESCE(EXCLUDED.ticker, companies.ticker),
        company_name = COALESCE(EXCLUDED.company_name, companies.company_name),
        updated_at = now()
      `,
      [cik, purchasedTicker || employerTicker, purchasedCompany || employerCompany]
    );

    inserted++;
  }

  return { inserted, skippedMissing };
}

export default async function handler(req, res) {
  if (req.method !== "GET" && req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });
  if (!process.env.DATABASE_URL) return res.status(500).json({ error: "DATABASE_URL missing" });

  const mode = String(req.query.mode || "status"); // status | run | backfill
  const dryRun = bool(req.query.dryRun, false);

  const pageSize = num(req.query.pageSize, 50);
  const maxPages = num(req.query.maxPages, 2);
  const throttleMs = num(req.query.throttleMs, 700);
  const days = num(req.query.days, 30);
  const includeGroups = bool(req.query.includeGroups, true);

  const client = await pool.connect();
  const startedAt = Date.now();

  try {
    if (mode === "status") {
      return res.status(200).json(await statusResponse(client));
    }

    const stateId = "insider_buys";
    const prev = await getState(client, stateId);

    // cursor format: "page=12"
    const prevPage =
      prev?.cursor && /^page=\d+$/i.test(prev.cursor) ? Number(prev.cursor.split("=")[1]) : 1;

    const startPage = num(req.query.page, prevPage);
    let page = startPage;

    let fetched = 0;
    let inserted = 0;
    let skippedMissing = 0;
    let lastUrl = null;

    for (let i = 0; i < maxPages; i++) {
      const { url, data } = await fetchInsiderBuys(req, { pageSize, page, days, includeGroups });
      lastUrl = url;

      fetched += data.length;

      if (!dryRun && data.length) {
        const r = await upsertBatch(client, data);
        inserted += r.inserted;
        skippedMissing += r.skippedMissing;
      } else if (data.length) {
        // dryrun still “counts” skipped missing for debugging
        for (const item of data) {
          if (!item?.id || !item?.cik) skippedMissing++;
        }
      }

      // update cursor after each page
      await upsertState(client, {
        id: stateId,
        cursor: `page=${page + 1}`,
        status: "ok",
        lastError: null,
        value: { ts: nowIso(), mode, page, pageSize, days },
      });

      page++;
      if (throttleMs > 0) await sleep(throttleMs);
      if (Date.now() - startedAt > 22_000) break;
      if (data.length === 0) break;
    }

    const out = {
      ok: true,
      mode,
      dryRun,
      fetched,
      inserted,
      skippedMissing,
      nextPage: page,
      lastUrl,
      ts: nowIso(),
    };

    return res.status(200).json(out);
  } catch (e) {
    const msg = e?.message || String(e);
    try {
      await upsertState(client, {
        id: "insider_buys",
        cursor: null,
        status: "error",
        lastError: msg,
        value: { ts: nowIso(), mode, error: msg, pageSize, maxPages, days },
      });
    } catch {}
    return res.status(500).json({ error: "Ingest error", details: msg });
  } finally {
    client.release();
  }
}
