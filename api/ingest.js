import crypto from "crypto";
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

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function normalizeTicker(t) {
  if (!t) return null;
  return String(t).trim().toUpperCase();
}

function pickCik(item) {
  // Try a bunch of likely keys across your pipeline versions
  const candidates = [
    item.cik,
    item.issuerCik,
    item.employerCik,
    item.companyCik,
    item.raw?.cik,
    item.raw?.issuerCik,
    item.raw?.issuer?.cik,
  ];
  for (const c of candidates) {
    const s = c === null || c === undefined ? "" : String(c).trim();
    if (s) return s.padStart(10, "0"); // SEC CIK is often 10 digits
  }
  return null;
}

function computeTransactionId(item) {
  // Prefer explicit IDs if present
  const explicit =
    item.transaction_id ||
    item.transactionId ||
    item.id ||
    item.raw?.transaction_id ||
    item.raw?.transactionId ||
    item.raw?.id;

  if (explicit) return String(explicit);

  // Deterministic fallback (stable across runs)
  const parts = [
    pickCik(item) || "",
    normalizeTicker(item.purchasedTicker) || normalizeTicker(item.employerTicker) || "",
    item.insiderName || item.insider_name || "",
    item.transactionDate || item.transaction_date || "",
    item.filingDate || item.filing_date || "",
    item.shares ?? "",
    item.pricePerShare ?? item.price_per_share ?? "",
    item.totalValue ?? item.total_value ?? "",
    item.transactionCode || item.transaction_code || "",
    item.sourceUrl || item.source_url || "",
  ].map((x) => String(x).trim());

  return sha1(parts.join("|"));
}

async function ensureSchema(client) {
  // Keep this LIGHT: we don’t want to fight whatever you already created.
  // Ensure ingestion_state exists with the schema your /api/migrate output shows.
  await client.query(`
    CREATE TABLE IF NOT EXISTS ingestion_state (
      id TEXT PRIMARY KEY,
      cursor TEXT,
      last_run_at TIMESTAMPTZ,
      status TEXT,
      last_error TEXT,
      value JSONB,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  // companies table (optional but helpful)
  await client.query(`
    CREATE TABLE IF NOT EXISTS companies (
      cik TEXT PRIMARY KEY,
      ticker TEXT,
      company_name TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  // insider_transactions table: create only if missing.
  // If you already have it, we won't try to redefine it here.
  await client.query(`
    CREATE TABLE IF NOT EXISTS insider_transactions (
      transaction_id TEXT PRIMARY KEY,
      cik TEXT NOT NULL,
      ticker TEXT,
      company_name TEXT,
      insider_name TEXT,
      insider_title TEXT,
      transaction_date DATE,
      filing_date DATE,
      shares NUMERIC,
      price_per_share NUMERIC,
      total_value NUMERIC,
      transaction_code TEXT,
      is_purchase BOOLEAN,
      is_exercise BOOLEAN,
      is_10b5_1 BOOLEAN,
      accession_no TEXT,
      source_url TEXT,
      employer_ticker TEXT,
      employer_company TEXT,
      purchased_ticker TEXT,
      purchased_company TEXT,
      signal_score NUMERIC,
      purchase_type TEXT,
      raw JSONB,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_it_purchased_ticker_date
    ON insider_transactions(purchased_ticker, transaction_date DESC);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_it_insider_date
    ON insider_transactions(insider_name, transaction_date DESC);
  `);
}

async function getState(client, id) {
  const r = await client.query(
    `SELECT id, cursor, last_run_at, status, last_error, value, updated_at
     FROM ingestion_state WHERE id=$1`,
    [id]
  );
  return r.rows?.[0] ?? null;
}

async function setState(client, id, patch) {
  const current = await getState(client, id);

  const next = {
    cursor: patch.cursor ?? current?.cursor ?? null,
    last_run_at: patch.last_run_at ?? current?.last_run_at ?? null,
    status: patch.status ?? current?.status ?? null,
    last_error: patch.last_error ?? current?.last_error ?? null,
    value: patch.value ?? current?.value ?? null,
  };

  await client.query(
    `
    INSERT INTO ingestion_state(id, cursor, last_run_at, status, last_error, value, updated_at)
    VALUES($1, $2, $3, $4, $5, $6::jsonb, now())
    ON CONFLICT (id) DO UPDATE SET
      cursor = EXCLUDED.cursor,
      last_run_at = EXCLUDED.last_run_at,
      status = EXCLUDED.status,
      last_error = EXCLUDED.last_error,
      value = EXCLUDED.value,
      updated_at = now()
    `,
    [id, next.cursor, next.last_run_at, next.status, next.last_error, JSON.stringify(next.value)]
  );

  return getState(client, id);
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

  return {
    ok: true,
    tables: tables.rows.map((r) => r.table_name),
    counts,
    state,
    ts: nowIso(),
  };
}

async function fetchInsiderBuysFromSelf(req, { pageSize, page, days, dataMode, includeGroups } = {}) {
  const base = getBaseUrl(req);
  const url =
    `${base}/api/insider-buys?wrap=1` +
    `&pageSize=${encodeURIComponent(pageSize)}` +
    `&page=${encodeURIComponent(page)}` +
    `&days=${encodeURIComponent(days)}` +
    (dataMode === "seed" ? `&mode=seed` : ``) +
    (includeGroups ? `&includeGroups=1` : ``);

  const r = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });

  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`insider-buys HTTP ${r.status} ${txt}`.slice(0, 500));
  }

  const payload = await r.json();
  const data = Array.isArray(payload) ? payload : payload?.data;
  return { url, data: Array.isArray(data) ? data : [], payload };
}

async function upsertBatch(client, rows) {
  let inserted = 0;
  let skippedMissing = 0;

  for (const item of rows) {
    const cik = pickCik(item);
    if (!cik) {
      skippedMissing += 1;
      continue; // REQUIRED by your DB schema
    }

    const transactionId = computeTransactionId(item);

    const purchasedTicker = normalizeTicker(item.purchasedTicker);
    const employerTicker = normalizeTicker(item.employerTicker);

    const purchasedCompany = item.purchasedCompany ? String(item.purchasedCompany) : null;
    const employerCompany = item.employerCompany ? String(item.employerCompany) : null;

    const insiderName = item.insiderName ? String(item.insiderName) : (item.insider_name ? String(item.insider_name) : null);
    const insiderTitle = item.insiderTitle ? String(item.insiderTitle) : (item.insider_title ? String(item.insider_title) : null);

    const transactionDate = item.transactionDate ?? item.transaction_date ?? null;
    const filingDate = item.filingDate ?? item.filing_date ?? null;

    const shares = item.shares ?? null;
    const pricePerShare = item.pricePerShare ?? item.price_per_share ?? null;
    const totalValue = item.totalValue ?? item.total_value ?? null;

    const signalScore = item.signalScore ?? item.signal_score ?? null;
    const purchaseType = item.purchaseType ?? item.purchase_type ?? null;

    await client.query(
      `
      INSERT INTO insider_transactions(
        transaction_id,
        cik,
        ticker,
        company_name,
        insider_name,
        insider_title,
        transaction_date,
        filing_date,
        shares,
        price_per_share,
        total_value,
        employer_ticker,
        employer_company,
        purchased_ticker,
        purchased_company,
        signal_score,
        purchase_type,
        raw,
        updated_at
      )
      VALUES(
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        NULLIF($7,'')::date,
        NULLIF($8,'')::date,
        $9::numeric,
        $10::numeric,
        $11::numeric,
        $12,
        $13,
        $14,
        $15,
        $16::numeric,
        $17,
        $18::jsonb,
        now()
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
        insiderName,
        insiderTitle,
        transactionDate ? String(transactionDate) : "",
        filingDate ? String(filingDate) : "",
        shares ?? null,
        pricePerShare ?? null,
        totalValue ?? null,
        employerTicker,
        employerCompany,
        purchasedTicker,
        purchasedCompany,
        signalScore ?? null,
        purchaseType ? String(purchaseType) : null,
        JSON.stringify(item),
      ]
    );

    // best-effort companies upsert
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

    inserted += 1;
  }

  return { inserted, skippedMissing };
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
  const maxPages = num(req.query.maxPages, 2);
  const throttleMs = num(req.query.throttleMs, 700);
  const days = num(req.query.days, 30);
  const dataMode = String(req.query.dataMode || "live"); // live | seed
  const includeGroups = bool(req.query.includeGroups, true);

  const client = await pool.connect();

  try {
    await ensureSchema(client);

    if (mode === "status") {
      const out = await statusResponse(client);
      return res.status(200).json(out);
    }

    // cursor lives in ingestion_state.id = 'insider_buys'
    const state = (await getState(client, "insider_buys")) || {};
    const startPage = num(req.query.page, (() => {
      const c = state.cursor || "";
      const m = String(c).match(/page=(\d+)/);
      return m ? Number(m[1]) : 1;
    })());

    let page = startPage;
    let fetched = 0;
    let inserted = 0;
    let skippedMissing = 0;
    let lastUrl = null;

    const startedAt = Date.now();

    for (let i = 0; i < maxPages; i++) {
      const { url, data } = await fetchInsiderBuysFromSelf(req, {
        pageSize,
        page,
        days,
        dataMode,
        includeGroups,
      });

      lastUrl = url;
      fetched += data.length;

      if (!dryRun && data.length) {
        const r = await upsertBatch(client, data);
        inserted += r.inserted;
        skippedMissing += r.skippedMissing;
      } else if (data.length) {
        // dryRun still counts how many would be skipped
        for (const item of data) if (!pickCik(item)) skippedMissing += 1;
      }

      // advance cursor regardless (prevents infinite loop on “bad” page)
      await setState(client, "insider_buys", {
        cursor: `page=${page + 1}`,
        last_run_at: new Date().toISOString(),
        status: "ok",
        last_error: null,
        value: {
          ts: nowIso(),
          mode,
          page,
          nextPage: page + 1,
          days,
          dataMode,
          pageSize,
          maxPages,
          throttleMs,
          fetched,
          inserted,
          skippedMissing,
          lastUrl,
          dryRun,
        },
      });

      if (data.length === 0) break;

      page += 1;
      if (throttleMs > 0) await sleep(throttleMs);

      // keep serverless safe
      if (Date.now() - startedAt > 22_000) break;
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
  } catch (err) {
    const msg = err?.message || String(err);
    try {
      await setState(client, "insider_buys", {
        last_run_at: new Date().toISOString(),
        status: "error",
        last_error: msg,
        value: { ts: nowIso(), mode, error: msg },
      });
    } catch {}
    return res.status(500).json({ error: "Ingest error", details: msg });
  } finally {
    client.release();
  }
}
