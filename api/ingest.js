import { Pool } from "pg";
import crypto from "crypto";

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

function stableTxId(item) {
  // Prefer an explicit id if the API gives it
  const direct =
    item?.transactionId ||
    item?.transaction_id ||
    item?.id ||
    item?.accessionNo ||
    item?.accession_no;

  if (direct && String(direct).trim()) return String(direct).trim();

  // Otherwise hash a deterministic fingerprint
  const fp = [
    item?.cik ?? "",
    item?.purchasedTicker ?? item?.ticker ?? "",
    item?.insiderName ?? "",
    item?.insiderTitle ?? "",
    item?.transactionDate ?? "",
    item?.shares ?? "",
    item?.pricePerShare ?? "",
    item?.totalValue ?? "",
  ].join("|");

  const h = crypto.createHash("sha256").update(fp).digest("hex").slice(0, 32);
  return `gen_${h}`;
}

async function ensureTablesExist(client) {
  // companies
  await client.query(`
    CREATE TABLE IF NOT EXISTS companies (
      cik TEXT PRIMARY KEY,
      ticker TEXT,
      company_name TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  // insider_transactions (match what your /api/migrate shows)
  await client.query(`
    CREATE TABLE IF NOT EXISTS insider_transactions (
      transaction_id TEXT NOT NULL,
      cik TEXT,
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
      created_at TIMESTAMPTZ DEFAULT now(),
      employer_ticker TEXT,
      employer_company TEXT,
      purchased_ticker TEXT,
      purchased_company TEXT,
      signal_score NUMERIC,
      purchase_type TEXT,
      raw JSONB,
      updated_at TIMESTAMPTZ DEFAULT now(),
      id TEXT,
      PRIMARY KEY (transaction_id)
    );
  `);

  // ingestion_state (match your columns: id/cursor/last_run_at/status/last_error/value/updated_at)
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

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_purchased_ticker_date
    ON insider_transactions(purchased_ticker, transaction_date DESC);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_insider_date
    ON insider_transactions(insider_name, transaction_date DESC);
  `);
}

async function getStateRow(client, id) {
  const r = await client.query(`SELECT * FROM ingestion_state WHERE id=$1`, [id]);
  return r.rows?.[0] ?? null;
}

async function upsertStateRow(client, id, patch) {
  // Patch fields are optional; we store a JSON blob in value as well
  const existing = (await getStateRow(client, id)) || {};
  const nextValue = { ...(existing.value || {}), ...(patch.value || {}) };

  const cursor = patch.cursor ?? existing.cursor ?? null;
  const status = patch.status ?? existing.status ?? null;
  const last_error = patch.last_error ?? existing.last_error ?? null;
  const last_run_at = patch.last_run_at ?? existing.last_run_at ?? null;

  await client.query(
    `
    INSERT INTO ingestion_state(id, cursor, last_run_at, status, last_error, value, updated_at)
    VALUES($1,$2,$3,$4,$5,$6::jsonb, now())
    ON CONFLICT (id) DO UPDATE SET
      cursor = EXCLUDED.cursor,
      last_run_at = EXCLUDED.last_run_at,
      status = EXCLUDED.status,
      last_error = EXCLUDED.last_error,
      value = EXCLUDED.value,
      updated_at = now()
    `,
    [id, cursor, last_run_at, status, last_error, JSON.stringify(nextValue)]
  );

  return await getStateRow(client, id);
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

  const state = await getStateRow(client, "insider_buys");

  return {
    ok: true,
    tables: tables.rows.map((r) => r.table_name),
    counts,
    state,
    ts: nowIso(),
  };
}

async function fetchInsiderBuysFromSelf(req, { pageSize, page, days, mode, includeGroups } = {}) {
  const base = getBaseUrl(req);
  const url =
    `${base}/api/insider-buys?wrap=1` +
    `&pageSize=${encodeURIComponent(pageSize)}` +
    `&page=${encodeURIComponent(page)}` +
    `&days=${encodeURIComponent(days)}` +
    (mode === "seed" ? `&mode=seed` : ``) +
    (includeGroups ? `&includeGroups=1` : ``);

  const r = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`insider-buys HTTP ${r.status} ${txt}`.slice(0, 500));
  }

  const payload = await r.json();
  const data = Array.isArray(payload) ? payload : payload?.data;
  const meta = Array.isArray(payload) ? null : payload?.meta;

  return { url, data: Array.isArray(data) ? data : [], meta, payload };
}

async function upsertBatch(client, rows) {
  let upserted = 0;

  for (const item of rows) {
    const transaction_id = stableTxId(item);

    const cik = item?.cik ? String(item.cik) : null;

    const purchased_ticker = item?.purchasedTicker ? String(item.purchasedTicker) : null;
    const purchased_company = item?.purchasedCompany ? String(item.purchasedCompany) : null;

    const employer_ticker = item?.employerTicker ? String(item.employerTicker) : null;
    const employer_company = item?.employerCompany ? String(item.employerCompany) : null;

    const insider_name = item?.insiderName ? String(item.insiderName) : null;
    const insider_title = item?.insiderTitle ? String(item.insiderTitle) : null;

    const transaction_date = item?.transactionDate ? String(item.transactionDate) : null;
    const filing_date = item?.filingDate ? String(item.filingDate) : null;

    const shares = item?.shares != null ? Number(item.shares) : null;
    const price_per_share = item?.pricePerShare != null ? Number(item.pricePerShare) : null;
    const total_value = item?.totalValue != null ? Number(item.totalValue) : null;

    const signal_score = item?.signalScore != null ? Number(item.signalScore) : null;
    const purchase_type = item?.purchaseType ? String(item.purchaseType) : null;

    const accession_no = item?.accessionNo ? String(item.accessionNo) : null;
    const source_url = item?.sourceUrl ? String(item.sourceUrl) : null;

    // Conservative flags (best-effort)
    const is_exercise = false;
    const is_10b5_1 = false;
    const is_purchase = true;

    // Insert/upsert into insider_transactions
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
        transaction_code,
        is_purchase,
        is_exercise,
        is_10b5_1,
        accession_no,
        source_url,
        employer_ticker,
        employer_company,
        purchased_ticker,
        purchased_company,
        signal_score,
        purchase_type,
        raw,
        updated_at,
        id
      )
      VALUES(
        $1,$2,$3,$4,$5,$6,
        NULLIF($7,'')::date,
        NULLIF($8,'')::date,
        $9,$10,$11,
        $12,
        $13,$14,$15,
        $16,$17,
        $18,$19,$20,$21,
        $22,$23,
        $24::jsonb,
        now(),
        $25
      )
      ON CONFLICT (transaction_id) DO UPDATE SET
        cik = COALESCE(EXCLUDED.cik, insider_transactions.cik),
        ticker = COALESCE(EXCLUDED.ticker, insider_transactions.ticker),
        company_name = COALESCE(EXCLUDED.company_name, insider_transactions.company_name),
        insider_name = COALESCE(EXCLUDED.insider_name, insider_transactions.insider_name),
        insider_title = COALESCE(EXCLUDED.insider_title, insider_transactions.insider_title),
        transaction_date = COALESCE(EXCLUDED.transaction_date, insider_transactions.transaction_date),
        filing_date = COALESCE(EXCLUDED.filing_date, insider_transactions.filing_date),
        shares = COALESCE(EXCLUDED.shares, insider_transactions.shares),
        price_per_share = COALESCE(EXCLUDED.price_per_share, insider_transactions.price_per_share),
        total_value = COALESCE(EXCLUDED.total_value, insider_transactions.total_value),
        transaction_code = COALESCE(EXCLUDED.transaction_code, insider_transactions.transaction_code),
        is_purchase = COALESCE(EXCLUDED.is_purchase, insider_transactions.is_purchase),
        is_exercise = COALESCE(EXCLUDED.is_exercise, insider_transactions.is_exercise),
        is_10b5_1 = COALESCE(EXCLUDED.is_10b5_1, insider_transactions.is_10b5_1),
        accession_no = COALESCE(EXCLUDED.accession_no, insider_transactions.accession_no),
        source_url = COALESCE(EXCLUDED.source_url, insider_transactions.source_url),
        employer_ticker = COALESCE(EXCLUDED.employer_ticker, insider_transactions.employer_ticker),
        employer_company = COALESCE(EXCLUDED.employer_company, insider_transactions.employer_company),
        purchased_ticker = COALESCE(EXCLUDED.purchased_ticker, insider_transactions.purchased_ticker),
        purchased_company = COALESCE(EXCLUDED.purchased_company, insider_transactions.purchased_company),
        signal_score = COALESCE(EXCLUDED.signal_score, insider_transactions.signal_score),
        purchase_type = COALESCE(EXCLUDED.purchase_type, insider_transactions.purchase_type),
        raw = COALESCE(EXCLUDED.raw, insider_transactions.raw),
        updated_at = now(),
        id = COALESCE(EXCLUDED.id, insider_transactions.id)
      `,
      [
        transaction_id,
        cik,
        purchased_ticker || employer_ticker,
        purchased_company || employer_company,
        insider_name,
        insider_title,
        transaction_date,
        filing_date,
        shares,
        price_per_share,
        total_value,
        null,
        is_purchase,
        is_exercise,
        is_10b5_1,
        accession_no,
        source_url,
        employer_ticker,
        employer_company,
        purchased_ticker,
        purchased_company,
        signal_score,
        purchase_type,
        JSON.stringify(item),
        item?.id ? String(item.id) : null,
      ]
    );

    // Upsert companies (best-effort)
    if (cik) {
      await client.query(
        `
        INSERT INTO companies(cik, ticker, company_name, updated_at)
        VALUES($1,$2,$3, now())
        ON CONFLICT (cik) DO UPDATE SET
          ticker = COALESCE(EXCLUDED.ticker, companies.ticker),
          company_name = COALESCE(EXCLUDED.company_name, companies.company_name),
          updated_at = now()
        `,
        [cik, purchased_ticker || employer_ticker, purchased_company || employer_company]
      );
    }

    upserted += 1;
  }

  return upserted;
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
    await ensureTablesExist(client);

    if (mode === "status") {
      return res.status(200).json(await statusResponse(client));
    }

    const stateId = "insider_buys";
    const state = (await getStateRow(client, stateId)) || {};
    const stateValue = state.value || {};

    const startPage =
      num(req.query.page, null) ??
      num(stateValue.page, 1);

    let page = startPage;
    let fetched = 0;
    let inserted = 0;
    let lastUrl = null;

    const startedAt = Date.now();

    await upsertStateRow(client, stateId, {
      status: "running",
      last_error: null,
      last_run_at: new Date().toISOString(),
      cursor: `page=${page}`,
      value: { ...stateValue, mode, page, days, pageSize, dataMode },
    });

    for (let i = 0; i < maxPages; i++) {
      const { url, data, meta } = await fetchInsiderBuysFromSelf(req, {
        pageSize,
        page,
        days,
        mode: dataMode,
        includeGroups,
      });

      lastUrl = url;
      fetched += data.length;

      if (!dryRun && data.length) {
        inserted += await upsertBatch(client, data);
      }

      // advance cursor
      page += 1;

      await upsertStateRow(client, stateId, {
        status: "running",
        cursor: `page=${page}`,
        value: {
          ...stateValue,
          mode,
          page,
          days,
          pageSize,
          dataMode,
          lastMeta: meta || null,
          lastUrl,
        },
      });

      if (data.length === 0) break;
      if (throttleMs > 0) await sleep(throttleMs);

      // serverless time guard
      if (Date.now() - startedAt > 22_000) break;
    }

    await upsertStateRow(client, stateId, {
      status: "ok",
      last_error: null,
      last_run_at: new Date().toISOString(),
      cursor: `page=${page}`,
      value: {
        ...stateValue,
        mode,
        page,
        days,
        pageSize,
        dataMode,
        lastUrl,
        fetched,
        inserted,
        finishedAt: nowIso(),
      },
    });

    return res.status(200).json({
      ok: true,
      mode,
      dryRun,
      fetched,
      inserted,
      nextCursor: `page=${page}`,
      lastUrl,
      ts: nowIso(),
    });
  } catch (err) {
    const msg = err?.message || String(err);

    try {
      await upsertStateRow(client, "insider_buys", {
        status: "error",
        last_error: msg,
        last_run_at: new Date().toISOString(),
        value: { error: msg, ts: nowIso() },
      });
    } catch {}

    return res.status(500).json({ error: "Ingest error", details: msg });
  } finally {
    client.release();
  }
}
