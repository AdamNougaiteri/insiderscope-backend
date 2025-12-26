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

async function ensureBaseTables(client) {
  // companies table (keep it simple / compatible)
  await client.query(`
    CREATE TABLE IF NOT EXISTS companies (
      cik TEXT PRIMARY KEY,
      ticker TEXT,
      company_name TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  // ingestion_state table (matches what your /api/migrate output shows)
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

  // insider_transactions: DO NOT recreate if it exists; just ensure key columns exist.
  // Your table already exists with many columns; we just ensure id exists for convenience.
  await client.query(`
    ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS id TEXT;
  `);

  // Helpful indexes
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_ticker_date
    ON insider_transactions(ticker, transaction_date DESC);
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_purchased_ticker_date
    ON insider_transactions(purchased_ticker, transaction_date DESC);
  `);
}

async function getState(client, id) {
  const r = await client.query(`SELECT * FROM ingestion_state WHERE id=$1`, [id]);
  return r.rows?.[0] ?? null;
}

async function setState(client, id, patch) {
  // patch merges into value JSONB and updates columns if provided
  const current = await getState(client, id);
  const nextValue = { ...(current?.value || {}), ...(patch.value || {}) };

  await client.query(
    `
    INSERT INTO ingestion_state(id, cursor, last_run_at, status, last_error, value, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6::jsonb, now())
    ON CONFLICT (id) DO UPDATE SET
      cursor = COALESCE(EXCLUDED.cursor, ingestion_state.cursor),
      last_run_at = COALESCE(EXCLUDED.last_run_at, ingestion_state.last_run_at),
      status = COALESCE(EXCLUDED.status, ingestion_state.status),
      last_error = COALESCE(EXCLUDED.last_error, ingestion_state.last_error),
      value = EXCLUDED.value,
      updated_at = now()
    `,
    [
      id,
      patch.cursor ?? current?.cursor ?? null,
      patch.last_run_at ?? current?.last_run_at ?? null,
      patch.status ?? current?.status ?? null,
      patch.last_error ?? current?.last_error ?? null,
      JSON.stringify(nextValue),
    ]
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

  const state = await getState(client, "insider_buys_ingest");

  return {
    ok: true,
    tables: tables.rows.map((r) => r.table_name),
    counts,
    state: state
      ? {
          cursor: state.cursor,
          last_run_at: state.last_run_at,
          status: state.status,
          last_error: state.last_error,
          value: state.value,
          updated_at: state.updated_at,
        }
      : null,
    ts: nowIso(),
  };
}

async function fetchInsiderBuysFromSelf(
  req,
  { pageSize, page, days, dataMode, includeGroups } = {}
) {
  const base = getBaseUrl(req);
  const url =
    `${base}/api/insider-buys?wrap=1` +
    `&pageSize=${encodeURIComponent(pageSize)}` +
    `&page=${encodeURIComponent(page)}` +
    `&days=${encodeURIComponent(days)}` +
    (dataMode === "seed" ? `&mode=seed` : ``) +
    (includeGroups ? `&includeGroups=1` : ``);

  const r = await fetch(url, { headers: { Accept: "application/json" } });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`insider-buys HTTP ${r.status} ${txt}`.slice(0, 800));
  }

  const payload = await r.json();
  const data = payload?.data;
  const meta = payload?.meta;

  return { url, data: Array.isArray(data) ? data : [], meta: meta || null };
}

function coerceText(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

function coerceNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function buildTransactionId(item) {
  // Prefer item.id; otherwise build a stable fallback (best effort)
  const id = coerceText(item?.id);
  if (id) return id;

  const acc = coerceText(item?.accessionNo) || coerceText(item?.accession_no);
  const insider = coerceText(item?.insiderName);
  const dt = coerceText(item?.transactionDate);
  const ticker = coerceText(item?.purchasedTicker) || coerceText(item?.employerTicker);
  if (acc && dt && (insider || ticker)) {
    return `${acc}::${dt}::${insider || ""}::${ticker || ""}`.slice(0, 240);
  }
  return null;
}

async function upsertBatch(client, rows) {
  let inserted = 0;
  let skippedMissing = 0;

  for (const item of rows) {
    const transaction_id = buildTransactionId(item);
    const cik = coerceText(item?.cik);

    // Your DB enforces NOT NULL on these; do not insert if missing.
    if (!transaction_id || !cik) {
      skippedMissing += 1;
      continue;
    }

    const purchased_ticker = coerceText(item?.purchasedTicker);
    const purchased_company = coerceText(item?.purchasedCompany);
    const employer_ticker = coerceText(item?.employerTicker);
    const employer_company = coerceText(item?.employerCompany);

    const insider_name = coerceText(item?.insiderName);
    const insider_title = coerceText(item?.insiderTitle);

    const shares = coerceNum(item?.shares);
    const price_per_share = coerceNum(item?.pricePerShare);
    const total_value = coerceNum(item?.totalValue);

    const transaction_date = coerceText(item?.transactionDate); // cast to date below
    const signal_score = coerceNum(item?.signalScore);
    const purchase_type = coerceText(item?.purchaseType);

    // Insert into insider_transactions using your column names
    await client.query(
      `
      INSERT INTO insider_transactions (
        transaction_id,
        cik,
        employer_ticker,
        employer_company,
        purchased_ticker,
        purchased_company,
        insider_name,
        insider_title,
        shares,
        price_per_share,
        total_value,
        transaction_date,
        signal_score,
        purchase_type,
        raw,
        id,
        updated_at
      )
      VALUES (
        $1, $2,
        $3, $4,
        $5, $6,
        $7, $8,
        $9, $10, $11,
        NULLIF($12,'')::date,
        $13, $14,
        $15::jsonb,
        $16,
        now()
      )
      ON CONFLICT (transaction_id) DO UPDATE SET
        cik = EXCLUDED.cik,
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
        id = COALESCE(EXCLUDED.id, insider_transactions.id),
        updated_at = now()
      `,
      [
        transaction_id,
        cik,
        employer_ticker,
        employer_company,
        purchased_ticker,
        purchased_company,
        insider_name,
        insider_title,
        shares,
        price_per_share,
        total_value,
        transaction_date,
        signal_score,
        purchase_type,
        JSON.stringify(item),
        coerceText(item?.id), // keep original id if present
      ]
    );

    // Upsert companies (best effort)
    await client.query(
      `
      INSERT INTO companies(cik, ticker, company_name, updated_at)
      VALUES($1,$2,$3, now())
      ON CONFLICT (cik) DO UPDATE SET
        ticker = COALESCE(EXCLUDED.ticker, companies.ticker),
        company_name = COALESCE(EXCLUDED.company_name, companies.company_name),
        updated_at = now()
      `,
      [
        cik,
        purchased_ticker || employer_ticker,
        purchased_company || employer_company,
      ]
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
    await ensureBaseTables(client);

    if (mode === "status") {
      return res.status(200).json(await statusResponse(client));
    }

    const stateId = "insider_buys_ingest";
    const state = (await getState(client, stateId)) || {};
    const cursorObj = state.value?.cursor || {};

    const startPage = num(req.query.page, cursorObj.page || 1);

    let page = startPage;
    let fetched = 0;
    let inserted = 0;
    let skippedMissing = 0;
    let lastUrl = null;

    const startedAt = Date.now();

    await setState(client, stateId, {
      status: "running",
      last_error: null,
      last_run_at: new Date(),
      value: { cursor: { ...cursorObj, page: startPage } },
    });

    for (let i = 0; i < maxPages; i++) {
      const { url, data, meta } = await fetchInsiderBuysFromSelf(req, {
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
      }

      // advance cursor
      const nextCursor = { page: page + 1, pageSize, days, dataMode, lastMeta: meta || null };
      await setState(client, stateId, {
        cursor: JSON.stringify(nextCursor),
        value: { cursor: nextCursor },
      });

      if (data.length === 0) break;

      page += 1;
      if (throttleMs > 0) await sleep(throttleMs);

      // serverless guardrail
      if (Date.now() - startedAt > 22_000) break;
    }

    await setState(client, stateId, {
      status: "ok",
      last_error: null,
      last_run_at: new Date(),
      value: {
        cursor: { page, pageSize, days, dataMode },
        lastResult: { fetched, inserted, skippedMissing, dryRun, lastUrl },
      },
    });

    return res.status(200).json({
      ok: true,
      mode,
      dryRun,
      fetched,
      inserted,
      skippedMissing,
      nextPage: page,
      lastUrl,
      ts: nowIso(),
    });
  } catch (err) {
    const msg = err?.message || String(err);
    try {
      await setState(client, "insider_buys_ingest", {
        status: "error",
        last_error: msg,
        last_run_at: new Date(),
      });
    } catch {}
    return res.status(500).json({ error: "Ingest error", details: msg });
  } finally {
    client.release();
  }
}
