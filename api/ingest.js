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

async function ensureSchema(client) {
  // --- Companies
  await client.query(`
    CREATE TABLE IF NOT EXISTS companies (
      cik TEXT PRIMARY KEY,
      ticker TEXT,
      company_name TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  // Add missing columns safely if table already exists with a different shape
  await client.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS ticker TEXT;`);
  await client.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_name TEXT;`);
  await client.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();`);
  await client.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();`);

  // --- Insider transactions
  await client.query(`
    CREATE TABLE IF NOT EXISTS insider_transactions (
      id TEXT PRIMARY KEY
    );
  `);

  // Ensure required columns exist
  const alterTx = [
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS cik TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS employer_ticker TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS employer_company TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS purchased_ticker TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS purchased_company TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS insider_name TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS insider_title TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS shares NUMERIC;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS price_per_share NUMERIC;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS total_value NUMERIC;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS transaction_date DATE;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS signal_score NUMERIC;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS purchase_type TEXT;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS raw JSONB;`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();`,
    `ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();`,
  ];
  for (const q of alterTx) await client.query(q);

  // Indexes
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_purchased_ticker_date
    ON insider_transactions(purchased_ticker, transaction_date DESC);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_insider_date
    ON insider_transactions(insider_name, transaction_date DESC);
  `);

  // --- Ingestion state (match what your /api/migrate shows: id + value)
  await client.query(`
    CREATE TABLE IF NOT EXISTS ingestion_state (
      id TEXT PRIMARY KEY,
      value JSONB,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  // If Neon integration created a fancier ingestion_state, these will just no-op if already present
  await client.query(`ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS id TEXT;`);
  await client.query(`ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS value JSONB;`);
  await client.query(`ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();`);

  // Ensure primary key on id (safe-ish)
  // If a PK already exists, this may throw; so wrap it.
  try {
    await client.query(`ALTER TABLE ingestion_state ADD CONSTRAINT ingestion_state_pkey PRIMARY KEY (id);`);
  } catch {}
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
  { pageSize, page, days, mode, includeGroups } = {}
) {
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
    throw new Error(`insider-buys HTTP ${r.status} ${txt}`.slice(0, 800));
  }

  const payload = await r.json();
  const data = Array.isArray(payload) ? payload : payload?.data;
  const meta = Array.isArray(payload) ? null : payload?.meta;

  return { url, data: Array.isArray(data) ? data : [], meta };
}

async function upsertBatch(client, rows) {
  let upserts = 0;

  for (const item of rows) {
    const id = String(item.id ?? "");
    if (!id) continue;

    const purchasedTicker = item.purchasedTicker ? String(item.purchasedTicker) : null;
    const purchasedCompany = item.purchasedCompany ? String(item.purchasedCompany) : null;
    const employerTicker = item.employerTicker ? String(item.employerTicker) : null;
    const employerCompany = item.employerCompany ? String(item.employerCompany) : null;

    // Note: your current insider-buys payload might not include cik — that's okay
    const cik = item.cik ? String(item.cik) : null;

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
        cik,
        employerTicker,
        employerCompany,
        purchasedTicker,
        purchasedCompany,
        String(item.insiderName ?? ""),
        String(item.insiderTitle ?? ""),
        Number(item.shares ?? 0),
        Number(item.pricePerShare ?? 0),
        Number(item.totalValue ?? 0),
        String(item.transactionDate ?? ""),
        Number(item.signalScore ?? 0),
        String(item.purchaseType ?? ""),
        JSON.stringify(item),
      ]
    );

    // Upsert companies best-effort if cik exists
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
        [cik, purchasedTicker || employerTicker, purchasedCompany || employerCompany]
      );
    }

    upserts += 1;
  }

  return upserts;
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
  const throttleMs = num(req.query.throttleMs, 600);
  const days = num(req.query.days, 30);

  const dataMode = String(req.query.dataMode || "live"); // live | seed
  const includeGroups = bool(req.query.includeGroups, true);

  const client = await pool.connect();

  try {
    await ensureSchema(client);

    if (mode === "status") {
      return res.status(200).json(await statusResponse(client));
    }

    if (mode !== "run" && mode !== "backfill") {
      return res.status(400).json({ error: "Unknown mode", allowed: ["status", "run", "backfill"] });
    }

    const cursorId = mode === "backfill" ? "insider_buys_backfill_cursor" : "insider_buys_cursor";
    const existingCursor = (await getState(client, cursorId)) || {};
    const startPage = num(req.query.page, existingCursor.page || 1);

    let page = startPage;
    let fetched = 0;
    let inserted = 0;
    let lastUrl = null;

    const startedAt = Date.now();

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

      // cursor advance
      await setState(client, cursorId, {
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

      // stay under typical serverless execution time
      if (Date.now() - startedAt > 22_000) break;
    }

    const out = {
      ok: true,
      mode,
      dryRun,
      fetched,
      inserted,
      cursor: await getState(client, cursorId),
      lastUrl,
      ts: nowIso(),
      hint:
        mode === "backfill"
          ? "Call backfill repeatedly (same URL) until fetched stops increasing / pages return 0 rows."
          : "Run is incremental. Call periodically to keep DB fresh.",
    };

    await setState(client, "ingest_last_run", { ...out, finishedAt: nowIso() });

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
