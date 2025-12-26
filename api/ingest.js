import { Pool } from "pg";

const pool =
  globalThis.__INSIDERSCOPE_POOL__ ||
  new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 1, // serverless friendly
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
  // Works on Vercel + local dev
  const proto = (req.headers["x-forwarded-proto"] || "https").toString();
  const host = (req.headers["x-forwarded-host"] || req.headers.host).toString();
  return `${proto}://${host}`;
}

async function ensureSchema(client) {
  // Minimal schema: keeps your existing tables if they already exist.
  // Adds required columns/constraints if missing (safe-ish).
  await client.query(`
    CREATE TABLE IF NOT EXISTS companies (
      cik TEXT PRIMARY KEY,
      ticker TEXT,
      company_name TEXT,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS insider_transactions (
      id TEXT PRIMARY KEY,
      cik TEXT,
      employer_ticker TEXT,
      employer_company TEXT,
      purchased_ticker TEXT,
      purchased_company TEXT,
      insider_name TEXT,
      insider_title TEXT,
      shares NUMERIC,
      price_per_share NUMERIC,
      total_value NUMERIC,
      transaction_date DATE,
      signal_score NUMERIC,
      purchase_type TEXT,
      raw JSONB,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ingestion_state (
      key TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  // Handy index for drilldowns
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_purchased_ticker_date
    ON insider_transactions(purchased_ticker, transaction_date DESC);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_insider_date
    ON insider_transactions(insider_name, transaction_date DESC);
  `);
}

async function getState(client, key) {
  const r = await client.query(
    `SELECT value FROM ingestion_state WHERE key=$1`,
    [key]
  );
  return r.rows?.[0]?.value ?? null;
}

async function setState(client, key, value) {
  await client.query(
    `
    INSERT INTO ingestion_state(key, value, updated_at)
    VALUES($1, $2::jsonb, now())
    ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value,
          updated_at = now()
  `,
    [key, JSON.stringify(value)]
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

  const cursor = await getState(client, "insider_buys_cursor");
  const lastRun = await getState(client, "ingest_last_run");

  return {
    ok: true,
    tables: tables.rows.map((r) => r.table_name),
    counts,
    cursor,
    lastRun,
    ts: nowIso(),
  };
}

async function fetchInsiderBuysFromSelf(req, {
  pageSize,
  page,
  days,
  mode,
  includeGroups,
  debug,
} = {}) {
  const base = getBaseUrl(req);
  const url =
    `${base}/api/insider-buys?wrap=1` +
    `&pageSize=${encodeURIComponent(pageSize)}` +
    `&page=${encodeURIComponent(page)}` +
    `&days=${encodeURIComponent(days)}` +
    (mode === "seed" ? `&mode=seed` : ``) +
    (includeGroups ? `&includeGroups=1` : ``) +
    (debug ? `&debug=1` : ``);

  const r = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`insider-buys HTTP ${r.status} ${txt}`.slice(0, 500));
  }

  const payload = await r.json();

  // expected: { data: [], meta: {...}, groups?: [] }
  const data = Array.isArray(payload) ? payload : payload?.data;
  const meta = Array.isArray(payload) ? null : payload?.meta;

  return { url, data: Array.isArray(data) ? data : [], meta, payload };
}

async function upsertBatch(client, rows) {
  let inserted = 0;

  // Upsert transactions
  for (const item of rows) {
    const id = String(item.id ?? "");
    if (!id) continue;

    const purchasedTicker = item.purchasedTicker ? String(item.purchasedTicker) : null;
    const purchasedCompany = item.purchasedCompany ? String(item.purchasedCompany) : null;
    const employerTicker = item.employerTicker ? String(item.employerTicker) : null;
    const employerCompany = item.employerCompany ? String(item.employerCompany) : null;

    // best-effort CIK
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

    // Upsert company record (best-effort)
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

    inserted += 1;
  }

  return inserted;
}

export default async function handler(req, res) {
  // allow GET for ease of use (and browser testing)
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  if (!process.env.DATABASE_URL) {
    return res.status(500).json({ error: "DATABASE_URL missing" });
  }

  const mode = String(req.query.mode || "status"); // status | run | backfill
  const dryRun = bool(req.query.dryRun, false);

  // knobs to avoid 429 + timeouts
  const pageSize = num(req.query.pageSize, 50);        // how many rows per page from insider-buys
  const maxPages = num(req.query.maxPages, 3);         // pages per invocation
  const throttleMs = num(req.query.throttleMs, 600);   // sleep between pages
  const days = num(req.query.days, 30);
  const dataMode = String(req.query.dataMode || "live"); // live | seed
  const includeGroups = bool(req.query.includeGroups, true);

  const client = await pool.connect();

  try {
    await ensureSchema(client);

    // STATUS
    if (mode === "status") {
      const out = await statusResponse(client);
      return res.status(200).json(out);
    }

    // RUN (incremental)
    // Uses a cursor stored in ingestion_state to continue where it left off.
    if (mode === "run") {
      const cursorKey = "insider_buys_cursor";
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
          const ins = await upsertBatch(client, data);
          totalInserted += ins;
        }

        // save cursor after each page
        await setState(client, cursorKey, {
          page: page + 1,
          pageSize,
          days,
          dataMode,
          updatedAt: nowIso(),
          lastMeta: meta || null,
        });

        // stop early if no rows
        if (data.length === 0) break;

        page += 1;
        if (throttleMs > 0) await sleep(throttleMs);

        // guardrails for serverless execution time
        if (Date.now() - startedAt > 22_000) break;
      }

      const runInfo = {
        ok: true,
        mode: "run",
        dryRun,
        fetched: totalFetched,
        inserted: totalInserted,
        cursor: await getState(client, cursorKey),
        lastUrl,
        ts: nowIso(),
      };

      await setState(client, "ingest_last_run", {
        ...runInfo,
        finishedAt: nowIso(),
      });

      return res.status(200).json(runInfo);
    }

    // BACKFILL (manual only)
    // Runs the same paged ingestion but you can crank up days + pages intentionally.
    if (mode === "backfill") {
      // recommended: call manually with days=365&maxPages=10&pageSize=100&throttleMs=900
      const cursorKey = "insider_buys_backfill_cursor";
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
          const ins = await upsertBatch(client, data);
          totalInserted += ins;
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

        if (Date.now() - startedAt > 22_000) break;
      }

      const out = {
        ok: true,
        mode: "backfill",
        dryRun,
        fetched: totalFetched,
        inserted: totalInserted,
        cursor: await getState(client, cursorKey),
        lastUrl,
        ts: nowIso(),
        hint:
          "Backfill is meant to be called repeatedly. Keep calling until fetched stops increasing / page returns 0 rows.",
      };

      await setState(client, "ingest_last_run", {
        ...out,
        finishedAt: nowIso(),
      });

      return res.status(200).json(out);
    }

    return res.status(400).json({
      error: "Unknown mode",
      allowed: ["status", "run", "backfill"],
    });
  } catch (err) {
    const msg = err?.message || String(err);
    try {
      await setState(client, "ingest_last_run", {
        ok: false,
        mode,
        error: msg,
        ts: nowIso(),
      });
    } catch {}
    return res.status(500).json({ error: "Ingest error", details: msg });
  } finally {
    client.release();
  }
}
