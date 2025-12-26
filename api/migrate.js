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

async function colExists(client, table, col) {
  const r = await client.query(
    `
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=$1 AND column_name=$2
    LIMIT 1
    `,
    [table, col]
  );
  return r.rowCount > 0;
}

async function ensureTables(client) {
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

  // insider_transactions (create if missing; otherwise we ALTER below)
  await client.query(`
    CREATE TABLE IF NOT EXISTS insider_transactions (
      id TEXT,
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

  // ingestion_state (you already have it; but ensure it exists in a compatible shape)
  // Neon/Vercel integration usually creates: id, value, updated_at (plus some extras)
  await client.query(`
    CREATE TABLE IF NOT EXISTS ingestion_state (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
}

async function ensureInsiderTxColumns(client) {
  const t = "insider_transactions";

  // Make sure required columns exist (ALTER is safe if they already exist)
  const required = [
    ["id", "TEXT"],
    ["cik", "TEXT"],
    ["employer_ticker", "TEXT"],
    ["employer_company", "TEXT"],
    ["purchased_ticker", "TEXT"],
    ["purchased_company", "TEXT"],
    ["insider_name", "TEXT"],
    ["insider_title", "TEXT"],
    ["shares", "NUMERIC"],
    ["price_per_share", "NUMERIC"],
    ["total_value", "NUMERIC"],
    ["transaction_date", "DATE"],
    ["signal_score", "NUMERIC"],
    ["purchase_type", "TEXT"],
    ["raw", "JSONB"],
    ["created_at", "TIMESTAMPTZ"],
    ["updated_at", "TIMESTAMPTZ"],
  ];

  for (const [c, type] of required) {
    const exists = await colExists(client, t, c);
    if (!exists) {
      await client.query(`ALTER TABLE ${t} ADD COLUMN ${c} ${type};`);
    }
  }

  // Backfill id for any existing rows that have NULL id
  // Deterministic md5 is fine and uses built-in Postgres.
  await client.query(`
    UPDATE ${t}
    SET id = md5(
      COALESCE(purchased_ticker,'') || '|' ||
      COALESCE(employer_ticker,'')  || '|' ||
      COALESCE(insider_name,'')     || '|' ||
      COALESCE(insider_title,'')    || '|' ||
      COALESCE(transaction_date::text,'') || '|' ||
      COALESCE(shares::text,'')     || '|' ||
      COALESCE(price_per_share::text,'')
    )
    WHERE id IS NULL OR id = '';
  `);

  // Ensure unique constraint for ON CONFLICT(id)
  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_insider_transactions_id
    ON insider_transactions(id);
  `);

  // Helpful indexes
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_purchased_ticker_date
    ON insider_transactions(purchased_ticker, transaction_date DESC);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_insider_tx_insider_date
    ON insider_transactions(insider_name, transaction_date DESC);
  `);
}

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }
  if (!process.env.DATABASE_URL) {
    return res.status(500).json({ error: "DATABASE_URL missing" });
  }

  const client = await pool.connect();
  try {
    await ensureTables(client);
    await ensureInsiderTxColumns(client);

    const cols = await client.query(`
      SELECT table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='public'
        AND table_name IN ('companies','insider_transactions','ingestion_state')
      ORDER BY table_name, ordinal_position;
    `);

    return res.status(200).json({
      ok: true,
      ts: nowIso(),
      columns: cols.rows,
      hint:
        "Now run: /api/ingest?mode=run&pageSize=50&maxPages=2&days=30&throttleMs=700",
    });
  } catch (e) {
    return res.status(500).json({ error: "Migrate error", details: e?.message || String(e) });
  } finally {
    client.release();
  }
}
