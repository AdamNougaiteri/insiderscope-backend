// /api/migrate.js
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

export default async function handler(req, res) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method not allowed" });
  if (!process.env.DATABASE_URL) return res.status(500).json({ error: "DATABASE_URL missing" });

  const client = await pool.connect();
  try {
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
      CREATE INDEX IF NOT EXISTS idx_insider_tx_ticker_date
      ON insider_transactions(purchased_ticker, transaction_date DESC);
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_insider_tx_cik_date
      ON insider_transactions(cik, transaction_date DESC);
    `);

    const columns = await client.query(`
      SELECT table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='public'
      ORDER BY table_name, ordinal_position;
    `);

    return res.status(200).json({
      ok: true,
      ts: nowIso(),
      columns: columns.rows,
      hint: "Now run: /api/ingest?mode=run&pageSize=50&maxPages=2&days=30&throttleMs=700",
    });
  } catch (e) {
    return res.status(500).json({ error: "Migration error", details: e.message });
  } finally {
    client.release();
  }
}
