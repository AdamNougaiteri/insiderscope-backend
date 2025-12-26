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

export default async function handler(req, res) {
  // Allow GET so you can click it in browser
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const client = await pool.connect();
  try {
    // Add columns that ingest expects (safe if they already exist)
    await client.query(`
      ALTER TABLE IF EXISTS insider_transactions
        ADD COLUMN IF NOT EXISTS cik TEXT,
        ADD COLUMN IF NOT EXISTS employer_ticker TEXT,
        ADD COLUMN IF NOT EXISTS employer_company TEXT,
        ADD COLUMN IF NOT EXISTS purchased_ticker TEXT,
        ADD COLUMN IF NOT EXISTS purchased_company TEXT,
        ADD COLUMN IF NOT EXISTS insider_name TEXT,
        ADD COLUMN IF NOT EXISTS insider_title TEXT,
        ADD COLUMN IF NOT EXISTS shares NUMERIC,
        ADD COLUMN IF NOT EXISTS price_per_share NUMERIC,
        ADD COLUMN IF NOT EXISTS total_value NUMERIC,
        ADD COLUMN IF NOT EXISTS transaction_date DATE,
        ADD COLUMN IF NOT EXISTS signal_score NUMERIC,
        ADD COLUMN IF NOT EXISTS purchase_type TEXT,
        ADD COLUMN IF NOT EXISTS raw JSONB,
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now(),
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
    `);

    // If the table exists but doesn't have a primary key on id, add it safely:
    // (This will fail if duplicate ids exist; that's okay—just tell me and we’ll handle.)
    try {
      await client.query(`
        ALTER TABLE insider_transactions
        ADD CONSTRAINT insider_transactions_pkey PRIMARY KEY (id);
      `);
    } catch (e) {
      // ignore if already exists or can't be added
    }

    // Companies table expected by ingest
    await client.query(`
      CREATE TABLE IF NOT EXISTS companies (
        cik TEXT PRIMARY KEY,
        ticker TEXT,
        company_name TEXT,
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ DEFAULT now()
      );
    `);

    // Ingestion state expected by ingest
    await client.query(`
      CREATE TABLE IF NOT EXISTS ingestion_state (
        key TEXT PRIMARY KEY,
        value JSONB NOT NULL,
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

    return res.status(200).json({ ok: true });
  } catch (err) {
    return res.status(500).json({
      error: "Migration error",
      details: err?.message || String(err),
    });
  } finally {
    client.release();
  }
}
