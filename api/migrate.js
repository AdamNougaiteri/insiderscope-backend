import { Client } from "pg";

export default async function handler(req, res) {
  // Allow GET/POST from anywhere (simple)
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  if (!process.env.DATABASE_URL) {
    return res.status(500).json({ error: "DATABASE_URL missing" });
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();

    // --- companies ---
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
      CREATE INDEX IF NOT EXISTS idx_companies_ticker ON companies (ticker);
    `);

    // --- insider_transactions ---
    // This schema matches the fields your UI expects + common ingest needs.
    await client.query(`
      CREATE TABLE IF NOT EXISTS insider_transactions (
        id TEXT PRIMARY KEY,

        insider_name TEXT,
        insider_title TEXT,

        employer_ticker TEXT,
        employer_company TEXT,

        purchased_ticker TEXT,
        purchased_company TEXT,

        shares BIGINT,
        price_per_share NUMERIC,
        total_value NUMERIC,

        transaction_date DATE,
        signal_score NUMERIC,
        purchase_type TEXT,

        source TEXT,        -- optional: "sec", "seed"
        filing_url TEXT,    -- optional: link to SEC filing
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ DEFAULT now()
      );
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_insider_tx_purchased_ticker ON insider_transactions (purchased_ticker);
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_insider_tx_employer_ticker ON insider_transactions (employer_ticker);
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_insider_tx_date ON insider_transactions (transaction_date DESC);
    `);

    // --- ingestion_state ---
    // This fixes your error: column "value" does not exist
    // and is safe even if the table already existed.
    await client.query(`
      CREATE TABLE IF NOT EXISTS ingestion_state (
        key TEXT PRIMARY KEY
      );
    `);

    await client.query(`
      ALTER TABLE ingestion_state
        ADD COLUMN IF NOT EXISTS value JSONB,
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
    `);

    // sanity: return table list + ingestion_state columns so you can confirm
    const tables = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema='public'
      ORDER BY table_name;
    `);

    const ingestionCols = await client.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='public'
        AND table_name='ingestion_state'
      ORDER BY ordinal_position;
    `);

    await client.end();

    return res.status(200).json({
      ok: true,
      tables: tables.rows.map((r) => r.table_name),
      ingestion_state_columns: ingestionCols.rows
    });
  } catch (err) {
    try { await client.end(); } catch {}
    return res.status(500).json({
      error: "Migration failed",
      details: err?.message || String(err)
    });
  }
}
