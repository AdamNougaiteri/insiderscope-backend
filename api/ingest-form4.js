import { Client } from "pg";
import { fetchRecentFilings } from "../lib/secDataServices";

export default async function handler(req, res) {
  const dryRun = req.query.dryRun === "1";

  if (!process.env.DATABASE_URL) {
    return res.status(500).json({ error: "DATABASE_URL not set" });
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();

  try {
    // Ensure ingestion_state table exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS ingestion_state (
        id SERIAL PRIMARY KEY,
        last_processed_date DATE
      )
    `);

    const stateRes = await client.query(
      `SELECT last_processed_date FROM ingestion_state ORDER BY id DESC LIMIT 1`
    );

    const sinceDate =
      stateRes.rows[0]?.last_processed_date ||
      new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);

    const filings = await fetchRecentFilings({ sinceDate, limit: 50 });

    let inserts = 0;

    for (const filing of filings) {
      if (dryRun) continue;

      const exists = await client.query(
        `SELECT 1 FROM insider_transactions WHERE id = $1`,
        [filing.id]
      );

      if (exists.rowCount) continue;

      await client.query(
        `
        INSERT INTO insider_transactions
        (id, insider_name, insider_title, employer_ticker, purchased_ticker,
         shares, price_per_share, total_value, transaction_date, purchase_type)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        `,
        [
          filing.id,
          filing.insiderName,
          filing.insiderTitle,
          filing.employerTicker,
          filing.purchasedTicker,
          filing.shares,
          filing.pricePerShare,
          filing.totalValue,
          filing.transactionDate,
          filing.purchaseType,
        ]
      );

      inserts++;
    }

    if (!dryRun && filings.length) {
      await client.query(
        `INSERT INTO ingestion_state (last_processed_date) VALUES ($1)`,
        [new Date().toISOString().slice(0, 10)]
      );
    }

    res.json({
      ok: true,
      dryRun,
      filingsFetched: filings.length,
      inserts,
      sinceDate,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    await client.end();
  }
}
