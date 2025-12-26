import { Client } from "pg";

export default async function handler(req, res) {
  if (req.method !== "POST" && req.method !== "GET") {
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

    // sanity check table
    const result = await client.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';`
    );

    await client.end();

    return res.status(200).json({
      ok: true,
      tables: result.rows.map(r => r.table_name)
    });

  } catch (err) {
    return res.status(500).json({
      error: "DB error",
      details: err.message
    });
  }
}
