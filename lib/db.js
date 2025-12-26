// lib/db.js
import pg from "pg";

const { Pool } = pg;

// Vercel/Neon often provides DATABASE_URL already.
// We also accept POSTGRES_URL if needed.
function getConnStr() {
  const raw =
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    "";

  if (!raw) return "";

  // Defensive cleanup if someone pasted "psql 'postgresql://...'"
  let s = String(raw).trim();
  if (s.startsWith("psql")) {
    s = s.replace(/^psql\s+/i, "").trim();
  }
  if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith('"') && s.endsWith('"'))) {
    s = s.slice(1, -1);
  }
  return s.trim();
}

const connectionString = getConnStr();

export const pool = connectionString
  ? new Pool({
      connectionString,
      ssl: { rejectUnauthorized: false }, // Neon needs SSL
      max: 1, // keep small for serverless
    })
  : null;

export function requireDb() {
  if (!pool) {
    throw new Error(
      "Missing DATABASE_URL (or POSTGRES_URL). Check Vercel env vars / Neon integration."
    );
  }
}
