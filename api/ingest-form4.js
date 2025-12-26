// /api/ingest-form4.js
import { XMLParser } from "fast-xml-parser";
import pkg from "pg";

const { Client } = pkg;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const toInt = (v, d) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
const safeText = (v) => (v === null || v === undefined ? "" : String(v).trim());
const isoDateOnly = (s) => (s ? String(s).slice(0, 10) : null);

function stripNamespaces(xml) {
  if (!xml) return "";
  let out = xml.replace(/\sxmlns(:\w+)?="[^"]*"/g, "");
  out = out.replace(/<(\/*)\w+:(\w+)([^>]*)>/g, "<$1$2$3>");
  return out;
}

function ensureArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

function deepFindAll(obj, keyName, out = []) {
  if (!obj || typeof obj !== "object") return out;
  if (Object.prototype.hasOwnProperty.call(obj, keyName)) out.push(obj[keyName]);
  for (const k of Object.keys(obj)) deepFindAll(obj[k], keyName, out);
  return out;
}

function pickArchivesLink(entry) {
  const links = entry?.link;
  if (!links) return "";
  const arr = Array.isArray(links) ? links : [links];
  const hrefs = arr.map((l) => safeText(l?.href)).filter(Boolean);
  return hrefs.find((h) => h.includes("/Archives/")) || hrefs[0] || "";
}

function extractCikAndAccession(url) {
  const u = safeText(url);
  // matches: .../Archives/edgar/data/{cik}/{accessionNoNoDash or dashed}/...
  const m = u.match(/edgar\/data\/(\d+)\/(\d{18}|\d{10}-\d{2}-\d{6})/i);
  if (!m) return null;

  const cik = m[1];
  const acc = m[2];

  const accessionNoDashed = acc.includes("-")
    ? acc
    : `${acc.slice(0, 10)}-${acc.slice(10, 12)}-${acc.slice(12)}`;

  const accessionNoNoDash = accessionNoDashed.replace(/-/g, "");
  return { cik, accessionNoDashed, accessionNoNoDash };
}

function indexJsonUrl(cik, accessionNoNoDash) {
  return `https://www.sec.gov/Archives/edgar/data/${Number(
    cik
  )}/${accessionNoNoDash}/index.json`;
}

function pickForm4Xml(indexJson) {
  const files = indexJson?.directory?.item || [];
  const arr = Array.isArray(files) ? files : [files].filter(Boolean);

  const xmls = arr
    .map((f) => safeText(f?.name))
    .filter(Boolean)
    .filter((n) => n.toLowerCase().endsWith(".xml"));

  return (
    xmls.find((n) => n.toLowerCase().includes("form4")) ||
    xmls.find((n) => n.toLowerCase().includes("primary")) ||
    xmls.find((n) => n.toLowerCase().includes("ownership")) ||
    xmls[0] ||
    null
  );
}

function parseAtom(xml) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
  });
  const doc = parser.parse(xml);
  const feed = doc?.feed;
  let entries = feed?.entry || [];
  if (!Array.isArray(entries)) entries = [entries].filter(Boolean);
  return entries;
}

function numVal(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function valueField(node) {
  if (node && typeof node === "object" && "value" in node) return node.value;
  return node;
}

/**
 * Parses Form4 XML and returns only NON-DERIVATIVE PURCHASES (transactionCode === "P")
 * Filters out obvious non-meaningful lines:
 * - missing shares/price
 * - price <= 0 (zero-priced lines often exercises/grants)
 */
function parseForm4Purchases(xmlTextRaw) {
  const xmlText = stripNamespaces(xmlTextRaw);
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
  });

  let doc;
  try {
    doc = parser.parse(xmlText);
  } catch {
    return [];
  }

  const root = doc?.ownershipDocument || doc;

  const issuerTradingSymbol =
    safeText(root?.issuer?.issuerTradingSymbol) ||
    safeText(root?.issuerTradingSymbol) ||
    safeText(root?.issuer?.tradingSymbol) ||
    "";

  const issuerName =
    safeText(root?.issuer?.issuerName) || safeText(root?.issuerName) || "";

  const ownerName =
    safeText(root?.reportingOwner?.reportingOwnerId?.rptOwnerName) ||
    safeText(root?.rptOwnerName) ||
    safeText(root?.reportingOwnerName) ||
    "";

  const officerTitle =
    safeText(root?.reportingOwner?.reportingOwnerRelationship?.officerTitle) ||
    safeText(root?.officerTitle) ||
    "";

  let txs =
    root?.nonDerivativeTable?.nonDerivativeTransaction ??
    root?.nonDerivativeTransaction ??
    null;

  txs = ensureArray(txs);

  if (txs.length === 0) {
    const hits = deepFindAll(root, "nonDerivativeTransaction");
    for (const h of hits) txs.push(...ensureArray(h));
  }

  const purchases = [];

  for (const t of txs) {
    const code = safeText(
      valueField(t?.transactionCoding?.transactionCode) ??
        t?.transactionCode ??
        ""
    );
    if (code !== "P") continue;

    const shares = numVal(
      valueField(t?.transactionAmounts?.transactionShares) ??
        valueField(t?.transactionShares) ??
        null
    );

    const price = numVal(
      valueField(t?.transactionAmounts?.transactionPricePerShare) ??
        valueField(t?.transactionPricePerShare) ??
        null
    );

    const date =
      isoDateOnly(
        valueField(t?.transactionDate) ??
          valueField(t?.transactionDate?.value) ??
          null
      ) || null;

    if (!Number.isFinite(shares) || !Number.isFinite(price)) continue;
    if (price <= 0) continue;

    purchases.push({
      issuerTradingSymbol,
      issuerName,
      ownerName,
      officerTitle,
      shares,
      pricePerShare: price,
      transactionDate: date,
      totalValue: Math.round(shares * price),
    });
  }

  return purchases;
}

async function fetchAtomPage({ start, headers }) {
  const url =
    `https://www.sec.gov/cgi-bin/browse-edgar?` +
    `action=getcurrent&type=4&count=100&start=${start}&output=atom`;
  const resp = await fetch(url, { headers });
  return { resp, url };
}

function getDbUrl() {
  return (
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    process.env.DATABASE_URL_UNPOOLED ||
    process.env.POSTGRES_URL_NON_POOLING ||
    ""
  );
}

async function withClient(fn) {
  const connectionString = getDbUrl();
  if (!connectionString) {
    throw new Error(
      "Missing DATABASE_URL/POSTGRES_URL env var. Add your Neon connection string to Vercel env."
    );
  }

  const client = new Client({
    connectionString,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

async function ensureSchema(client) {
  // safe if tables already exist
  await client.query(`
    CREATE TABLE IF NOT EXISTS companies (
      ticker TEXT PRIMARY KEY,
      cik TEXT,
      company_name TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS insider_transactions (
      id TEXT PRIMARY KEY,
      cik TEXT,
      accession_no TEXT,
      issuer_ticker TEXT,
      issuer_name TEXT,
      insider_name TEXT,
      insider_title TEXT,
      transaction_date DATE,
      shares NUMERIC,
      price_per_share NUMERIC,
      total_value NUMERIC,
      transaction_code TEXT,
      source_url TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ingestion_state (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // helpful indexes (safe to run; IF NOT EXISTS supported for indexes on newer PG;
  // if it errors on your instance, it's non-fatal and we ignore)
  try {
    await client.query(
      `CREATE INDEX IF NOT EXISTS idx_insider_tx_issuer_date ON insider_transactions (issuer_ticker, transaction_date DESC);`
    );
    await client.query(
      `CREATE INDEX IF NOT EXISTS idx_insider_tx_insider_date ON insider_transactions (insider_name, transaction_date DESC);`
    );
  } catch {}
}

async function upsertCompany(client, { ticker, cik, companyName }) {
  if (!ticker) return;
  await client.query(
    `
    INSERT INTO companies (ticker, cik, company_name, updated_at)
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT (ticker)
    DO UPDATE SET
      cik = COALESCE(EXCLUDED.cik, companies.cik),
      company_name = COALESCE(EXCLUDED.company_name, companies.company_name),
      updated_at = NOW()
  `,
    [ticker, cik || null, companyName || null]
  );
}

async function upsertTransaction(client, row) {
  // id should be deterministic enough to avoid dupes across re-runs
  await client.query(
    `
    INSERT INTO insider_transactions (
      id, cik, accession_no, issuer_ticker, issuer_name, insider_name, insider_title,
      transaction_date, shares, price_per_share, total_value, transaction_code, source_url
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,
      $8,$9,$10,$11,$12,$13
    )
    ON CONFLICT (id) DO NOTHING
  `,
    [
      row.id,
      row.cik || null,
      row.accessionNoDashed || null,
      row.issuerTicker || null,
      row.issuerName || null,
      row.insiderName || null,
      row.insiderTitle || null,
      row.transactionDate || null,
      row.shares ?? null,
      row.pricePerShare ?? null,
      row.totalValue ?? null,
      row.transactionCode || "P",
      row.sourceUrl || null,
    ]
  );
}

function makeDeterministicId({ cik, accessionNoDashed, insiderName, transactionDate, shares, pricePerShare }) {
  // Keep it stable across re-runs
  const base = [
    safeText(cik),
    safeText(accessionNoDashed),
    safeText(insiderName).toLowerCase(),
    safeText(transactionDate),
    String(shares ?? ""),
    String(pricePerShare ?? ""),
    "P",
  ].join("|");

  // cheap hash
  let h = 0;
  for (let i = 0; i < base.length; i++) h = (h * 31 + base.charCodeAt(i)) >>> 0;
  return `tx_${h.toString(16)}`;
}

export default async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

  // --- SEC header requirements
  const SEC_UA = process.env.SEC_USER_AGENT;
  if (!SEC_UA) {
    return res.status(500).json({
      error: "Missing SEC_USER_AGENT env var",
      hint: 'Set SEC_USER_AGENT like: "InsiderScope (your_email@example.com)"',
    });
  }

  // --- knobs (keep default conservative so it won't time out)
  const days = clamp(toInt(req.query.days, 30), 1, 365);
  const pages = clamp(toInt(req.query.pages, 1), 1, 3);
  const scanCap = clamp(toInt(req.query.scan, 25), 5, 60);
  const betweenCallsMs = clamp(toInt(req.query.throttle, 350), 200, 900);

  // “budget” must stay below Vercel function time limits.
  const TIME_BUDGET_MS = clamp(toInt(req.query.budget, 8000), 2500, 9000);

  const dryRun = String(req.query.dryRun || "") === "1";

  const startedAt = Date.now();
  const timeLeft = () => TIME_BUDGET_MS - (Date.now() - startedAt);

  const headers = {
    "User-Agent": SEC_UA,
    Accept: "application/atom+xml,application/xml,text/xml,*/*",
    "Accept-Encoding": "identity",
  };

  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;

  try {
    const result = await withClient(async (client) => {
      await ensureSchema(client);

      let entriesSeenTotal = 0;
      let filingsTried = 0;
      let txFound = 0;
      let inserted = 0;

      const errorsSample = [];
      const examples = [];

      // Basic incremental marker (optional)
      const stateKey = "last_ingest_run";
      if (!dryRun) {
        await client.query(
          `
          INSERT INTO ingestion_state (key, value, updated_at)
          VALUES ($1, $2, NOW())
          ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        `,
          [stateKey, new Date().toISOString()]
        );
      }

      for (let p = 0; p < pages; p++) {
        if (timeLeft() < 1500) break;

        const start = p * 100;
        let atomXml = "";
        let atomUrl = "";

        try {
          if (p > 0) await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1200)));
          const { resp, url } = await fetchAtomPage({ start, headers });
          atomUrl = url;

          if (resp.status === 429) {
            errorsSample.push({ where: "atom", status: 429, atomUrl });
            break;
          }
          if (!resp.ok) {
            errorsSample.push({ where: "atom", status: resp.status, atomUrl });
            continue;
          }
          atomXml = await resp.text();
        } catch (e) {
          errorsSample.push({ where: "atom", error: String(e), atomUrl });
          continue;
        }

        const entries = parseAtom(atomXml);
        entriesSeenTotal += entries.length;

        const recentEntries = entries
          .map((en) => {
            const linkHref = pickArchivesLink(en);
            const updated = safeText(en?.updated || en?.published || "");
            const ts = updated ? Date.parse(updated) : 0;
            return { linkHref, updated, ts };
          })
          .filter((x) => x.linkHref && x.ts && x.ts >= cutoff)
          .slice(0, scanCap);

        for (let i = 0; i < recentEntries.length; i++) {
          if (timeLeft() < 1500) break;

          filingsTried++;
          const { linkHref, updated } = recentEntries[i];
          const meta = extractCikAndAccession(linkHref);
          if (!meta) continue;

          try {
            await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1200)));

            const idxUrl = indexJsonUrl(meta.cik, meta.accessionNoNoDash);
            const idxResp = await fetch(idxUrl, { headers });
            if (!idxResp.ok) {
              errorsSample.push({ where: "index.json", status: idxResp.status, idxUrl });
              continue;
            }

            const idxJson = await idxResp.json();
            const xmlName = pickForm4Xml(idxJson);
            if (!xmlName) continue;

            const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${Number(
              meta.cik
            )}/${meta.accessionNoNoDash}/${xmlName}`;

            await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1200)));

            const xmlResp = await fetch(xmlUrl, { headers });
            if (!xmlResp.ok) {
              errorsSample.push({ where: "form4.xml", status: xmlResp.status, xmlUrl });
              continue;
            }

            const xmlText = await xmlResp.text();
            const purchases = parseForm4Purchases(xmlText);
            txFound += purchases.length;

            if (examples.length < 5) {
              examples.push({
                cik: meta.cik,
                accession: meta.accessionNoDashed,
                xmlName,
                purchasesFound: purchases.length,
                xmlUrl,
              });
            }

            for (const pch of purchases) {
              const transactionDate = pch.transactionDate || isoDateOnly(updated) || null;
              const issuerTicker = pch.issuerTradingSymbol || "";
              const issuerName = pch.issuerName || "";
              const insiderName = pch.ownerName || "";
              const insiderTitle = pch.officerTitle || "";

              if (!issuerTicker || !insiderName || !transactionDate) continue;

              const id = makeDeterministicId({
                cik: meta.cik,
                accessionNoDashed: meta.accessionNoDashed,
                insiderName,
                transactionDate,
                shares: pch.shares,
                pricePerShare: pch.pricePerShare,
              });

              if (!dryRun) {
                await upsertCompany(client, {
                  ticker: issuerTicker,
                  cik: meta.cik,
                  companyName: issuerName,
                });

                await upsertTransaction(client, {
                  id,
                  cik: meta.cik,
                  accessionNoDashed: meta.accessionNoDashed,
                  issuerTicker,
                  issuerName,
                  insiderName,
                  insiderTitle,
                  transactionDate,
                  shares: pch.shares,
                  pricePerShare: pch.pricePerShare,
                  totalValue: pch.totalValue,
                  transactionCode: "P",
                  sourceUrl: xmlUrl,
                });
              }

              inserted++;
            }
          } catch (e) {
            errorsSample.push({ where: "loop", error: String(e) });
          }
        }
      }

      return {
        ok: true,
        dryRun,
        days,
        pages,
        scanCap,
        throttle: betweenCallsMs,
        budget: TIME_BUDGET_MS,
        timeSpentMs: Date.now() - startedAt,
        cutoff: new Date(cutoff).toISOString(),
        stats: {
          entriesSeen: entriesSeenTotal,
          filingsTried,
          purchasesFound: txFound,
          rowsInsertedOrAttempted: inserted,
        },
        examples,
        errorsSample,
      };
    });

    return res.status(200).json(result);
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e?.message || String(e),
    });
  }
}
