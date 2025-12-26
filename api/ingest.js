// api/ingest.js
import { XMLParser } from "fast-xml-parser";
import { pool, requireDb } from "../lib/db.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const safeText = (v) => (v === null || v === undefined ? "" : String(v).trim());
const isoDateOnly = (s) => (s ? String(s).slice(0, 10) : null);
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));

function ensureArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

function stripNamespaces(xml) {
  if (!xml) return "";
  let out = xml.replace(/\sxmlns(:\w+)?="[^"]*"/g, "");
  out = out.replace(/<(\/*)\w+:(\w+)([^>]*)>/g, "<$1$2$3>");
  return out;
}

function parseAtom(xml) {
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
  const doc = parser.parse(xml);
  const feed = doc?.feed;
  let entries = feed?.entry || [];
  if (!Array.isArray(entries)) entries = [entries].filter(Boolean);
  return entries;
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
  return `https://www.sec.gov/Archives/edgar/data/${Number(cik)}/${accessionNoNoDash}/index.json`;
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

function deepFindAll(obj, keyName, out = []) {
  if (!obj || typeof obj !== "object") return out;
  if (Object.prototype.hasOwnProperty.call(obj, keyName)) out.push(obj[keyName]);
  for (const k of Object.keys(obj)) deepFindAll(obj[k], keyName, out);
  return out;
}

function parseForm4Purchases(xmlTextRaw) {
  const xmlText = stripNamespaces(xmlTextRaw);
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });

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

  // non-derivative transactions
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

    // only "P" (Purchase)
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

export default async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

  // OPTIONAL: lock this down (recommended)
  const required = process.env.INGEST_SECRET;
  if (required) {
    const got = req.headers["x-ingest-secret"] || req.query.secret;
    if (got !== required) {
      return res.status(401).json({ error: "Unauthorized (missing/invalid INGEST_SECRET)" });
    }
  }

  try {
    requireDb();
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }

  const SEC_UA = process.env.SEC_USER_AGENT;
  if (!SEC_UA) {
    return res.status(500).json({
      error: "Missing SEC_USER_AGENT env var",
      hint: 'Set SEC_USER_AGENT like: "InsiderScope (your_email@example.com)"',
    });
  }

  // Keep each run small to avoid timeouts/429.
  const days = clamp(Number(req.query.days || 30), 1, 3650);
  const pages = clamp(Number(req.query.pages || 1), 1, 2);         // atom pages
  const scanCap = clamp(Number(req.query.scan || 25), 5, 60);      // entries per page
  const throttle = clamp(Number(req.query.throttle || 350), 200, 900);
  const maxPurchases = clamp(Number(req.query.max || 100), 10, 300);

  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;

  const headers = {
    "User-Agent": SEC_UA,
    Accept: "application/atom+xml,application/xml,text/xml,*/*",
    "Accept-Encoding": "identity",
  };

  let entriesSeenTotal = 0;
  let purchasesInserted = 0;
  let purchasesFound = 0;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Make sure ingestion_state exists & read cursor
    await client.query(`
      CREATE TABLE IF NOT EXISTS ingestion_state (
        id INTEGER PRIMARY KEY DEFAULT 1,
        atom_start INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await client.query(`
      INSERT INTO ingestion_state (id, atom_start)
      VALUES (1, 0)
      ON CONFLICT (id) DO NOTHING;
    `);

    const state = await client.query(`SELECT atom_start FROM ingestion_state WHERE id=1;`);
    let atomStart = Number(state.rows?.[0]?.atom_start || 0);

    // Tables (if you already created them, these are no-ops)
    await client.query(`
      CREATE TABLE IF NOT EXISTS companies (
        cik TEXT PRIMARY KEY,
        ticker TEXT,
        company_name TEXT
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS insider_transactions (
        id TEXT PRIMARY KEY,
        cik TEXT,
        ticker TEXT,
        company_name TEXT,
        insider_name TEXT,
        insider_title TEXT,
        transaction_date DATE,
        shares NUMERIC,
        price_per_share NUMERIC,
        total_value NUMERIC,
        source TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    // Ingest loop (advances atomStart cursor)
    for (let p = 0; p < pages; p++) {
      const start = atomStart + p * 100;

      await sleep(throttle);

      const { resp } = await fetchAtomPage({ start, headers });
      if (resp.status === 429) break;
      if (!resp.ok) continue;

      const atomXml = await resp.text();
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

      for (const en of recentEntries) {
        if (purchasesInserted >= maxPurchases) break;

        const meta = extractCikAndAccession(en.linkHref);
        if (!meta) continue;

        await sleep(throttle);

        const idxUrl = indexJsonUrl(meta.cik, meta.accessionNoNoDash);
        const idxResp = await fetch(idxUrl, { headers });
        if (!idxResp.ok) continue;

        const idxJson = await idxResp.json();
        const xmlName = pickForm4Xml(idxJson);
        if (!xmlName) continue;

        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${Number(meta.cik)}/${meta.accessionNoNoDash}/${xmlName}`;

        await sleep(throttle);

        const xmlResp = await fetch(xmlUrl, { headers });
        if (!xmlResp.ok) continue;

        const xmlText = await xmlResp.text();
        const purchases = parseForm4Purchases(xmlText);

        purchasesFound += purchases.length;

        for (const pch of purchases) {
          if (purchasesInserted >= maxPurchases) break;

          const dt = pch.transactionDate || isoDateOnly(en.updated);
          const ticker = pch.issuerTradingSymbol || null;
          const companyName = pch.issuerName || null;

          // Stable-ish id
          const id = `${meta.cik}-${ticker || "NA"}-${pch.ownerName || "NA"}-${dt || "NA"}-${pch.shares}-${pch.pricePerShare}`;

          // Upsert company
          await client.query(
            `
            INSERT INTO companies (cik, ticker, company_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (cik) DO UPDATE SET
              ticker = COALESCE(EXCLUDED.ticker, companies.ticker),
              company_name = COALESCE(EXCLUDED.company_name, companies.company_name)
          `,
            [meta.cik, ticker, companyName]
          );

          // Insert transaction (dedupe by primary key)
          const ins = await client.query(
            `
            INSERT INTO insider_transactions
              (id, cik, ticker, company_name, insider_name, insider_title, transaction_date, shares, price_per_share, total_value, source)
            VALUES
              ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
          `,
            [
              id,
              meta.cik,
              ticker,
              companyName,
              pch.ownerName || null,
              pch.officerTitle || null,
              dt,
              pch.shares,
              pch.pricePerShare,
              pch.totalValue,
              "sec-form4",
            ]
          );

          if (ins.rowCount === 1) purchasesInserted += 1;
        }
      }

      if (purchasesInserted >= maxPurchases) break;
    }

    // Advance cursor for next run (so repeated calls gradually build history)
    atomStart = atomStart + pages * 100;
    await client.query(
      `UPDATE ingestion_state SET atom_start=$1, updated_at=now() WHERE id=1;`,
      [atomStart]
    );

    await client.query("COMMIT");

    return res.status(200).json({
      ok: true,
      inserted: purchasesInserted,
      purchasesFound,
      entriesSeen: entriesSeenTotal,
      nextAtomStart: atomStart,
      note:
        "Run /api/ingest repeatedly to build history slowly without 429/timeouts. Increase days/pages/scan/max carefully.",
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    return res.status(500).json({ error: String(e?.message || e) });
  } finally {
    client.release();
  }
}
