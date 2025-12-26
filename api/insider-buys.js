// /api/insider-buys.js
// Cached + throttled SEC Atom fetcher for insider buys (Form 4).
// Works on Vercel Serverless. Uses in-memory cache (best-effort) + Vercel edge caching.

import { XMLParser } from "fast-xml-parser";

// --- tiny utils ---
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function toInt(v, d) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function isoDateOnly(s) {
  if (!s) return null;
  // Handles "2025-12-22T..." or "2025-12-22"
  return String(s).slice(0, 10);
}

function safeText(v) {
  if (v === null || v === undefined) return "";
  return String(v).trim();
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

// Extract accession number + CIK from an SEC archive URL
// Example: https://www.sec.gov/Archives/edgar/data/320193/000032019325000123/...
function extractCikAndAccession(url) {
  const u = safeText(url);
  const m = u.match(/edgar\/data\/(\d+)\/(\d{18,})/i);
  if (!m) return null;
  const cik = m[1];
  const accessionNoRaw = m[2];
  // Convert 000032019325000123 -> 0000320193-25-000123 (SEC format)
  const accessionNo =
    accessionNoRaw.length === 18
      ? `${accessionNoRaw.slice(0, 10)}-${accessionNoRaw.slice(
          10,
          12
        )}-${accessionNoRaw.slice(12)}`
      : accessionNoRaw;
  return { cik, accessionNo };
}

// Build SEC "index.json" URL for filing directory
function indexJsonUrl(cik, accessionNoRawOrDashed) {
  const dashed = accessionNoRawOrDashed.includes("-")
    ? accessionNoRawOrDashed
    : accessionNoRawOrDashed;
  const nodash = dashed.replace(/-/g, "");
  return `https://data.sec.gov/Archives/edgar/data/${Number(
    cik
  )}/${nodash}/index.json`;
}

// Find likely Form 4 XML file inside index.json listing
function pickForm4Xml(indexJson) {
  const files = indexJson?.directory?.item || [];
  const arr = Array.isArray(files) ? files : [files].filter(Boolean);

  // Prefer something that looks like "form4.xml" or contains "primary_doc.xml"
  const candidates = arr
    .map((f) => safeText(f?.name))
    .filter(Boolean)
    .filter((name) => name.toLowerCase().endsWith(".xml"));

  const preferred =
    candidates.find((n) => n.toLowerCase().includes("form4")) ||
    candidates.find((n) => n.toLowerCase().includes("primary")) ||
    candidates[0];

  return preferred || null;
}

// Minimal parse: pull issuer, reporting owner, and NON-derivative purchases ("P")
function parseForm4Xml(xmlText) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
  });
  const doc = parser.parse(xmlText);

  // XML shapes vary; be defensive.
  const ownershipDocument =
    doc?.ownershipDocument || doc?.document || doc || {};

  const issuer = ownershipDocument?.issuer || {};
  const reportingOwner = ownershipDocument?.reportingOwner || {};

  // reportingOwner can be array
  const ro = Array.isArray(reportingOwner)
    ? reportingOwner[0]
    : reportingOwner || {};

  const issuerTradingSymbol = safeText(issuer?.issuerTradingSymbol);
  const issuerName = safeText(issuer?.issuerName);

  const ownerName = safeText(
    ro?.reportingOwnerId?.rptOwnerName || ro?.rptOwnerName
  );
  const ownerTitle = safeText(
    ro?.reportingOwnerRelationship?.officerTitle ||
      ro?.officerTitle ||
      ro?.reportingOwnerRelationship?.otherText
  );

  const nonDerivTable =
    ownershipDocument?.nonDerivativeTable?.nonDerivativeTransaction || [];

  const txs = Array.isArray(nonDerivTable)
    ? nonDerivTable
    : [nonDerivTable].filter(Boolean);

  // Keep only purchases ("P")
  const purchases = txs
    .map((t) => {
      const code = safeText(
        t?.transactionCoding?.transactionCode || t?.transactionCode
      );
      if (code !== "P") return null;

      const shares = Number(
        t?.transactionAmounts?.transactionShares?.value ??
          t?.transactionShares?.value ??
          t?.transactionShares ??
          0
      );
      const price = Number(
        t?.transactionAmounts?.transactionPricePerShare?.value ??
          t?.transactionPricePerShare?.value ??
          t?.transactionPricePerShare ??
          0
      );
      const date = safeText(
        t?.transactionDate?.value ?? t?.transactionDate ?? ""
      );

      if (!Number.isFinite(shares) || !Number.isFinite(price)) return null;

      return {
        issuerTradingSymbol,
        issuerName,
        ownerName,
        ownerTitle,
        shares,
        pricePerShare: price,
        transactionDate: isoDateOnly(date),
        totalValue: Math.round(shares * price),
      };
    })
    .filter(Boolean);

  return purchases;
}

// --- BEST-EFFORT in-memory cache across warm invocations ---
globalThis.__INSIDER_CACHE__ = globalThis.__INSIDER_CACHE__ || {
  ts: 0,
  key: "",
  data: null,
};
const memCache = globalThis.__INSIDER_CACHE__;

// Main handler
export default async function handler(req, res) {
  // Edge cache headers (lets Vercel cache responses)
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=3600");

  const SEC_UA = process.env.SEC_USER_AGENT; // you already set this
  if (!SEC_UA) {
    return res.status(500).json({
      error: "Missing SEC_USER_AGENT env var",
      hint: 'Set SEC_USER_AGENT like: "YourAppName (youremail+insiderscope@gmail.com)"',
    });
  }

  const limit = clamp(toInt(req.query.limit, 25), 1, 200);
  const days = clamp(toInt(req.query.days, 30), 1, 365);
  const debug = String(req.query.debug || "") === "1";

  // scan = how many Atom entries we will attempt to fully parse (each may trigger extra SEC calls)
  const scanCap = clamp(toInt(req.query.scan, 15), 1, 50);

  const cacheKey = `limit=${limit}|days=${days}|scan=${scanCap}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000; // 5 min

  // Serve in-memory cache if fresh
  if (
    memCache.data &&
    memCache.key === cacheKey &&
    now - memCache.ts < cacheTtlMs
  ) {
    return res.status(200).json(
      debug
        ? { data: memCache.data, debug: { cache: "mem-hit", limit, days, scanCap } }
        : memCache.data
    );
  }

  // Atom feed: "Insider Transactions (Form 4)"
  // NOTE: SEC sometimes changes feeds; this one is commonly used.
  const atomUrl = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=100&output=atom";

  const headers = {
    "User-Agent": SEC_UA,
    Accept: "application/atom+xml,application/xml,text/xml,*/*",
    "Accept-Encoding": "identity",
  };

  let atomXml;
  try {
    const atomResp = await fetch(atomUrl, { headers });
    if (atomResp.status === 429) {
      // If we have any cached data (even stale), return it instead of failing hard
      if (memCache.data) {
        return res.status(200).json(
          debug
            ? {
                data: memCache.data,
                debug: { cache: "mem-stale-after-429", limit, days, scanCap, atomStatus: 429 },
              }
            : memCache.data
        );
      }
      return res.status(429).json({
        error: "SEC Atom fetch failed (429)",
        hint: "SEC rate limited the backend. Add caching/throttling (this file does), then redeploy and avoid rapid refreshes.",
      });
    }
    if (!atomResp.ok) {
      return res.status(atomResp.status).json({
        error: `SEC Atom fetch failed (${atomResp.status})`,
      });
    }
    atomXml = await atomResp.text();
  } catch (e) {
    return res.status(500).json({ error: "SEC Atom fetch threw", detail: String(e) });
  }

  const entries = parseAtom(atomXml);

  // Filter by days cutoff using Atom "updated"/"published"
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;

  const recentEntries = entries
    .map((en) => {
      const linkHref =
        en?.link?.href ||
        (Array.isArray(en?.link) ? en.link[0]?.href : null) ||
        "";
      const updated = safeText(en?.updated || en?.published || "");
      const ts = updated ? Date.parse(updated) : 0;
      return { linkHref, updated, ts };
    })
    .filter((x) => x.linkHref && x.ts && x.ts >= cutoff)
    .slice(0, scanCap);

  // Now for each entry:
  // 1) get index.json
  // 2) pick an XML
  // 3) fetch XML
  // 4) parse purchases
  const out = [];
  const errorsSample = [];

  for (let i = 0; i < recentEntries.length; i++) {
    const { linkHref, updated } = recentEntries[i];
    const meta = extractCikAndAccession(linkHref);
    if (!meta) continue;

    try {
      // throttle to be nice to SEC
      if (i > 0) await sleep(250);

      const idxUrl = indexJsonUrl(meta.cik, meta.accessionNo);
      const idxResp = await fetch(idxUrl, { headers });

      if (idxResp.status === 429) {
        errorsSample.push({ where: "index.json", status: 429, idxUrl });
        break; // stop to avoid a cascade of 429s
      }
      if (!idxResp.ok) {
        errorsSample.push({ where: "index.json", status: idxResp.status, idxUrl });
        continue;
      }

      const idxJson = await idxResp.json();
      const xmlName = pickForm4Xml(idxJson);
      if (!xmlName) continue;

      const nodash = meta.accessionNo.replace(/-/g, "");
      const xmlUrl = `https://data.sec.gov/Archives/edgar/data/${Number(
        meta.cik
      )}/${nodash}/${xmlName}`;

      // throttle again
      await sleep(250);

      const xmlResp = await fetch(xmlUrl, { headers });

      if (xmlResp.status === 429) {
        errorsSample.push({ where: "form4.xml", status: 429, xmlUrl });
        break;
      }
      if (!xmlResp.ok) {
        errorsSample.push({ where: "form4.xml", status: xmlResp.status, xmlUrl });
        continue;
      }

      const xmlText = await xmlResp.text();
      const purchases = parseForm4Xml(xmlText);

      // Convert to your frontend schema
      for (const p of purchases) {
        const id = `${p.ownerName || "owner"}-${p.issuerTradingSymbol || "sym"}-${p.transactionDate || isoDateOnly(updated)}`;

        out.push({
          id,
          insiderName: p.ownerName || "—",
          insiderTitle: p.ownerTitle || "—",
          employerTicker: p.issuerTradingSymbol || "—",
          employerCompany: p.issuerName || "—",
          purchasedTicker: p.issuerTradingSymbol || "—",
          purchasedCompany: p.issuerName || "—",
          shares: p.shares,
          pricePerShare: p.pricePerShare,
          totalValue: p.totalValue,
          transactionDate: p.transactionDate || isoDateOnly(updated),
          signalScore: 50, // placeholder; you can compute later
          purchaseType: "own-company",
        });

        if (out.length >= limit) break;
      }

      if (out.length >= limit) break;
    } catch (e) {
      errorsSample.push({ where: "loop", error: String(e) });
    }
  }

  // Save to in-memory cache (even if empty, so we don’t hammer SEC on refresh)
  memCache.ts = Date.now();
  memCache.key = cacheKey;
  memCache.data = out;

  if (debug) {
    return res.status(200).json({
      data: out,
      debug: {
        cache: "miss-fill",
        returned: out.length,
        entriesSeen: entries.length,
        recentEntries: recentEntries.length,
        cutoff: new Date(cutoff).toISOString(),
        scanCap,
        errorsSample,
      },
    });
  }

  return res.status(200).json(out);
}
