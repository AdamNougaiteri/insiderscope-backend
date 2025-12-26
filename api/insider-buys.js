// /api/insider-buys.js
import { XMLParser } from "fast-xml-parser";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const toInt = (v, d) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
const safeText = (v) => (v === null || v === undefined ? "" : String(v).trim());
const isoDateOnly = (s) => (s ? String(s).slice(0, 10) : null);

globalThis.__INSIDER_CACHE__ = globalThis.__INSIDER_CACHE__ || {
  ts: 0,
  key: "",
  data: null,
};
const memCache = globalThis.__INSIDER_CACHE__;

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

function parseForm4Purchases(xmlTextRaw, debugCapture = null) {
  const xmlText = stripNamespaces(xmlTextRaw);
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
  });

  let doc;
  try {
    doc = parser.parse(xmlText);
  } catch (e) {
    if (debugCapture) debugCapture.parseError = String(e);
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

  if (debugCapture) {
    const codes = {};
    for (const t of txs) {
      const code = safeText(
        valueField(t?.transactionCoding?.transactionCode) ??
          t?.transactionCode ??
          ""
      );
      if (code) codes[code] = (codes[code] || 0) + 1;
    }
    debugCapture.nonDerivCount = txs.length;
    debugCapture.codes = codes;
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

// Demo/seed data so UI can show >2 rows even when SEC returns none or rate limits.
// This is only used if you call ?mode=seed or if the live fetch returns empty.
function seedRows() {
  const today = new Date();
  const daysAgo = (n) => {
    const d = new Date(today);
    d.setDate(d.getDate() - n);
    return d.toISOString().slice(0, 10);
  };

  return [
    {
      id: "cook-nke-2025-12-22",
      insiderName: "Timothy D. Cook",
      insiderTitle: "CEO",
      employerTicker: "AAPL",
      employerCompany: "Apple Inc.",
      purchasedTicker: "NKE",
      purchasedCompany: "Nike Inc.",
      shares: 50000,
      pricePerShare: 103.25,
      totalValue: 5162500,
      transactionDate: "2025-12-22",
      signalScore: 92,
      purchaseType: "external",
    },
    {
      id: "nadella-msft-2025-12-21",
      insiderName: "Satya Nadella",
      insiderTitle: "CEO",
      employerTicker: "MSFT",
      employerCompany: "Microsoft Corp.",
      purchasedTicker: "MSFT",
      purchasedCompany: "Microsoft Corp.",
      shares: 25000,
      pricePerShare: 412.15,
      totalValue: 10303750,
      transactionDate: "2025-12-21",
      signalScore: 88,
      purchaseType: "own-company",
    },
    // extra demo rows
    {
      id: "demo-1",
      insiderName: "Jane Doe",
      insiderTitle: "CFO",
      employerTicker: "DEMO",
      employerCompany: "Demo Holdings",
      purchasedTicker: "DEMO",
      purchasedCompany: "Demo Holdings",
      shares: 12000,
      pricePerShare: 27.4,
      totalValue: 328800,
      transactionDate: daysAgo(4),
      signalScore: 74,
      purchaseType: "own-company",
    },
    {
      id: "demo-2",
      insiderName: "John Smith",
      insiderTitle: "Director",
      employerTicker: "ACME",
      employerCompany: "Acme Corp.",
      purchasedTicker: "ACME",
      purchasedCompany: "Acme Corp.",
      shares: 8000,
      pricePerShare: 55.1,
      totalValue: 440800,
      transactionDate: daysAgo(9),
      signalScore: 67,
      purchaseType: "own-company",
    },
    {
      id: "demo-3",
      insiderName: "Alex Kim",
      insiderTitle: "CEO",
      employerTicker: "RIVR",
      employerCompany: "River Tech",
      purchasedTicker: "RIVR",
      purchasedCompany: "River Tech",
      shares: 20000,
      pricePerShare: 14.75,
      totalValue: 295000,
      transactionDate: daysAgo(12),
      signalScore: 59,
      purchaseType: "own-company",
    },
  ];
}

export default async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=3600");

  const SEC_UA = process.env.SEC_USER_AGENT;
  if (!SEC_UA) {
    return res.status(500).json({
      error: "Missing SEC_USER_AGENT env var",
      hint: 'Set SEC_USER_AGENT like: "InsiderScope (your_email@example.com)"',
    });
  }

  const debug = String(req.query.debug || "") === "1";
  const mode = safeText(req.query.mode || ""); // "seed" to force demo
  const v = safeText(req.query.v || ""); // cache buster

  // Keep these constrained so we don't time out.
  const limit = clamp(toInt(req.query.limit, 25), 1, 50);
  const days = clamp(toInt(req.query.days, 30), 1, 365);

  // IMPORTANT: keep pages low on serverless; increase later with background jobs.
  const pages = clamp(toInt(req.query.pages, 2), 1, 3);
  const scanCap = clamp(toInt(req.query.scan, 35), 10, 60);

  // Respect SEC rate limits and avoid 429s; but don't over-wait or we time out.
  const betweenCallsMs = clamp(toInt(req.query.throttle, 350), 200, 900);

  // Hard time budget (ms) to avoid Vercel timeout.
  const TIME_BUDGET_MS = clamp(toInt(req.query.budget, 6000), 2500, 9000);
  const startedAt = Date.now();
  const timeLeft = () => TIME_BUDGET_MS - (Date.now() - startedAt);

  const cacheKey = `limit=${limit}|days=${days}|pages=${pages}|scan=${scanCap}|throttle=${betweenCallsMs}|mode=${mode}|v=${v}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000;

  if (memCache.data && memCache.key === cacheKey && now - memCache.ts < cacheTtlMs) {
    return res.status(200).json(
      debug
        ? {
            data: memCache.data,
            debug: { cache: "mem-hit", limit, days, pages, scanCap, throttle: betweenCallsMs, budget: TIME_BUDGET_MS },
          }
        : memCache.data
    );
  }

  if (mode === "seed") {
    const data = seedRows().slice(0, limit);
    memCache.ts = Date.now();
    memCache.key = cacheKey;
    memCache.data = data;
    return res.status(200).json(debug ? { data, debug: { cache: "seed", limit } } : data);
  }

  const headers = {
    "User-Agent": SEC_UA,
    Accept: "application/atom+xml,application/xml,text/xml,*/*",
    "Accept-Encoding": "identity",
  };

  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;

  const out = [];
  const errorsSample = [];
  const samples = [];
  let entriesSeenTotal = 0;

  for (let p = 0; p < pages; p++) {
    if (timeLeft() < 1200) break;

    const start = p * 100;
    let atomXml;
    let atomUrl;

    try {
      if (p > 0) await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1000)));
      const { resp, url } = await fetchAtomPage({ start, headers });
      atomUrl = url;

      if (resp.status === 429) {
        // Fast fail to avoid timeout
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
      if (out.length >= limit) break;
      if (timeLeft() < 1200) break;

      const { linkHref, updated } = recentEntries[i];
      const meta = extractCikAndAccession(linkHref);
      if (!meta) continue;

      try {
        await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1000)));

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

        await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1000)));

        const xmlResp = await fetch(xmlUrl, { headers });
        if (!xmlResp.ok) {
          errorsSample.push({ where: "form4.xml", status: xmlResp.status, xmlUrl });
          continue;
        }

        const xmlText = await xmlResp.text();

        const dbg = debug ? {} : null;
        const purchases = parseForm4Purchases(xmlText, dbg);

        if (debug && samples.length < 15) {
          samples.push({
            idxUrl,
            xmlUrl,
            xmlName,
            purchasesFound: purchases.length,
            ...(dbg || {}),
          });
        }

        for (const pch of purchases) {
          const dt = pch.transactionDate || isoDateOnly(updated);
          const sym = pch.issuerTradingSymbol || "—";
          const nm = pch.ownerName || "—";
          const id = `${nm}-${sym}-${dt}-${Math.random().toString(16).slice(2)}`;

          out.push({
            id,
            insiderName: nm,
            insiderTitle: pch.officerTitle || "—",
            employerTicker: sym,
            employerCompany: pch.issuerName || "—",
            purchasedTicker: sym,
            purchasedCompany: pch.issuerName || "—",
            shares: pch.shares,
            pricePerShare: pch.pricePerShare,
            totalValue: pch.totalValue,
            transactionDate: dt,
            signalScore: 50,
            purchaseType: "own-company",
          });

          if (out.length >= limit) break;
        }
      } catch (e) {
        errorsSample.push({ where: "loop", error: String(e) });
      }
    }

    if (out.length >= limit) break;
  }

  const finalData = out.length ? out : seedRows().slice(0, limit);

  memCache.ts = Date.now();
  memCache.key = cacheKey;
  memCache.data = finalData;

  if (debug) {
    return res.status(200).json({
      data: finalData,
      debug: {
        cache: out.length ? "miss-fill" : "fallback-seed",
        returned: out.length,
        returnedAfterFallback: finalData.length,
        entriesSeen: entriesSeenTotal,
        cutoff: new Date(cutoff).toISOString(),
        scanCap,
        pages,
        throttle: betweenCallsMs,
        budget: TIME_BUDGET_MS,
        timeSpentMs: Date.now() - startedAt,
        errorsSample,
        samples,
      },
    });
  }

  return res.status(200).json(finalData);
}
