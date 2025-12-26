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

globalThis.__INSIDER_CACHE__ = globalThis.__INSIDER_CACHE__ || { ts: 0, key: "", data: null };
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

  const accessionNoDashed = acc.includes("-") ? acc : `${acc.slice(0, 10)}-${acc.slice(10, 12)}-${acc.slice(12)}`;
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

function parseAtom(xml) {
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
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

  const issuerName = safeText(root?.issuer?.issuerName) || safeText(root?.issuerName) || "";

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
    const code = safeText(valueField(t?.transactionCoding?.transactionCode) ?? t?.transactionCode ?? "");
    if (code !== "P") continue;

    const shares = numVal(valueField(t?.transactionAmounts?.transactionShares) ?? valueField(t?.transactionShares) ?? null);
    const price = numVal(
      valueField(t?.transactionAmounts?.transactionPricePerShare) ??
        valueField(t?.transactionPricePerShare) ??
        null
    );

    const date = isoDateOnly(valueField(t?.transactionDate) ?? valueField(t?.transactionDate?.value) ?? null) || null;

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

// Seed rows now include cik + stable id
function seedRows() {
  const today = new Date();
  const daysAgo = (n) => {
    const d = new Date(today);
    d.setDate(d.getDate() - n);
    return d.toISOString().slice(0, 10);
  };

  return [
    {
      id: "0000789019-000000000000000000-0",
      cik: "0000789019",
      accessionNo: "0000000000-00-000000",
      sourceUrl: "",
      insiderName: "Satya Nadella",
      insiderTitle: "CEO",
      employerTicker: "MSFT",
      employerCompany: "Microsoft Corp.",
      purchasedTicker: "MSFT",
      purchasedCompany: "Microsoft Corp.",
      shares: 25000,
      pricePerShare: 412.15,
      totalValue: 10303750,
      transactionDate: daysAgo(5),
      filingDate: daysAgo(5),
      signalScore: 88,
      purchaseType: "own-company",
    },
    {
      id: "0000789019-000000000000000000-1",
      cik: "0000789019",
      accessionNo: "0000000000-00-000000",
      sourceUrl: "",
      insiderName: "Amy Hood",
      insiderTitle: "CFO",
      employerTicker: "MSFT",
      employerCompany: "Microsoft Corp.",
      purchasedTicker: "MSFT",
      purchasedCompany: "Microsoft Corp.",
      shares: 8000,
      pricePerShare: 408.0,
      totalValue: 3264000,
      transactionDate: daysAgo(6),
      filingDate: daysAgo(6),
      signalScore: 82,
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

  const wrap = String(req.query.wrap || "") === "1";
  const includeGroups = String(req.query.includeGroups || "") === "1";
  const mode = safeText(req.query.mode || ""); // "seed"
  const v = safeText(req.query.v || "");

  const pageSize = clamp(toInt(req.query.pageSize, 50), 1, 100);
  const page = clamp(toInt(req.query.page, 1), 1, 1000);
  const limit = clamp(toInt(req.query.limit, pageSize), 1, 100);
  const days = clamp(toInt(req.query.days, 30), 1, 365);

  const pages = clamp(toInt(req.query.pages, 2), 1, 3);
  const scanCap = clamp(toInt(req.query.scan, 35), 10, 80);
  const betweenCallsMs = clamp(toInt(req.query.throttle, 350), 200, 900);

  const TIME_BUDGET_MS = clamp(toInt(req.query.budget, 6000), 2500, 9000);
  const startedAt = Date.now();
  const timeLeft = () => TIME_BUDGET_MS - (Date.now() - startedAt);

  const cacheKey = `wrap=${wrap}|groups=${includeGroups}|page=${page}|pageSize=${pageSize}|limit=${limit}|days=${days}|pages=${pages}|scan=${scanCap}|throttle=${betweenCallsMs}|mode=${mode}|v=${v}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000;

  if (memCache.data && memCache.key === cacheKey && now - memCache.ts < cacheTtlMs) {
    return res.status(200).json(memCache.data);
  }

  if (mode === "seed") {
    const all = seedRows();
    const total = all.length;
    const slice = wrap
      ? all.slice((page - 1) * pageSize, (page - 1) * pageSize + pageSize)
      : all.slice(0, limit);

    const payload = wrap
      ? { data: slice, meta: { total, page, pageSize, offset: (page - 1) * pageSize, mode: "seed", source: "seed" } }
      : slice;

    memCache.ts = Date.now();
    memCache.key = cacheKey;
    memCache.data = payload;
    return res.status(200).json(payload);
  }

  const headers = {
    "User-Agent": SEC_UA,
    Accept: "application/atom+xml,application/xml,text/xml,*/*",
    "Accept-Encoding": "identity",
  };

  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
  const out = [];

  for (let p = 0; p < pages; p++) {
    if (timeLeft() < 1200) break;

    const start = p * 100;

    let atomXml;
    try {
      if (p > 0) await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1000)));
      const { resp } = await fetchAtomPage({ start, headers });
      if (resp.status === 429) break;
      if (!resp.ok) continue;
      atomXml = await resp.text();
    } catch {
      continue;
    }

    const entries = parseAtom(atomXml);

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
      if (out.length >= (wrap ? 100 : limit)) break;
      if (timeLeft() < 1200) break;

      const { linkHref, updated } = recentEntries[i];
      const meta = extractCikAndAccession(linkHref);
      if (!meta) continue;

      try {
        await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1000)));

        const idxUrl = indexJsonUrl(meta.cik, meta.accessionNoNoDash);
        const idxResp = await fetch(idxUrl, { headers });
        if (!idxResp.ok) continue;

        const idxJson = await idxResp.json();
        const xmlName = pickForm4Xml(idxJson);
        if (!xmlName) continue;

        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${Number(meta.cik)}/${meta.accessionNoNoDash}/${xmlName}`;

        await sleep(Math.min(betweenCallsMs, Math.max(0, timeLeft() - 1000)));

        const xmlResp = await fetch(xmlUrl, { headers });
        if (!xmlResp.ok) continue;

        const xmlText = await xmlResp.text();
        const purchases = parseForm4Purchases(xmlText);

        const filingDate = isoDateOnly(updated);

        purchases.forEach((pch, idx) => {
          const dt = pch.transactionDate || filingDate;
          const stableId = `${meta.accessionNoNoDash}-${idx}`;

          out.push({
            id: stableId,                 // <- this is what ingest will use as transaction_id
            cik: meta.cik,                // <- fixes your NOT NULL cik issue
            accessionNo: meta.accessionNoDashed,
            sourceUrl: xmlUrl,
            filingDate,
            insiderName: pch.ownerName || "—",
            insiderTitle: pch.officerTitle || "—",
            employerTicker: pch.issuerTradingSymbol || "",
            employerCompany: pch.issuerName || "",
            purchasedTicker: pch.issuerTradingSymbol || "",
            purchasedCompany: pch.issuerName || "",
            shares: pch.shares,
            pricePerShare: pch.pricePerShare,
            totalValue: pch.totalValue,
            transactionDate: dt,
            signalScore: 50,
            purchaseType: "own-company",
          });
        });

        if (out.length >= (wrap ? 100 : limit)) break;
      } catch {
        continue;
      }
    }

    if (out.length >= (wrap ? 100 : limit)) break;
  }

  // If SEC live yields nothing, fallback to seed
  const liveReturned = out.length;
  const allRows = liveReturned ? out : seedRows();

  let payload;
  if (wrap) {
    const total = allRows.length;
    const slice = allRows.slice((page - 1) * pageSize, (page - 1) * pageSize + pageSize);
    payload = {
      data: slice,
      meta: {
        total,
        page,
        pageSize,
        offset: (page - 1) * pageSize,
        mode: "live",
        source: liveReturned ? "sec-live" : "fallback-seed",
      },
    };
  } else {
    payload = allRows.slice(0, limit);
  }

  memCache.ts = Date.now();
  memCache.key = cacheKey;
  memCache.data = payload;

  return res.status(200).json(payload);
}
