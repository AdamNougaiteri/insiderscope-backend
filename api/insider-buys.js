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

// Simple signal scoring (shared for seed + live rows)
function calcSignalScore({ insiderTitle, totalValue, transactionDate, purchaseType }) {
  let score = 0;

  const title = String(insiderTitle || "").toLowerCase();
  if (title.includes("ceo") || title.includes("chief executive")) score += 40;
  else if (title.includes("cfo") || title.includes("chief financial")) score += 35;
  else if (title.includes("director") || title.includes("chairman")) score += 25;
  else if (title.includes("president") || title.includes("evp")) score += 20;
  else score += 10;

  const val = Number(totalValue || 0);
  if (val > 1000000) score += 40;
  else if (val > 500000) score += 30;
  else if (val > 100000) score += 20;
  else if (val > 50000) score += 10;
  else score += 5;

  const dt = new Date(transactionDate || new Date());
  const now = new Date();
  const diffDays = Math.ceil(Math.abs(now.getTime() - dt.getTime()) / (1000 * 60 * 60 * 24));
  if (diffDays <= 2) score += 20;
  else if (diffDays <= 7) score += 10;
  else score += 5;

  // Penalize option exercises a bit vs. true purchases
  const pt = String(purchaseType || "").toLowerCase();
  if (pt.includes("option")) score -= 10;

  return Math.max(0, Math.min(100, score));
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
    // Keep strict: only true purchases
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

/**
 * Seed data generator: creates ~120 rows so your UI can show lots of data
 * even if SEC live fetching is empty/rate-limited.
 */
function seedRows() {
  const companies = [
    ["AAPL", "Apple Inc."],
    ["MSFT", "Microsoft Corp."],
    ["AMZN", "Amazon.com, Inc."],
    ["GOOGL", "Alphabet Inc."],
    ["META", "Meta Platforms, Inc."],
    ["NVDA", "NVIDIA Corp."],
    ["TSLA", "Tesla, Inc."],
    ["JPM", "JPMorgan Chase & Co."],
    ["BAC", "Bank of America Corp."],
    ["WMT", "Walmart Inc."],
    ["COST", "Costco Wholesale Corp."],
    ["NKE", "Nike, Inc."],
    ["DIS", "Walt Disney Co."],
    ["NFLX", "Netflix, Inc."],
    ["CRM", "Salesforce, Inc."],
    ["ORCL", "Oracle Corp."],
    ["PEP", "PepsiCo, Inc."],
    ["KO", "Coca-Cola Co."],
    ["XOM", "Exxon Mobil Corp."],
    ["CVX", "Chevron Corp."],
    ["UNH", "UnitedHealth Group Inc."],
    ["ABBV", "AbbVie Inc."],
    ["LLY", "Eli Lilly and Co."],
    ["PFE", "Pfizer Inc."],
    ["INTC", "Intel Corp."],
    ["AMD", "Advanced Micro Devices, Inc."],
    ["QCOM", "QUALCOMM Inc."],
    ["ADBE", "Adobe Inc."],
    ["V", "Visa Inc."],
    ["MA", "Mastercard Inc."],
  ];

  const people = [
    ["CEO", "Alex Kim"],
    ["CEO", "Jordan Lee"],
    ["CEO", "Taylor Nguyen"],
    ["CFO", "Morgan Patel"],
    ["CFO", "Casey Chen"],
    ["Director", "Riley Brooks"],
    ["Director", "Avery Johnson"],
    ["EVP", "Parker Rivera"],
    ["President", "Cameron Singh"],
    ["Chairman", "Quinn Davis"],
  ];

  const pick = (arr, i) => arr[i % arr.length];

  // deterministic pseudo-random (no Math.random() variance between calls)
  const prng = (i) => {
    // simple hash-like number in (0..1)
    const x = Math.sin(i * 999) * 10000;
    return x - Math.floor(x);
  };

  const today = new Date();
  const daysAgo = (n) => {
    const d = new Date(today);
    d.setDate(d.getDate() - n);
    return d.toISOString().slice(0, 10);
  };

  const out = [];
  const ROWS = 120;

  for (let i = 0; i < ROWS; i++) {
    const [pTitle, pName] = pick(people, i);
    const [empT, empC] = pick(companies, i * 3);
    const [buyT, buyC] = pick(companies, i * 3 + 7);

    const r1 = prng(i + 1);
    const r2 = prng(i + 2);
    const r3 = prng(i + 3);

    const price = Number((5 + r1 * 495).toFixed(2));           // $5 - $500
    const shares = Math.floor(500 + r2 * 75000);               // 500 - 75,500
    const totalValue = Math.round(price * shares);

    const purchaseType =
      r3 < 0.15 ? "option-exercise" : empT === buyT ? "own-company" : "external";

    const transactionDate = daysAgo(1 + Math.floor(prng(i + 10) * 29)); // last ~30 days

    const row = {
      id: `${pName.replace(/\s+/g, "-")}-${buyT}-${transactionDate}-${i}`,
      insiderName: pName,
      insiderTitle: pTitle,
      employerTicker: empT,
      employerCompany: empC,
      purchasedTicker: buyT,
      purchasedCompany: buyC,
      shares,
      pricePerShare: price,
      totalValue,
      transactionDate,
      signalScore: 0,
      purchaseType,
    };

    row.signalScore = calcSignalScore(row);
    out.push(row);
  }

  // Sort so UI “Market Signals” feels real
  out.sort((a, b) => (b.signalScore || 0) - (a.signalScore || 0));
  return out;
}

function paginate(allRows, page, pageSize) {
  const total = allRows.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const p = clamp(page, 1, totalPages);
  const offset = (p - 1) * pageSize;
  const data = allRows.slice(offset, offset + pageSize);
  return { data, total, page: p, pageSize, offset, totalPages };
}

export default async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=3600");

  const wrap = String(req.query.wrap || "") === "1";
  const debug = String(req.query.debug || "") === "1";
  const mode = safeText(req.query.mode || "live"); // live | seed
  const v = safeText(req.query.v || ""); // cache buster

  const pageSize = clamp(toInt(req.query.pageSize, 50), 1, 100);
  const page = clamp(toInt(req.query.page, 1), 1, 9999);
  const days = clamp(toInt(req.query.days, 30), 1, 365);

  // Live crawling constraints (serverless-friendly)
  const pages = clamp(toInt(req.query.pages, 1), 1, 3);
  const scanCap = clamp(toInt(req.query.scan, 25), 10, 60);
  const betweenCallsMs = clamp(toInt(req.query.throttle, 350), 200, 900);
  const TIME_BUDGET_MS = clamp(toInt(req.query.budget, 6000), 2500, 9000);

  const startedAt = Date.now();
  const timeLeft = () => TIME_BUDGET_MS - (Date.now() - startedAt);

  const cacheKey = `wrap=${wrap}|debug=${debug}|mode=${mode}|days=${days}|pageSize=${pageSize}|page=${page}|pages=${pages}|scan=${scanCap}|throttle=${betweenCallsMs}|budget=${TIME_BUDGET_MS}|v=${v}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000;

  if (memCache.data && memCache.key === cacheKey && now - memCache.ts < cacheTtlMs) {
    return res.status(200).json(memCache.data);
  }

  // SEED MODE: always return seed (paginated)
  if (mode === "seed") {
    const all = seedRows();
    const pg = paginate(all, page, pageSize);

    const payload = {
      data: pg.data,
      meta: {
        total: pg.total,
        page: pg.page,
        pageSize: pg.pageSize,
        offset: pg.offset,
        totalPages: pg.totalPages,
        mode: "seed",
        source: "seed",
      },
      ...(debug
        ? {
            debug: {
              cache: "seed",
              timeSpentMs: Date.now() - startedAt,
            },
          }
        : {}),
    };

    memCache.ts = Date.now();
    memCache.key = cacheKey;
    memCache.data = payload;

    // If wrap=0 (older clients), return just array
    return res.status(200).json(wrap ? payload : pg.data);
  }

  // LIVE MODE: fetch SEC, fall back to seed if empty
  const SEC_UA = process.env.SEC_USER_AGENT;
  if (!SEC_UA) {
    const payload = {
      error: "Missing SEC_USER_AGENT env var",
      hint: 'Set SEC_USER_AGENT like: "InsiderScope (your_email@example.com)"',
    };
    return res.status(500).json(payload);
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

      if (resp.status === 429) break;
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

          const row = {
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
            signalScore: 0,
            purchaseType: "own-company",
          };

          row.signalScore = calcSignalScore(row);
          out.push(row);
        }
      } catch (e) {
        errorsSample.push({ where: "loop", error: String(e) });
      }
    }
  }

  // Sort + paginate live rows
  out.sort((a, b) => (b.signalScore || 0) - (a.signalScore || 0));
  const livePg = paginate(out, page, pageSize);

  // If live returned nothing, fall back to seed
  let finalData = livePg.data;
  let finalMeta = {
    total: livePg.total,
    page: livePg.page,
    pageSize: livePg.pageSize,
    offset: livePg.offset,
    totalPages: livePg.totalPages,
    mode: "live",
    source: "live",
  };

  if (out.length === 0) {
    const allSeed = seedRows();
    const seedPg = paginate(allSeed, page, pageSize);
    finalData = seedPg.data;
    finalMeta = {
      total: seedPg.total,
      page: seedPg.page,
      pageSize: seedPg.pageSize,
      offset: seedPg.offset,
      totalPages: seedPg.totalPages,
      mode: "live",
      source: "fallback-seed",
    };
  }

  const payload = {
    data: finalData,
    meta: finalMeta,
    ...(debug
      ? {
          debug: {
            cache: finalMeta.source,
            returnedLive: out.length,
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
        }
      : {}),
  };

  memCache.ts = Date.now();
  memCache.key = cacheKey;
  memCache.data = payload;

  return res.status(200).json(wrap ? payload : payload.data);
}
