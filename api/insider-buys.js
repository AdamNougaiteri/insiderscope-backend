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

// Seed rows — includes at least one cluster (multiple insiders buying same ticker)
function seedRows() {
  const today = new Date();
  const daysAgo = (n) => {
    const d = new Date(today);
    d.setDate(d.getDate() - n);
    return d.toISOString().slice(0, 10);
  };

  return [
    // Cluster example: MSFT has 2 insiders buying within window
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
      transactionDate: daysAgo(5),
      signalScore: 88,
      purchaseType: "own-company",
    },
    {
      id: "hood-msft-2025-12-20",
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
      signalScore: 82,
      purchaseType: "own-company",
    },

    // Other sample rows
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
      transactionDate: daysAgo(4),
      signalScore: 92,
      purchaseType: "external",
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

// Groups (“Company Clusters”) computed from returned rows
function computeGroups(rows) {
  // “Best-effort excluding scheduled buys” (we don't have 10b5-1 reliably here),
  // but we can exclude obvious non-buys / exercises:
  const eligible = rows.filter((r) => {
    const type = safeText(r.purchaseType).toLowerCase();
    const price = Number(r.pricePerShare ?? 0);
    const shares = Number(r.shares ?? 0);
    // exclude option exercises / zero-priced lines
    if (type.includes("option") || type.includes("exercise")) return false;
    if (!(price > 0) || !(shares > 0)) return false;
    return true;
  });

  const byTicker = new Map();

  for (const r of eligible) {
    const t = safeText(r.purchasedTicker || r.employerTicker);
    if (!t) continue;

    const key = t.toUpperCase();
    if (!byTicker.has(key)) {
      byTicker.set(key, {
        ticker: key,
        company: safeText(r.purchasedCompany || r.employerCompany || ""),
        insiders: [],
        insiderSet: new Set(),
        totalValue: 0,
        latestDate: "",
      });
    }

    const g = byTicker.get(key);
    const name = safeText(r.insiderName);
    if (name && !g.insiderSet.has(name)) {
      g.insiderSet.add(name);
    }

    g.insiders.push({
      insiderName: name || "—",
      insiderTitle: safeText(r.insiderTitle) || "—",
      value: Number(r.totalValue ?? 0),
      date: safeText(r.transactionDate) || "",
      purchaseType: safeText(r.purchaseType) || "",
    });

    g.totalValue += Number(r.totalValue ?? 0);

    const dt = safeText(r.transactionDate);
    if (dt && (!g.latestDate || dt > g.latestDate)) g.latestDate = dt;
  }

  const groups = [];
  for (const [, g] of byTicker) {
    const insiderCount = g.insiderSet.size;
    if (insiderCount < 2) continue; // requires multiple insiders

    // keep top insider lines (largest value)
    const insidersSorted = g.insiders
      .slice()
      .sort((a, b) => (b.value || 0) - (a.value || 0))
      .slice(0, 6);

    groups.push({
      ticker: g.ticker,
      company: g.company || "—",
      insiderCount,
      totalValue: Math.round(g.totalValue),
      latestDate: g.latestDate || "",
      insiders: insidersSorted,
    });
  }

  groups.sort((a, b) => {
    if (b.insiderCount !== a.insiderCount) return b.insiderCount - a.insiderCount;
    return (b.totalValue || 0) - (a.totalValue || 0);
  });

  return groups;
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
  const wrap = String(req.query.wrap || "") === "1";
  const includeGroups = String(req.query.includeGroups || "") === "1";
  const mode = safeText(req.query.mode || ""); // "seed" to force demo
  const v = safeText(req.query.v || ""); // cache buster

  // Pagination (used only when wrap=1)
  const pageSize = clamp(toInt(req.query.pageSize, 50), 1, 100);
  const page = clamp(toInt(req.query.page, 1), 1, 1000);

  // If old clients pass limit/days:
  const limit = clamp(toInt(req.query.limit, pageSize), 1, 100);
  const days = clamp(toInt(req.query.days, 30), 1, 365);

  // Live scan caps (serverless friendly)
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

  // Seed mode (fast)
  if (mode === "seed") {
    const all = seedRows();
    const total = all.length;

    const slice = wrap
      ? all.slice((page - 1) * pageSize, (page - 1) * pageSize + pageSize)
      : all.slice(0, limit);

    const payload = wrap
      ? {
          data: slice,
          meta: {
            total,
            page,
            pageSize,
            offset: (page - 1) * pageSize,
            mode: "seed",
            source: "seed",
          },
          ...(includeGroups ? { groups: computeGroups(all) } : {}),
        }
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
      if (out.length >= (wrap ? 100 : limit)) break; // keep bounded
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

          if (out.length >= (wrap ? 100 : limit)) break;
        }
      } catch (e) {
        errorsSample.push({ where: "loop", error: String(e) });
      }
    }

    if (out.length >= (wrap ? 100 : limit)) break;
  }

  // If live found nothing, fallback to seed (keeps UI alive)
  const liveReturned = out.length;
  const allRows = liveReturned ? out : seedRows();

  // Final response shape
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
      ...(includeGroups ? { groups: computeGroups(allRows) } : {}),
      ...(debug
        ? {
            debug: {
              cache: liveReturned ? "miss-fill" : "fallback-seed",
              returnedLive: liveReturned,
              totalServed: allRows.length,
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
  } else {
    payload = allRows.slice(0, limit);
  }

  memCache.ts = Date.now();
  memCache.key = cacheKey;
  memCache.data = payload;

  return res.status(200).json(payload);
}
