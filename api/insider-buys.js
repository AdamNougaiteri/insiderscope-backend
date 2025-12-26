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

function collectFootnoteAndRemarksText(root) {
  const texts = [];

  // remarks can be string or { value: ... }
  const remarks =
    safeText(valueField(root?.remarks)) ||
    safeText(valueField(root?.remarks?.value)) ||
    "";
  if (remarks) texts.push(remarks);

  // footnotes: can be nested
  const footnoteNodes = [];
  // common
  footnoteNodes.push(...ensureArray(root?.footnotes?.footnote));
  // fallback deep search
  const deep = deepFindAll(root, "footnote");
  for (const d of deep) footnoteNodes.push(...ensureArray(d));

  for (const fn of footnoteNodes) {
    if (!fn) continue;
    // fn can be string, object, array
    if (typeof fn === "string") {
      const t = safeText(fn);
      if (t) texts.push(t);
      continue;
    }
    // some are like { value: "..." } or { "#text": "..." } or mixed
    const candidates = [
      fn?.value,
      fn?.text,
      fn?.["#text"],
      fn?.["$text"],
      fn,
    ];
    for (const c of candidates) {
      const t = safeText(typeof c === "object" ? "" : c);
      if (t) texts.push(t);
    }
  }

  return texts.join(" \n");
}

function looksScheduled(footnotesAndRemarks) {
  const s = safeText(footnotesAndRemarks).toLowerCase();
  if (!s) return false;
  // best-effort heuristics
  return (
    s.includes("10b5-1") ||
    s.includes("10b5 1") ||
    s.includes("rule 10b5") ||
    s.includes("10b5 plan") ||
    s.includes("trading plan")
  );
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

  const notesText = collectFootnoteAndRemarksText(root);
  const isScheduled = looksScheduled(notesText);

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
    debugCapture.isScheduled = isScheduled;
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

    const totalValue = Math.round(shares * price);

    purchases.push({
      issuerTradingSymbol,
      issuerName,
      ownerName,
      officerTitle,
      shares,
      pricePerShare: price,
      transactionDate: date,
      totalValue,
      isScheduled,
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

// Demo/seed data
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
      isScheduled: false,
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
      isScheduled: false,
    },
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
      isScheduled: false,
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
      isScheduled: true, // example scheduled flag
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
      isScheduled: false,
    },
  ];
}

function buildGroups(rows) {
  // Group “multiple execs buying same company”
  // We focus on: own-company purchases, NOT scheduled, totalValue > 0
  const m = new Map();

  for (const r of rows) {
    const sym = safeText(r.purchasedTicker || "");
    if (!sym) continue;

    const purchaseType = safeText(r.purchaseType || "");
    const isScheduled = !!r.isScheduled;

    // “non scheduled purchases” heuristic:
    if (purchaseType !== "own-company") continue;
    if (isScheduled) continue;
    if (!Number.isFinite(Number(r.totalValue)) || Number(r.totalValue) <= 0) continue;

    const key = sym.toUpperCase();
    if (!m.has(key)) {
      m.set(key, {
        purchasedTicker: key,
        purchasedCompany: safeText(r.purchasedCompany || r.employerCompany || ""),
        execCount: 0,
        uniqueInsiders: new Set(),
        totalValue: 0,
        latestDate: safeText(r.transactionDate || ""),
        insiders: [],
      });
    }

    const g = m.get(key);
    const insider = safeText(r.insiderName || "—");

    if (!g.uniqueInsiders.has(insider)) {
      g.uniqueInsiders.add(insider);
      g.insiders.push({
        insiderName: insider,
        insiderTitle: safeText(r.insiderTitle || ""),
        totalValue: Number(r.totalValue) || 0,
        transactionDate: safeText(r.transactionDate || ""),
      });
    }

    g.totalValue += Number(r.totalValue) || 0;

    const dt = safeText(r.transactionDate || "");
    if (dt && (!g.latestDate || dt > g.latestDate)) g.latestDate = dt;
  }

  const groups = Array.from(m.values()).map((g) => {
    g.execCount = g.uniqueInsiders.size;
    delete g.uniqueInsiders;
    // sort insider list by value desc
    g.insiders.sort((a, b) => (b.totalValue || 0) - (a.totalValue || 0));
    return g;
  });

  // Rank: more execs first, then bigger $ value
  groups.sort((a, b) => {
    if (b.execCount !== a.execCount) return b.execCount - a.execCount;
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
  const mode = safeText(req.query.mode || ""); // "seed"
  const v = safeText(req.query.v || "");
  const wrap = String(req.query.wrap || "") === "1";
  const includeGroups = String(req.query.includeGroups || "") === "1";

  const pageSize = clamp(toInt(req.query.pageSize, 50), 1, 100);
  const page = clamp(toInt(req.query.page, 1), 1, 500);
  const offset = (page - 1) * pageSize;

  const days = clamp(toInt(req.query.days, 30), 1, 365);
  const pages = clamp(toInt(req.query.pages, 2), 1, 3);
  const scanCap = clamp(toInt(req.query.scan, 35), 10, 60);
  const betweenCallsMs = clamp(toInt(req.query.throttle, 350), 200, 900);
  const TIME_BUDGET_MS = clamp(toInt(req.query.budget, 6000), 2500, 9000);

  const startedAt = Date.now();
  const timeLeft = () => TIME_BUDGET_MS - (Date.now() - startedAt);

  const cacheKey = `wrap=${wrap}|includeGroups=${includeGroups}|pageSize=${pageSize}|page=${page}|days=${days}|pages=${pages}|scan=${scanCap}|throttle=${betweenCallsMs}|budget=${TIME_BUDGET_MS}|mode=${mode}|v=${v}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000;

  if (memCache.data && memCache.key === cacheKey && now - memCache.ts < cacheTtlMs) {
    return res.status(200).json(memCache.data);
  }

  if (mode === "seed") {
    const all = seedRows();
    const data = all.slice(offset, offset + pageSize);
    const meta = { total: all.length, page, pageSize, offset, mode: "seed", source: "seed" };
    const groups = includeGroups ? buildGroups(all) : undefined;

    const payload = wrap ? { data, meta, ...(includeGroups ? { groups } : {}) } : data;

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

  // We collect more than one page worth so pagination has something to work with
  // But keep it serverless-safe.
  const desired = Math.min(200, offset + pageSize);

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
      if (out.length >= desired) break;
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
            isScheduled: !!pch.isScheduled,
          });

          if (out.length >= desired) break;
        }
      } catch (e) {
        errorsSample.push({ where: "loop", error: String(e) });
      }
    }

    if (out.length >= desired) break;
  }

  // If live returns nothing, use seed as fallback so UI doesn’t die
  const allRows = out.length ? out : seedRows();
  const data = allRows.slice(offset, offset + pageSize);

  const meta = {
    total: allRows.length,
    page,
    pageSize,
    offset,
    mode: "live",
    source: out.length ? "live" : "fallback-seed",
  };

  const groups = includeGroups ? buildGroups(allRows) : undefined;

  const payload = wrap
    ? {
        data,
        meta,
        ...(includeGroups ? { groups } : {}),
        ...(debug
          ? {
              debug: {
                cache: out.length ? "miss-fill" : "fallback-seed",
                returnedLive: out.length,
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
      }
    : data;

  memCache.ts = Date.now();
  memCache.key = cacheKey;
  memCache.data = payload;

  return res.status(200).json(payload);
}
