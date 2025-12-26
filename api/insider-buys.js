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

// fast-xml-parser sometimes stores text under "#text"
function textVal(node) {
  if (node === null || node === undefined) return null;
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (typeof node === "object") {
    if ("value" in node) return String(node.value);
    if ("#text" in node) return String(node["#text"]);
  }
  return null;
}

function parseForm4Buys(xmlTextRaw, allowedCodesSet, debugCapture = null) {
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
    safeText(textVal(root?.issuer?.issuerTradingSymbol)) ||
    safeText(textVal(root?.issuerTradingSymbol)) ||
    safeText(textVal(root?.issuer?.tradingSymbol)) ||
    "";

  const issuerName =
    safeText(textVal(root?.issuer?.issuerName)) ||
    safeText(textVal(root?.issuerName)) ||
    "";

  const ownerName =
    safeText(textVal(root?.reportingOwner?.reportingOwnerId?.rptOwnerName)) ||
    safeText(textVal(root?.rptOwnerName)) ||
    safeText(textVal(root?.reportingOwnerName)) ||
    "";

  const officerTitle =
    safeText(
      textVal(root?.reportingOwner?.reportingOwnerRelationship?.officerTitle)
    ) || safeText(textVal(root?.officerTitle)) || "";

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
        textVal(t?.transactionCoding?.transactionCode) ??
          textVal(t?.transactionCode) ??
          ""
      );
      if (code) codes[code] = (codes[code] || 0) + 1;
    }
    debugCapture.nonDerivCount = txs.length;
    debugCapture.codes = codes;
  }

  const buys = [];

  for (const t of txs) {
    const code = safeText(
      textVal(t?.transactionCoding?.transactionCode) ??
        textVal(t?.transactionCode) ??
        ""
    );

    if (!allowedCodesSet.has(code)) continue;

    const shares = numVal(
      textVal(t?.transactionAmounts?.transactionShares) ??
        textVal(t?.transactionShares) ??
        null
    );

    const price = numVal(
      textVal(t?.transactionAmounts?.transactionPricePerShare) ??
        textVal(t?.transactionPricePerShare) ??
        null
    );

    const date =
      isoDateOnly(
        textVal(t?.transactionDate) ?? textVal(t?.transactionDate?.value) ?? null
      ) || null;

    // Some codes may not have price; keep P strict, allow others if shares exist.
    if (!Number.isFinite(shares)) continue;

    buys.push({
      issuerTradingSymbol,
      issuerName,
      ownerName,
      officerTitle,
      shares,
      pricePerShare: Number.isFinite(price) ? price : null,
      transactionDate: date,
      totalValue: Number.isFinite(price) ? Math.round(shares * price) : null,
      code,
    });
  }

  return buys;
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
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=3600");

  const SEC_UA = process.env.SEC_USER_AGENT;
  if (!SEC_UA) {
    return res.status(500).json({
      error: "Missing SEC_USER_AGENT env var",
      hint: 'Set SEC_USER_AGENT like: "InsiderScope (your_email@example.com)"',
    });
  }

  const debug = String(req.query.debug || "") === "1";
  const v = safeText(req.query.v || "");

  const limit = clamp(toInt(req.query.limit, 25), 1, 50);
  const days = clamp(toInt(req.query.days, 30), 1, 365);

  // NEW: allowed transaction codes (comma-separated), default = P only
  // Examples:
  // codes=P          (open-market purchases only)
  // codes=P,M        (include option exercises / acquisitions)
  // codes=P,M,A      (broader)
  const codesParam = safeText(req.query.codes || "P");
  const allowedCodes = new Set(
    codesParam
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean)
  );

  // SAFE defaults for serverless
  const pages = clamp(toInt(req.query.pages, 1), 1, 3);
  const scanCap = clamp(toInt(req.query.scan, 25), 10, 60);
  const betweenCallsMs = clamp(toInt(req.query.throttle, 450), 250, 900);
  const TIME_BUDGET_MS = clamp(toInt(req.query.budget, 8500), 3000, 9500);

  const startedAt = Date.now();
  const timeLeft = () => TIME_BUDGET_MS - (Date.now() - startedAt);

  const cacheKey = `limit=${limit}|days=${days}|pages=${pages}|scan=${scanCap}|throttle=${betweenCallsMs}|budget=${TIME_BUDGET_MS}|codes=${[...allowedCodes].sort().join(",")}|v=${v}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000;

  if (
    memCache.data &&
    memCache.key === cacheKey &&
    now - memCache.ts < cacheTtlMs
  ) {
    return res.status(200).json(
      debug
        ? {
            data: memCache.data,
            debug: {
              cache: "mem-hit",
              limit,
              days,
              pages,
              scanCap,
              throttle: betweenCallsMs,
              budget: TIME_BUDGET_MS,
              codes: [...allowedCodes],
            },
          }
        : memCache.data
    );
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
    if (timeLeft() < 1500) break;

    const start = p * 100;
    let atomXml;
    let atomUrl;

    try {
      if (p > 0) await sleep(betweenCallsMs);
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
      if (out.length >= limit) break;
      if (timeLeft() < 1500) break;

      const { linkHref, updated } = recentEntries[i];
      const meta = extractCikAndAccession(linkHref);
      if (!meta) continue;

      try {
        await sleep(betweenCallsMs);

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

        await sleep(betweenCallsMs);

        const xmlResp = await fetch(xmlUrl, { headers });
        if (!xmlResp.ok) {
          errorsSample.push({ where: "form4.xml", status: xmlResp.status, xmlUrl });
          continue;
        }

        const xmlText = await xmlResp.text();

        const dbg = debug ? {} : null;
        const buys = parseForm4Buys(xmlText, allowedCodes, dbg);

        if (debug && samples.length < 15) {
          samples.push({
            idxUrl,
            xmlUrl,
            xmlName,
            purchasesFound: buys.length,
            ...(dbg || {}),
          });
        }

        for (const b of buys) {
          const dt = b.transactionDate || isoDateOnly(updated);
          const sym = b.issuerTradingSymbol || "—";
          const nm = b.ownerName || "—";
          const id = `${nm}-${sym}-${dt}-${b.code}-${Math.random().toString(16).slice(2)}`;

          const purchaseType =
            b.code === "P"
              ? "open-market"
              : b.code === "M"
              ? "option-exercise"
              : b.code === "A"
              ? "acquisition"
              : `code-${b.code}`;

          out.push({
            id,
            insiderName: nm,
            insiderTitle: b.officerTitle || "—",
            employerTicker: sym,
            employerCompany: b.issuerName || "—",
            purchasedTicker: sym,
            purchasedCompany: b.issuerName || "—",
            shares: b.shares,
            pricePerShare: b.pricePerShare ?? 0,
            totalValue: b.totalValue ?? 0,
            transactionDate: dt,
            signalScore: 50,
            purchaseType,
          });

          if (out.length >= limit) break;
        }
      } catch (e) {
        errorsSample.push({ where: "loop", error: String(e) });
      }
    }

    if (out.length >= limit) break;
  }

  const finalData = out.slice(0, limit);

  memCache.ts = Date.now();
  memCache.key = cacheKey;
  memCache.data = finalData;

  if (debug) {
    return res.status(200).json({
      data: finalData,
      debug: {
        cache: "miss-fill",
        returned: finalData.length,
        entriesSeen: entriesSeenTotal,
        cutoff: new Date(cutoff).toISOString(),
        scanCap,
        pages,
        throttle: betweenCallsMs,
        budget: TIME_BUDGET_MS,
        codes: [...allowedCodes],
        timeSpentMs: Date.now() - startedAt,
        errorsSample,
        samples,
      },
    });
  }

  return res.status(200).json(finalData);
}
