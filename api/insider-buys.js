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
  if (Object.prototype.hasOwnProperty.call(obj, keyName)) {
    out.push(obj[keyName]);
  }
  for (const k of Object.keys(obj)) {
    deepFindAll(obj[k], keyName, out);
  }
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
  // Form4 often: { value: "123" }
  if (node && typeof node === "object" && "value" in node) return node.value;
  return node;
}

function parseForm4Purchases(xmlTextRaw, debugCapture = null) {
  const xmlText = stripNamespaces(xmlTextRaw);

  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
    // keep order off; standard object parse
  });

  let doc;
  try {
    doc = parser.parse(xmlText);
  } catch (e) {
    if (debugCapture) debugCapture.parseError = String(e);
    return [];
  }

  // SEC Form 4 root commonly: ownershipDocument
  const root = doc?.ownershipDocument || doc;

  // issuer / owner
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

  // Non-derivative transactions live here:
  // ownershipDocument.nonDerivativeTable.nonDerivativeTransaction (array or object)
  let txs =
    root?.nonDerivativeTable?.nonDerivativeTransaction ??
    root?.nonDerivativeTransaction ??
    null;

  txs = ensureArray(txs);

  // If not found, do a deep search fallback (covers odd layouts)
  if (txs.length === 0) {
    const hits = deepFindAll(root, "nonDerivativeTransaction");
    for (const h of hits) {
      txs.push(...ensureArray(h));
    }
  }

  // Debug: count codes seen
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

  const limit = clamp(toInt(req.query.limit, 25), 1, 200);
  const days = clamp(toInt(req.query.days, 30), 1, 365);
  const scanCap = clamp(toInt(req.query.scan, 25), 1, 60);
  const debug = String(req.query.debug || "") === "1";
  const v = safeText(req.query.v || ""); // cache buster

  const cacheKey = `limit=${limit}|days=${days}|scan=${scanCap}|v=${v}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000;

  if (memCache.data && memCache.key === cacheKey && now - memCache.ts < cacheTtlMs) {
    return res.status(200).json(
      debug
        ? { data: memCache.data, debug: { cache: "mem-hit", limit, days, scanCap } }
        : memCache.data
    );
  }

  const atomUrl =
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=100&output=atom";

  const headers = {
    "User-Agent": SEC_UA,
    Accept: "application/atom+xml,application/xml,text/xml,*/*",
    "Accept-Encoding": "identity",
  };

  let atomXml;
  try {
    const atomResp = await fetch(atomUrl, { headers });
    if (atomResp.status === 429) {
      return res.status(429).json({
        error: "SEC Atom fetch failed (429)",
        hint: "SEC rate limited the backend.",
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
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;

  const recentEntries = entries
    .map((en) => {
      const linkHref = pickArchivesLink(en);
      const updated = safeText(en?.updated || en?.published || "");
      const ts = updated ? Date.parse(updated) : 0;
      return { linkHref, updated, ts };
    })
    .filter((x) => x.linkHref && x.ts && x.ts >= cutoff)
    .slice(0, scanCap);

  const out = [];
  const errorsSample = [];
  const samples = [];

  for (let i = 0; i < recentEntries.length; i++) {
    const { linkHref, updated } = recentEntries[i];
    const meta = extractCikAndAccession(linkHref);
    if (!meta) continue;

    try {
      if (i > 0) await sleep(250);

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

      await sleep(250);

      const xmlResp = await fetch(xmlUrl, { headers });
      if (!xmlResp.ok) {
        errorsSample.push({ where: "form4.xml", status: xmlResp.status, xmlUrl });
        continue;
      }

      const xmlText = await xmlResp.text();

      const dbg = debug ? {} : null;
      const purchases = parseForm4Purchases(xmlText, dbg);

      if (debug && samples.length < 12) {
        samples.push({
          idxUrl,
          xmlUrl,
          xmlName,
          purchasesFound: purchases.length,
          ...(dbg || {}),
        });
      }

      for (const p of purchases) {
        const dt = p.transactionDate || isoDateOnly(updated);
        const sym = p.issuerTradingSymbol || "—";
        const nm = p.ownerName || "—";
        const id = `${nm}-${sym}-${dt}-${Math.random().toString(16).slice(2)}`;

        out.push({
          id,
          insiderName: nm,
          insiderTitle: p.officerTitle || "—",
          employerTicker: sym,
          employerCompany: p.issuerName || "—",
          purchasedTicker: sym,
          purchasedCompany: p.issuerName || "—",
          shares: p.shares,
          pricePerShare: p.pricePerShare,
          totalValue: p.totalValue,
          transactionDate: dt,
          signalScore: 50,
          purchaseType: "own-company",
        });

        if (out.length >= limit) break;
      }

      if (out.length >= limit) break;
    } catch (e) {
      errorsSample.push({ where: "loop", error: String(e) });
    }
  }

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
        samples,
      },
    });
  }

  return res.status(200).json(out);
}
