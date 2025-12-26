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

// Best-effort warm cache
globalThis.__INSIDER_CACHE__ = globalThis.__INSIDER_CACHE__ || {
  ts: 0,
  key: "",
  data: null,
};
const memCache = globalThis.__INSIDER_CACHE__;

// Prefer the Atom link that contains /Archives/
function pickArchivesLink(entry) {
  const links = entry?.link;
  if (!links) return "";
  const arr = Array.isArray(links) ? links : [links];
  const hrefs = arr.map((l) => safeText(l?.href)).filter(Boolean);
  return (
    hrefs.find((h) => h.includes("/Archives/")) ||
    hrefs[0] ||
    ""
  );
}

// Extract cik + accession from archive URL (handles dashed or nodash accession)
function extractCikAndAccession(url) {
  const u = safeText(url);

  // matches:
  // /edgar/data/1935209/000119312525331321/
  // /edgar/data/1935209/0001193125-25-331321/
  const m = u.match(
    /edgar\/data\/(\d+)\/(\d{18}|\d{10}-\d{2}-\d{6})/i
  );
  if (!m) return null;

  const cik = m[1];
  const acc = m[2];

  const accessionNoDashed = acc.includes("-")
    ? acc
    : `${acc.slice(0, 10)}-${acc.slice(10, 12)}-${acc.slice(12)}`;

  const accessionNoNoDash = accessionNoDashed.replace(/-/g, "");

  return { cik, accessionNoDashed, accessionNoNoDash };
}

// IMPORTANT: use www.sec.gov/Archives for archive files
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

// Minimal Form 4 parser: purchases ("P") in non-derivatives
function parseForm4Xml(xmlText) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
  });
  const doc = parser.parse(xmlText);
  const ownershipDocument = doc?.ownershipDocument || doc?.document || doc || {};

  const issuer = ownershipDocument?.issuer || {};
  const reportingOwner = ownershipDocument?.reportingOwner || {};
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

  const table =
    ownershipDocument?.nonDerivativeTable?.nonDerivativeTransaction || [];
  const txs = Array.isArray(table) ? table : [table].filter(Boolean);

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
      const date = safeText(t?.transactionDate?.value ?? t?.transactionDate ?? "");

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

export default async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=3600");

  const SEC_UA = process.env.SEC_USER_AGENT;
  if (!SEC_UA) {
    return res.status(500).json({
      error: "Missing SEC_USER_AGENT env var",
      hint:
        'Set SEC_USER_AGENT like: "InsiderScope (your_email@example.com)"',
    });
  }

  const limit = clamp(toInt(req.query.limit, 25), 1, 200);
  const days = clamp(toInt(req.query.days, 30), 1, 365);
  const scanCap = clamp(toInt(req.query.scan, 15), 1, 50);
  const debug = String(req.query.debug || "") === "1";

  const cacheKey = `limit=${limit}|days=${days}|scan=${scanCap}`;
  const now = Date.now();
  const cacheTtlMs = 5 * 60 * 1000;

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
        hint: "SEC rate limited the backend. Reduce refreshes; caching should prevent repeated scrapes.",
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

  for (let i = 0; i < recentEntries.length; i++) {
    const { linkHref, updated } = recentEntries[i];
    const meta = extractCikAndAccession(linkHref);
    if (!meta) continue;

    try {
      if (i > 0) await sleep(250);

      const idxUrl = indexJsonUrl(meta.cik, meta.accessionNoNoDash);
      const idxResp = await fetch(idxUrl, { headers });

      if (idxResp.status === 429) {
        errorsSample.push({ where: "index.json", status: 429, idxUrl });
        break;
      }
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

      for (const p of purchases) {
        const id = `${p.ownerName || "owner"}-${p.issuerTradingSymbol || "sym"}-${
          p.transactionDate || isoDateOnly(updated)
        }`;

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
          signalScore: 50, // placeholder
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
      },
    });
  }

  return res.status(200).json(out);
}
