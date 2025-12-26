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

/** Strip XML namespace prefixes so <ns1:tag> becomes <tag> */
function stripNamespaces(xml) {
  if (!xml) return "";
  // remove xmlns="..." declarations (optional)
  let out = xml.replace(/\sxmlns(:\w+)?="[^"]*"/g, "");
  // replace opening tags <ns:Tag ...> -> <Tag ...>
  out = out.replace(/<(\/*)\w+:(\w+)([^>]*)>/g, "<$1$2$3>");
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

  // Prefer likely Form 4 doc names
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

// ---------- Regex-based Form 4 parsing (namespace-safe after stripNamespaces) ----------
function firstTag(xml, tag) {
  const re = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const m = xml.match(re);
  return m ? safeText(m[1]) : "";
}

function firstNestedTag(xml, parentTag, childTag) {
  const parentRe = new RegExp(
    `<${parentTag}\\b[^>]*>([\\s\\S]*?)<\\/${parentTag}>`,
    "i"
  );
  const pm = xml.match(parentRe);
  if (!pm) return "";
  const parentBody = pm[1];
  return firstTag(parentBody, childTag);
}

function parseNumberTag(block, tag) {
  const inner = firstTag(block, tag);
  if (!inner) return null;
  const v = firstTag(inner, "value") || inner;
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function parseDateTag(block, tag) {
  const inner = firstTag(block, tag);
  if (!inner) return null;
  const v = firstTag(inner, "value") || inner;
  return isoDateOnly(v);
}

function parseForm4PurchasesRegex(xmlTextRaw) {
  const xmlText = stripNamespaces(xmlTextRaw);

  const issuerTradingSymbol =
    firstNestedTag(xmlText, "issuer", "issuerTradingSymbol") ||
    firstTag(xmlText, "issuerTradingSymbol");

  const issuerName =
    firstNestedTag(xmlText, "issuer", "issuerName") ||
    firstTag(xmlText, "issuerName");

  const ownerName = firstTag(xmlText, "rptOwnerName") || firstTag(xmlText, "reportingOwnerName");
  const officerTitle = firstTag(xmlText, "officerTitle");

  // nonDerivativeTransaction blocks (after stripping namespaces this matches)
  const txBlocks = [];
  const txRe = /<nonDerivativeTransaction\b[^>]*>([\s\S]*?)<\/nonDerivativeTransaction>/gi;
  let m;
  while ((m = txRe.exec(xmlText)) !== null) {
    txBlocks.push(m[1]);
    if (txBlocks.length > 400) break;
  }

  const purchases = [];
  for (const block of txBlocks) {
    const code =
      firstNestedTag(block, "transactionCoding", "transactionCode") ||
      firstTag(block, "transactionCode");

    if (safeText(code) !== "P") continue;

    const shares = parseNumberTag(block, "transactionShares");
    const price = parseNumberTag(block, "transactionPricePerShare");
    const date = parseDateTag(block, "transactionDate");

    if (!Number.isFinite(shares) || !Number.isFinite(price)) continue;

    purchases.push({
      issuerTradingSymbol: issuerTradingSymbol || "",
      issuerName: issuerName || "",
      ownerName: ownerName || "",
      officerTitle: officerTitle || "",
      shares,
      pricePerShare: price,
      transactionDate: date,
      totalValue: Math.round(shares * price),
    });
  }

  return purchases;
}
// --------------------------------------------------------

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

  const cacheKey = `limit=${limit}|days=${days}|scan=${scanCap}`;
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
      if (memCache.data) {
        return res.status(200).json(
          debug
            ? {
                data: memCache.data,
                debug: {
                  cache: "mem-stale-after-429",
                  limit,
                  days,
                  scanCap,
                  atomStatus: 429,
                },
              }
            : memCache.data
        );
      }
      return res.status(429).json({
        error: "SEC Atom fetch failed (429)",
        hint: "SEC rate limited the backend. Reduce refreshes.",
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
      const purchases = parseForm4PurchasesRegex(xmlText);

      if (debug && samples.length < 10) {
        const norm = stripNamespaces(xmlText);
        samples.push({
          idxUrl,
          xmlUrl,
          xmlName,
          purchasesFound: purchases.length,
          issuerTradingSymbol: firstTag(norm, "issuerTradingSymbol"),
          rptOwnerName: firstTag(norm, "rptOwnerName"),
        });
      }

      for (const p of purchases) {
        const dt = p.transactionDate || isoDateOnly(updated);
        const sym = p.issuerTradingSymbol || "—";
        const nm = p.ownerName || "—";
        const id = `${nm}-${sym}-${dt}`;

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
        samples,
      },
    });
  }

  return res.status(200).json(out);
}
