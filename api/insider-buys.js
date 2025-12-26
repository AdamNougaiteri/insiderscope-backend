// api/insider-buys.js

const BASE_ATOM =
  "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=100&output=atom";

const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const REQUEST_DELAY_MS = 350;

let cache = { ts: 0, key: "", data: [], debug: {} };

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function clampInt(v, def, min, max) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, Math.floor(n)));
}
function daysAgoIso(days) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString();
}
async function fetchText(url, headers) {
  await sleep(REQUEST_DELAY_MS);
  const r = await fetch(url, { headers });
  const t = await r.text();
  return { ok: r.ok, status: r.status, text: t };
}

function extractTag(xml, tag) {
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const m = xml.match(re);
  return m ? m[1].trim() : null;
}
function extractTagValue(xml, tag) {
  const inner = extractTag(xml, tag);
  if (!inner) return null;
  const v = extractTag(inner, "value");
  if (v) return v.trim();
  return inner.replace(/<[^>]+>/g, "").trim() || null;
}

function extractAllEntries(atomXml) {
  const entries = [];
  const re = /<entry>([\s\S]*?)<\/entry>/gi;
  let m;
  while ((m = re.exec(atomXml))) entries.push(m[1]);
  return entries;
}
function extractLinksFromEntry(entryXml) {
  const hrefs = [];
  const re = /href="([^"]+)"/gi;
  let m;
  while ((m = re.exec(entryXml))) hrefs.push(m[1]);
  return hrefs;
}
function pickFilingIndexHtmlLink(hrefs) {
  const idx = hrefs.find((h) => /-index\.html$/i.test(h));
  return idx || hrefs[0] || null;
}
async function fetchFilingIndexHtml(indexUrl, headers) {
  const { ok, status, text } = await fetchText(indexUrl, headers);
  if (!ok) throw new Error(`SEC index fetch failed ${status}`);
  return text;
}
function findXmlPrimaryDoc(indexHtml) {
  const re = /href="([^"]+\.xml)"/gi;
  let m;
  while ((m = re.exec(indexHtml))) {
    const href = m[1];
    if (/\.xml$/i.test(href)) return href;
  }
  return null;
}
async function fetchForm4Xml(indexUrl, indexHtml, headers) {
  let xmlHref = findXmlPrimaryDoc(indexHtml);
  if (!xmlHref) return null;

  if (xmlHref.startsWith("/")) xmlHref = `https://www.sec.gov${xmlHref}`;
  else if (!xmlHref.startsWith("http")) {
    const base = new URL(indexUrl);
    xmlHref = `${base.origin}${xmlHref.startsWith("/") ? "" : "/"}${xmlHref}`;
  }

  const { ok, status, text } = await fetchText(xmlHref, headers);
  if (!ok) throw new Error(`SEC XML fetch failed ${status}`);
  return text;
}

function parseTransactionsFromForm4Xml(form4Xml) {
  const txs = [];
  const re = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/gi;
  let m;
  while ((m = re.exec(form4Xml))) {
    const block = m[1];
    const code = extractTagValue(block, "transactionCode");
    if (String(code || "").toUpperCase() !== "P") continue;

    const sharesStr = extractTagValue(block, "transactionShares");
    const priceStr = extractTagValue(block, "transactionPricePerShare");
    const dateStr = extractTagValue(block, "transactionDate");

    const shares = sharesStr ? Number(String(sharesStr).replace(/,/g, "")) : null;
    const pricePerShare = priceStr ? Number(String(priceStr).replace(/,/g, "")) : null;

    txs.push({
      shares: Number.isFinite(shares) ? shares : null,
      pricePerShare: Number.isFinite(pricePerShare) ? pricePerShare : null,
      transactionDate: dateStr ? String(dateStr).trim() : null,
    });
  }
  return txs;
}

function computeSignalScore(totalValue, role) {
  let score = 50;
  if (typeof totalValue === "number") {
    if (totalValue >= 5_000_000) score += 35;
    else if (totalValue >= 1_000_000) score += 25;
    else if (totalValue >= 250_000) score += 15;
    else if (totalValue >= 50_000) score += 5;
  }
  const r = String(role || "").toLowerCase();
  if (r.includes("chief executive") || r === "ceo") score += 10;
  if (r.includes("chief financial") || r === "cfo") score += 6;
  if (r.includes("director")) score += 3;
  return Math.max(1, Math.min(99, Math.round(score)));
}

export default async function handler(req, res) {
  const ua = process.env.SEC_USER_AGENT;
  if (!ua) {
    return res.status(500).json({
      error: "Missing SEC_USER_AGENT env var. Set it in Vercel project settings.",
    });
  }

  const limit = clampInt(req.query.limit, 25, 1, 200);
  const days = clampInt(req.query.days, 30, 1, 180);

  // NEW: cap how many filings we scan per request so it can't hang/time out
  // (you can raise this later once caching is in and stable)
  const scan = clampInt(req.query.scan, 25, 5, 100);

  const debug = String(req.query.debug || "") === "1";

  const cacheKey = `${limit}:${days}:${scan}`;
  const now = Date.now();

  if (cache.key === cacheKey && now - cache.ts < CACHE_TTL_MS) {
    if (debug) return res.status(200).json({ data: cache.data, debug: { ...cache.debug, cache: "hit-fresh" } });
    return res.status(200).json(cache.data);
  }

  const headers = {
    "User-Agent": ua,
    "Accept-Encoding": "gzip, deflate, br",
    Accept: "application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
  };

  const cutoffIso = daysAgoIso(days);

  try {
    const atom = await fetchText(BASE_ATOM, headers);

    if (!atom.ok) {
      if ((atom.status === 429 || atom.status === 403) && cache.data?.length) {
        if (debug) {
          return res.status(200).json({
            data: cache.data,
            debug: { cache: "hit-stale", reason: `SEC Atom fetch failed (${atom.status})` },
          });
        }
        return res.status(200).json(cache.data);
      }
      return res.status(atom.status).json({
        error: `SEC Atom fetch failed (${atom.status})`,
        hint: "SEC rate limited you. Try again later.",
      });
    }

    const entries = extractAllEntries(atom.text);
    const out = [];
    const errors = [];

    let filingsScanned = 0;

    for (const entryXml of entries) {
      if (out.length >= limit) break;
      if (filingsScanned >= scan) break;

      const updated = extractTag(entryXml, "updated") || extractTag(entryXml, "published");
      if (updated && new Date(updated).toISOString() < cutoffIso) continue;

      const hrefs = extractLinksFromEntry(entryXml);
      const indexUrl = pickFilingIndexHtmlLink(hrefs);
      if (!indexUrl) continue;

      filingsScanned++;

      try {
        const indexHtml = await fetchFilingIndexHtml(indexUrl, headers);
        const form4Xml = await fetchForm4Xml(indexUrl, indexHtml, headers);
        if (!form4Xml) continue;

        const issuerName = extractTagValue(form4Xml, "issuerName") || extractTag(form4Xml, "issuerName");
        const issuerTradingSymbol =
          extractTagValue(form4Xml, "issuerTradingSymbol") || extractTag(form4Xml, "issuerTradingSymbol");

        const reportingOwnerName =
          extractTagValue(form4Xml, "rptOwnerName") || extractTag(form4Xml, "rptOwnerName");
        const officerTitle = extractTagValue(form4Xml, "officerTitle") || extractTag(form4Xml, "officerTitle");

        const role = officerTitle || null;
        const txs = parseTransactionsFromForm4Xml(form4Xml);

        for (const t of txs) {
          if (out.length >= limit) break;

          const totalValue =
            typeof t.shares === "number" && typeof t.pricePerShare === "number"
              ? t.shares * t.pricePerShare
              : null;

          out.push({
            id: `${(reportingOwnerName || "insider").toLowerCase().replace(/\s+/g, "-")}-${issuerTradingSymbol || "na"}-${t.transactionDate || "na"}`,
            insiderName: reportingOwnerName || null,
            insiderTitle: role,
            employerTicker: issuerTradingSymbol || null,
            employerCompany: issuerName || null,
            purchasedTicker: issuerTradingSymbol || null,
            purchasedCompany: issuerName || null,
            shares: t.shares,
            pricePerShare: t.pricePerShare,
            totalValue,
            transactionDate: t.transactionDate || null,
            signalScore: computeSignalScore(totalValue, role),
            purchaseType: "own-company",
          });
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        errors.push({ indexUrl, message: msg });
      }
    }

    cache = {
      ts: now,
      key: cacheKey,
      data: out,
      debug: {
        cache: "miss-refresh",
        returned: out.length,
        entriesSeen: entries.length,
        filingsScanned,
        scanCap: scan,
        cutoff: cutoffIso,
        errorsSample: errors.slice(0, 10),
      },
    };

    if (debug) return res.status(200).json({ data: out, debug: cache.debug });
    return res.status(200).json(out);
  } catch (e) {
    return res.status(500).json({ error: e instanceof Error ? e.message : "Unknown error" });
  }
}
