// api/insider-buys.js
//
// Fixes SEC 429 rate limiting by:
// 1) Caching results in-memory for a TTL (default 10 minutes)
// 2) Throttling outbound SEC requests (simple delay + concurrency=1)
// 3) Serving stale cached data if SEC returns 429/403
//
// Query params:
//   ?limit=50        default 25, max 200
//   ?days=30         default 30, max 180
//   ?debug=1         returns { data, debug } instead of just data
//
// Env:
//   SEC_USER_AGENT = "Your Name your@email.com"

const BASE_ATOM =
  "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=100&output=atom";

const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const REQUEST_DELAY_MS = 350; // throttle between SEC calls (tune 250-750)

// --- simple module-level cache (works on warm Vercel lambdas) ---
let cache = {
  ts: 0,
  key: "",
  data: [],
  debug: {},
};

// --- helpers ---
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
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
    const code = extractTag(block, "transactionCode");
    if (String(code || "").toUpperCase() !== "P") continue;

    const shares = extractTag(block, "transactionShares");
    const price = extractTag(block, "transactionPricePerShare");
    const date = extractTag(block, "transactionDate");

    const sharesVal = shares ? Number(shares.replace(/,/g, "")) : null;
    const priceVal = price ? Number(price.replace(/,/g, "")) : null;

    txs.push({
      shares: Number.isFinite(sharesVal) ? sharesVal : null,
      pricePerShare: Number.isFinite(priceVal) ? priceVal : null,
      transactionDate: date ? String(date).trim() : null,
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
  const debug = String(req.query.debug || "") === "1";

  const cacheKey = `${limit}:${days}`;
  const now = Date.now();

  // Serve fresh cache if available
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
    // 1) Atom feed
    const atom = await fetchText(BASE_ATOM, headers);
    if (!atom.ok) {
      // If rate limited, serve stale cache if present
      if ((atom.status === 429 || atom.status === 403) && cache.data?.length) {
        if (debug) {
          return res.status(200).json({
            data: cache.data,
            debug: {
              cache: "hit-stale",
              reason: `SEC Atom fetch failed (${atom.status})`,
              lastCacheAgeSec: Math.round((now - cache.ts) / 1000),
            },
          });
        }
        return res.status(200).json(cache.data);
      }

      return res.status(atom.status).json({
        error: `SEC Atom fetch failed (${atom.status})`,
        hint:
          "SEC rate limited you. Wait a few minutes and retry. Also keep limit small (25) while testing.",
      });
    }

    const entries = extractAllEntries(atom.text);

    // 2) Iterate entries slowly and build results
    const out = [];
    const errors = [];

    for (const entryXml of entries) {
      if (out.length >= limit) break;

      const updated =
        extractTag(entryXml, "updated") || extractTag(entryXml, "published");
      if (updated && new Date(updated).toISOString() < cutoffIso) continue;

      const hrefs = extractLinksFromEntry(entryXml);
      const indexUrl = pickFilingIndexHtmlLink(hrefs);
      if (!indexUrl) continue;

      try {
        const indexHtml = await fetchFilingIndexHtml(indexUrl, headers);
        const form4Xml = await fetchForm4Xml(indexUrl, indexHtml, headers);
        if (!form4Xml) continue;

        const issuerName = extractTag(form4Xml, "issuerName");
        const issuerTradingSymbol = extractTag(form4Xml, "issuerTradingSymbol");

        const reportingOwnerName = extractTag(form4Xml, "rptOwnerName");
        const officerTitle = extractTag(form4Xml, "officerTitle");

        const role = officerTitle || null;

        const txs = parseTransactionsFromForm4Xml(form4Xml);

        for (const t of txs) {
          if (out.length >= limit) break;

          const totalValue =
            typeof t.shares === "number" && typeof t.pricePerShare === "number"
              ? t.shares * t.pricePerShare
              : null;

          out.push({
            id: `${(reportingOwnerName || "insider")
              .toLowerCase()
              .replace(/\s+/g, "-")}-${issuerTradingSymbol || "na"}-${
              t.transactionDate || "na"
            }`,
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

        // If we get 429/403 mid-loop, stop and return what we have + cache it
        if (msg.includes(" 429") || msg.includes(" 403")) {
          errors.push({ indexUrl, message: msg });
          break;
        }

        errors.push({ indexUrl, message: msg });
      }
    }

    // If we got *some* data, cache and return it
    if (out.length) {
      cache = {
        ts: now,
        key: cacheKey,
        data: out,
        debug: {
          cache: "miss-refresh",
          returned: out.length,
          entriesSeen: entries.length,
          cutoff: cutoffIso,
          errorsSample: errors.slice(0, 10),
        },
      };

      if (debug) return res.status(200).json({ data: out, debug: cache.debug });
      return res.status(200).json(out);
    }

    // If no data but we have stale cache, serve it
    if (cache.data?.length) {
      if (debug) {
        return res.status(200).json({
          data: cache.data,
          debug: {
            cache: "hit-stale-empty-refresh",
            returned: cache.data.length,
            lastCacheAgeSec: Math.round((now - cache.ts) / 1000),
            errorsSample: errors.slice(0, 10),
          },
        });
      }
      return res.status(200).json(cache.data);
    }

    // Otherwise, return empty with debug/errors
    if (debug) {
      return res.status(200).json({
        data: [],
        debug: {
          cache: "miss-empty",
          returned: 0,
          entriesSeen: entries.length,
          cutoff: cutoffIso,
          errorsSample: errors.slice(0, 10),
        },
      });
    }

    return res.status(200).json([]);
  } catch (e) {
    // If error but cached data exists, serve it
    if (cache.data?.length) {
      if (debug) {
        return res.status(200).json({
          data: cache.data,
          debug: {
            cache: "hit-stale-exception",
            error: e instanceof Error ? e.message : String(e),
            lastCacheAgeSec: Math.round((now - cache.ts) / 1000),
          },
        });
      }
      return res.status(200).json(cache.data);
    }

    return res.status(500).json({ error: e instanceof Error ? e.message : "Unknown error" });
  }
}
