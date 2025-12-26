// api/insider-buys.js
// Returns recent insider BUY transactions from SEC + enriches with a simple signal score.
// Query params:
//   ?limit=50        -> number of rows to return (default 50, max 200)
//   ?days=30         -> how many days back to look (default 30, max 180)
//   ?debug=1         -> includes debug block in response
//
// Requires env var:
//   SEC_USER_AGENT = "YourName your_email+insiderscope@gmail.com"

const BASE_ATOM =
  "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=100&output=atom";

// --- helpers ---
function clampInt(v, def, min, max) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function isoDateOnly(d) {
  // YYYY-MM-DD
  return new Date(d).toISOString().slice(0, 10);
}

function daysAgoIso(days) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString();
}

async function fetchText(url, headers) {
  const r = await fetch(url, { headers });
  const t = await r.text();
  return { ok: r.ok, status: r.status, text: t };
}

function extractTag(xml, tag) {
  // very small XML extractor; good enough for SEC atom fields we use
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const m = xml.match(re);
  return m ? m[1].trim() : null;
}

function extractAllEntries(atomXml) {
  // Split on <entry>...</entry>
  const entries = [];
  const re = /<entry>([\s\S]*?)<\/entry>/gi;
  let m;
  while ((m = re.exec(atomXml))) entries.push(m[1]);
  return entries;
}

function extractLinksFromEntry(entryXml) {
  // Find all href="..."
  const hrefs = [];
  const re = /href="([^"]+)"/gi;
  let m;
  while ((m = re.exec(entryXml))) hrefs.push(m[1]);
  return hrefs;
}

function pickFilingIndexHtmlLink(hrefs) {
  // SEC pages: prefer "-index.html"
  const idx = hrefs.find((h) => /-index\.html$/i.test(h));
  return idx || hrefs[0] || null;
}

async function fetchFilingIndexHtml(indexUrl, headers) {
  const { ok, status, text } = await fetchText(indexUrl, headers);
  if (!ok) throw new Error(`SEC index fetch failed ${status}`);
  return text;
}

function parseAccessionFromIndexUrl(indexUrl) {
  // example: https://www.sec.gov/Archives/edgar/data/320193/000032019325000123/0000320193-25-000123-index.html
  const m = indexUrl.match(/\/(\d{10}-\d{2}-\d{6})-index\.html$/i);
  return m ? m[1] : null;
}

function parseCikFromIndexUrl(indexUrl) {
  const m = indexUrl.match(/\/data\/(\d+)\//i);
  return m ? m[1] : null;
}

function findXmlPrimaryDoc(indexHtml) {
  // Look for an XML doc link in the table (Form 4 XML usually ends with .xml)
  const re = /href="([^"]+\.xml)"/gi;
  let m;
  while ((m = re.exec(indexHtml))) {
    const href = m[1];
    // ignore exhibit xmls; prefer ones that look like primary doc
    if (/\.xml$/i.test(href)) return href;
  }
  return null;
}

async function fetchForm4Xml(indexUrl, indexHtml, headers) {
  let xmlHref = findXmlPrimaryDoc(indexHtml);
  if (!xmlHref) return null;

  // SEC href can be relative
  if (xmlHref.startsWith("/")) {
    xmlHref = `https://www.sec.gov${xmlHref}`;
  } else if (!xmlHref.startsWith("http")) {
    const base = new URL(indexUrl);
    xmlHref = `${base.origin}${xmlHref.startsWith("/") ? "" : "/"}${xmlHref}`;
  }

  const { ok, status, text } = await fetchText(xmlHref, headers);
  if (!ok) throw new Error(`SEC XML fetch failed ${status}`);
  return text;
}

function parseTransactionsFromForm4Xml(form4Xml) {
  // We’ll extract NON-DERIVATIVE transactions with transactionCode=P (purchase)
  // Minimal parsing: pull out blocks by <nonDerivativeTransaction>...</nonDerivativeTransaction>
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

function safeNum(n) {
  return Number.isFinite(n) ? n : null;
}

function computeSignalScore(totalValue, role) {
  // Very simple placeholder scoring so UI looks consistent.
  // You can replace this later with your real model.
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

// --- handler ---
export default async function handler(req, res) {
  try {
    const ua = process.env.SEC_USER_AGENT;
    if (!ua) {
      return res.status(500).json({
        error:
          "Missing SEC_USER_AGENT env var. Set it in Vercel project settings.",
      });
    }

    const limit = clampInt(req.query.limit, 50, 1, 200);
    const days = clampInt(req.query.days, 30, 1, 180);
    const debug = String(req.query.debug || "") === "1";

    const headers = {
      "User-Agent": ua,
      "Accept-Encoding": "gzip, deflate, br",
      Accept: "application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
    };

    // 1) Pull current Form 4 filings feed (Atom)
    const atom = await fetchText(BASE_ATOM, headers);
    if (!atom.ok) {
      return res.status(atom.status).json({
        error: `SEC Atom fetch failed (${atom.status})`,
        hint:
          "If 403, SEC is blocking the request. Double-check SEC_USER_AGENT format and slow down.",
      });
    }

    const entries = extractAllEntries(atom.text);

    // 2) For each entry, get the filing index page, then fetch the Form 4 XML and parse buys.
    const out = [];
    const errors = [];
    const cutoffIso = daysAgoIso(days);

    for (const entryXml of entries) {
      if (out.length >= limit) break;

      // Optional: filter by Atom updated/published date if present
      const updated = extractTag(entryXml, "updated") || extractTag(entryXml, "published");
      if (updated && new Date(updated).toISOString() < cutoffIso) continue;

      const hrefs = extractLinksFromEntry(entryXml);
      const indexUrl = pickFilingIndexHtmlLink(hrefs);
      if (!indexUrl) continue;

      let indexHtml, form4Xml;
      try {
        indexHtml = await fetchFilingIndexHtml(indexUrl, headers);
        form4Xml = await fetchForm4Xml(indexUrl, indexHtml, headers);
        if (!form4Xml) continue;

        // issuer / insider details
        const issuerName = extractTag(form4Xml, "issuerName");
        const issuerTradingSymbol = extractTag(form4Xml, "issuerTradingSymbol");

        const reportingOwnerName = extractTag(form4Xml, "rptOwnerName");
        const officerTitle = extractTag(form4Xml, "officerTitle");
        const isDirector = extractTag(form4Xml, "isDirector");
        const isOfficer = extractTag(form4Xml, "isOfficer");

        let role = officerTitle || "";
        if (!role) {
          if (String(isDirector).toLowerCase() === "1" || String(isDirector).toLowerCase() === "true") role = "Director";
          else if (String(isOfficer).toLowerCase() === "1" || String(isOfficer).toLowerCase() === "true") role = "Officer";
        }

        const cik = parseCikFromIndexUrl(indexUrl);
        const accession = parseAccessionFromIndexUrl(indexUrl);

        const txs = parseTransactionsFromForm4Xml(form4Xml);

        for (const t of txs) {
          if (out.length >= limit) break;

          const totalValue = safeNum(
            typeof t.shares === "number" && typeof t.pricePerShare === "number"
              ? t.shares * t.pricePerShare
              : null
          );

          out.push({
            id: `${(reportingOwnerName || "insider").toLowerCase().replace(/\s+/g, "-")}-${issuerTradingSymbol || "na"}-${(t.transactionDate || isoDateOnly(new Date())).replace(/-/g, "-")}`,
            cik: cik || null,
            ticker: issuerTradingSymbol || null,
            company: issuerName || null,
            insider: reportingOwnerName || null,
            role: role || null,
            shares: t.shares,
            price: t.pricePerShare,
            value: totalValue,
            filingDate: t.transactionDate || null,
            signalScore: computeSignalScore(totalValue, role),

            // keep your newer schema too (so your current UI keeps working)
            insiderName: reportingOwnerName || null,
            insiderTitle: role || null,
            employerTicker: issuerTradingSymbol || null,
            employerCompany: issuerName || null,
            purchasedTicker: issuerTradingSymbol || null,
            purchasedCompany: issuerName || null,
            pricePerShare: t.pricePerShare,
            totalValue,
            transactionDate: t.transactionDate || null,

            purchaseType: "own-company",
          });
        }
      } catch (e) {
        errors.push({
          indexUrl,
          message: e instanceof Error ? e.message : String(e),
        });
      }
    }

    // 3) Return
    if (debug) {
      return res.status(200).json({
        data: out,
        debug: {
          requested: { limit, days },
          returned: out.length,
          entriesSeen: entries.length,
          cutoff: cutoffIso,
          errorsSample: errors.slice(0, 10),
        },
      });
    }

    return res.status(200).json(out);
  } catch (e) {
    return res.status(500).json({
      error: e instanceof Error ? e.message : "Unknown error",
    });
  }
}
