// lib/secDataServices.js
import { XMLParser } from "fast-xml-parser";

/**
 * SEC requires a descriptive User-Agent with a way to contact you if your traffic causes issues.
 * We'll use your Gmail + tag.
 */
const SEC_USER_AGENT = "InsiderScope (personal project) foodiepostig+insiderscope@gmail.com";

/**
 * Lightweight in-memory cache (works well on warm Vercel lambdas).
 * Prevents repeated refresh clicks from hammering SEC and crashing the function.
 */
const CACHE_TTL_MS = 60 * 1000; // 60s
const cache = globalThis.__INSIDERSCOPE_CACHE__ ?? { ts: 0, key: "", data: null };
globalThis.__INSIDERSCOPE_CACHE__ = cache;

function now() {
  return Date.now();
}

function toArray(x) {
  if (!x) return [];
  return Array.isArray(x) ? x : [x];
}

function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function normalizeCik(cik) {
  const s = String(cik ?? "").replace(/\D/g, "");
  return s.padStart(10, "0");
}

async function fetchSec(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": SEC_USER_AGENT,
      "Accept": "*/*",
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`SEC fetch failed ${res.status} ${res.statusText} for ${url} :: ${text.slice(0, 300)}`);
  }
  return res;
}

/**
 * Limit concurrency so Vercel doesn't explode and SEC doesn't throttle you.
 */
async function mapLimit(items, limit, mapper) {
  const results = new Array(items.length);
  let i = 0;

  async function worker() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await mapper(items[idx], idx);
    }
  }

  const workers = Array.from({ length: Math.min(limit, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

/**
 * Pull latest Form 4 filings from SEC Atom feed.
 * We intentionally keep this small and fast.
 */
async function getRecentForm4Entries({ count = 50 } = {}) {
  const atomUrl =
    `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=${encodeURIComponent(
      count
    )}&output=atom`;

  const atomText = await (await fetchSec(atomUrl)).text();
  // Atom is XML; reuse XMLParser
  const parser = new XMLParser({ ignoreAttributes: false });
  const parsed = parser.parse(atomText);

  const feed = parsed?.feed;
  const entries = toArray(feed?.entry);

  return entries
    .map((e) => {
      const links = toArray(e?.link);
      const alternate = links.find((l) => l?.["@_rel"] === "alternate") ?? links[0];
      const href = alternate?.["@_href"];

      // Example href:
      // https://www.sec.gov/Archives/edgar/data/320193/000032019325000XXX/...
      // We'll keep it and later derive folder.
      const title = e?.title ?? "";
      const updated = e?.updated ?? null;

      return { href, title, updated };
    })
    .filter((x) => typeof x.href === "string" && x.href.includes("/Archives/edgar/data/"));
}

function deriveArchivesFolderFromFilingHref(href) {
  // Atom alternate link usually points to the filing detail page.
  // We want the /Archives/edgar/data/{cik}/{accession_no_no_dashes}/ folder.
  // The href often already contains it; if it doesn't, this function may fail.
  const m = href.match(/\/Archives\/edgar\/data\/(\d+)\/(\d{18,})\//);
  if (!m) return null;
  const cik = m[1];
  const accessionNoNoDashes = m[2];
  return `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNoNoDashes}/`;
}

async function pickForm4XmlFromIndex(folderUrl) {
  const indexUrl = folderUrl.endsWith("/") ? `${folderUrl}index.json` : `${folderUrl}/index.json`;
  const idxJson = await (await fetchSec(indexUrl)).json();

  const items = idxJson?.directory?.item ?? [];
  const xmlCandidates = items
    .map((it) => it?.name)
    .filter((name) => typeof name === "string" && name.toLowerCase().endsWith(".xml"));

  // Prefer common Form 4 XML names
  const preferred = xmlCandidates.find((n) => /form4|doc4|primarydocument/i.test(n));
  const chosen = preferred ?? xmlCandidates[0];

  if (!chosen) return null;
  return folderUrl.endsWith("/") ? `${folderUrl}${chosen}` : `${folderUrl}/${chosen}`;
}

function parseForm4Xml(xmlText) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    // This helps if a node sometimes appears once vs many times
    isArray: (name) => {
      return [
        "reportingOwner",
        "nonDerivativeTransaction",
        "derivativeTransaction",
        "nonDerivativeHolding",
        "derivativeHolding",
      ].includes(name);
    },
  });

  const doc = parser.parse(xmlText);
  // Most Form 4 XML uses <ownershipDocument> root
  const od = doc?.ownershipDocument;
  if (!od) throw new Error("Unexpected Form 4 XML format: missing ownershipDocument");

  const issuer = od?.issuer ?? {};
  const issuerCik = normalizeCik(issuer?.issuerCik);
  const issuerName = issuer?.issuerName ?? null;
  const issuerTicker = issuer?.issuerTradingSymbol ?? null;

  const reportingOwners = toArray(od?.reportingOwner);

  const owners = reportingOwners.map((ro) => {
    const rid = ro?.reportingOwnerId ?? {};
    const rel = ro?.reportingOwnerRelationship ?? {};

    const ownerCik = normalizeCik(rid?.rptOwnerCik);
    const ownerName = rid?.rptOwnerName ?? null;

    const isDirector = String(rel?.isDirector ?? "").toLowerCase() === "true";
    const isOfficer = String(rel?.isOfficer ?? "").toLowerCase() === "true";
    const isTenPercentOwner = String(rel?.isTenPercentOwner ?? "").toLowerCase() === "true";
    const isOther = String(rel?.isOther ?? "").toLowerCase() === "true";
    const officerTitle = rel?.officerTitle ?? null;
    const otherText = rel?.otherText ?? null;

    let affiliationType = "Other";
    if (isOfficer) affiliationType = "Officer";
    else if (isDirector) affiliationType = "Director";
    else if (isTenPercentOwner) affiliationType = "10% Owner";
    else if (isOther) affiliationType = "Other";

    // If officerTitle exists and includes CEO/CFO/etc, we can display it.
    return {
      reportingOwnerCik: ownerCik,
      reportingOwnerName: ownerName,
      relationship: {
        affiliationType,
        isDirector,
        isOfficer,
        isTenPercentOwner,
        isOther,
        officerTitle,
        otherText,
      },
    };
  });

  const filingDate = od?.periodOfReport ?? null;

  const nonDeriv = od?.nonDerivativeTable ?? {};
  const nonDerivTx = toArray(nonDeriv?.nonDerivativeTransaction);

  // Extract purchases (transactionCode === "P")
  const purchases = nonDerivTx
    .map((tx) => {
      const coding = tx?.transactionCoding ?? {};
      const amounts = tx?.transactionAmounts ?? {};
      const post = tx?.postTransactionAmounts ?? {};
      const ownership = tx?.ownershipNature ?? {};

      const code = coding?.transactionCode ?? null;
      if (code !== "P") return null;

      const shares = safeNum(amounts?.transactionShares?.value);
      const price = safeNum(amounts?.transactionPricePerShare?.value);
      const acquiredDisposed = amounts?.transactionAcquiredDisposedCode?.value ?? null;

      // Some filings omit price for certain purchase types; handle gracefully.
      const value = shares != null && price != null ? shares * price : null;

      const directOrIndirect = ownership?.directOrIndirectOwnership?.value ?? null;
      const nature = ownership?.natureOfOwnership?.value ?? null;

      const sharesOwnedAfter = safeNum(post?.sharesOwnedFollowingTransaction?.value);

      const transactionDate = tx?.transactionDate?.value ?? null;

      return {
        transactionCode: code,
        acquiredDisposed,
        transactionDate,
        shares,
        price,
        value,
        sharesOwnedAfter,
        directOrIndirect,
        nature,
      };
    })
    .filter(Boolean);

  return {
    issuer: { issuerCik, issuerName, issuerTicker },
    owners,
    filingDate,
    purchases,
  };
}

/**
 * A simple score for now:
 * - more $ value => higher
 * - officer purchases get a bump vs director
 * You can replace this later with your real model.
 */
function computeSignalScore({ value, affiliationType, officerTitle }) {
  const v = value ?? 0;
  const logv = Math.log10(Math.max(v, 1)); // 0..~8
  let score = 50 + logv * 10; // rough scale

  if (affiliationType === "Officer") score += 10;
  if (affiliationType === "Director") score += 5;

  const t = String(officerTitle ?? "").toLowerCase();
  if (t.includes("chief executive") || t.includes("ceo")) score += 10;
  if (t.includes("chief financial") || t.includes("cfo")) score += 6;

  score = Math.round(Math.max(1, Math.min(99, score)));
  return score;
}

export async function fetchRecentInsiderBuys({
  limit = 25,
  minValue = 0,
  affiliation = "any", // any | Officer | Director | 10% Owner | Other
} = {}) {
  const key = JSON.stringify({ limit, minValue, affiliation });
  if (cache.data && cache.key === key && now() - cache.ts < CACHE_TTL_MS) {
    return cache.data;
  }

  const entries = await getRecentForm4Entries({ count: Math.max(limit * 3, 50) });

  // Convert entries to folders
  const folders = entries
    .map((e) => deriveArchivesFolderFromFilingHref(e.href))
    .filter(Boolean);

  // We’ll scan a limited number of filings to stay within runtime.
  const scanFolders = folders.slice(0, Math.max(limit * 2, 40));

  const rowsNested = await mapLimit(scanFolders, 3, async (folderUrl) => {
    try {
      const xmlUrl = await pickForm4XmlFromIndex(folderUrl);
      if (!xmlUrl) return [];

      const xmlText = await (await fetchSec(xmlUrl)).text();
      const parsed = parseForm4Xml(xmlText);

      const issuer = parsed.issuer;
      const filingDate = parsed.filingDate;

      // Most Form 4s have 1 reporting owner; handle multiple just in case:
      const results = [];
      for (const owner of parsed.owners) {
        const rel = owner.relationship;

        // Filter by affiliation if requested
        if (affiliation !== "any" && rel.affiliationType !== affiliation) continue;

        for (const p of parsed.purchases) {
          const value = p.value ?? 0;
          if (value < minValue) continue;

          const signalScore = computeSignalScore({
            value,
            affiliationType: rel.affiliationType,
            officerTitle: rel.officerTitle,
          });

          results.push({
            issuerCik: issuer.issuerCik,
            ticker: issuer.issuerTicker,
            company: issuer.issuerName,

            insider: owner.reportingOwnerName,
            insiderCik: owner.reportingOwnerCik,

            // Role relative to the ISSUER (this fixes the Tim Cook issue)
            affiliationType: rel.affiliationType,
            role: rel.officerTitle || (rel.affiliationType === "Director" ? "Director" : rel.affiliationType),

            shares: p.shares,
            price: p.price,
            value: p.value,

            filingDate: filingDate,
            transactionDate: p.transactionDate,

            signalScore,
            // Useful for future UX
            relationship: rel,
          });
        }
      }

      return results;
    } catch (err) {
      // Swallow per-filing failures to avoid killing the whole endpoint.
      return [];
    }
  });

  const rows = rowsNested.flat();

  // Sort highest score then highest value
  rows.sort((a, b) => (b.signalScore - a.signalScore) || ((b.value ?? 0) - (a.value ?? 0)));

  const trimmed = rows.slice(0, limit);

  cache.ts = now();
  cache.key = key;
  cache.data = trimmed;

  return trimmed;
}
