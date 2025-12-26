// lib/secDataServices.js
import { XMLParser } from "fast-xml-parser";

const SEC_USER_AGENT = "InsiderScope (personal project) foodiepostig+insiderscope@gmail.com";

// Cache longer to avoid crash loops from refresh spam
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
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
    throw new Error(`SEC fetch failed ${res.status} ${res.statusText} for ${url} :: ${text.slice(0, 200)}`);
  }
  return res;
}

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

async function getRecentForm4Entries({ count = 50 } = {}) {
  const atomUrl =
    `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=${encodeURIComponent(
      count
    )}&output=atom`;

  const atomText = await (await fetchSec(atomUrl)).text();
  const parser = new XMLParser({ ignoreAttributes: false });
  const parsed = parser.parse(atomText);

  const feed = parsed?.feed;
  const entries = toArray(feed?.entry);

  return entries
    .map((e) => {
      const links = toArray(e?.link);
      const alternate = links.find((l) => l?.["@_rel"] === "alternate") ?? links[0];
      const href = alternate?.["@_href"];
      const title = e?.title ?? "";
      const updated = e?.updated ?? null;
      return { href, title, updated };
    })
    .filter((x) => typeof x.href === "string");
}

function deriveArchivesFolderFromFilingHref(href) {
  // We want: /Archives/edgar/data/{cik}/{accessionNoNoDashes}/
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

  const preferred = xmlCandidates.find((n) => /form4|doc4|primarydocument/i.test(n));
  const chosen = preferred ?? xmlCandidates[0];

  if (!chosen) return null;
  return folderUrl.endsWith("/") ? `${folderUrl}${chosen}` : `${folderUrl}/${chosen}`;
}

function parseForm4Xml(xmlText) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
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
      const value = shares != null && price != null ? shares * price : null;

      const transactionDate = tx?.transactionDate?.value ?? null;
      const sharesOwnedAfter = safeNum(post?.sharesOwnedFollowingTransaction?.value);
      const directOrIndirect = ownership?.directOrIndirectOwnership?.value ?? null;

      return {
        transactionCode: code,
        transactionDate,
        shares,
        price,
        value,
        sharesOwnedAfter,
        directOrIndirect,
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

function computeSignalScore({ value, affiliationType, officerTitle }) {
  const v = value ?? 0;
  const logv = Math.log10(Math.max(v, 1));
  let score = 50 + logv * 10;

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
  affiliation = "any",
  debug = false,
  nocache = false,
} = {}) {
  const key = JSON.stringify({ limit, minValue, affiliation });

  // If debugging, always bypass cache so you see the real failure
  if (!debug && !nocache && cache.data && cache.key === key && now() - cache.ts < CACHE_TTL_MS) {
    return cache.data;
  }

  const stats = {
    atomEntries: 0,
    foldersDerived: 0,
    foldersScanned: 0,
    xmlFound: 0,
    form4Parsed: 0,
    purchasesFound: 0,
    rowsEmitted: 0,
  };

  const errors = [];

  const entries = await getRecentForm4Entries({ count: Math.max(limit * 3, 50) });
  stats.atomEntries = entries.length;

  const folders = entries
    .map((e) => deriveArchivesFolderFromFilingHref(e.href))
    .filter(Boolean);

  stats.foldersDerived = folders.length;

  const scanFolders = folders.slice(0, Math.max(limit * 2, 40));
  stats.foldersScanned = scanFolders.length;

  const rowsNested = await mapLimit(scanFolders, 2, async (folderUrl) => {
    try {
      const xmlUrl = await pickForm4XmlFromIndex(folderUrl);
      if (!xmlUrl) return [];

      stats.xmlFound += 1;

      const xmlText = await (await fetchSec(xmlUrl)).text();
      const parsed = parseForm4Xml(xmlText);
      stats.form4Parsed += 1;

      const issuer = parsed.issuer;
      const filingDate = parsed.filingDate;

      const results = [];
      for (const owner of parsed.owners) {
        const rel = owner.relationship;

        if (affiliation !== "any" && rel.affiliationType !== affiliation) continue;

        for (const p of parsed.purchases) {
          stats.purchasesFound += 1;

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

            affiliationType: rel.affiliationType,
            role: rel.officerTitle || (rel.affiliationType === "Director" ? "Director" : rel.affiliationType),

            shares: p.shares,
            price: p.price,
            value: p.value,

            filingDate,
            transactionDate: p.transactionDate,

            signalScore,
          });
        }
      }

      return results;
    } catch (err) {
      if (errors.length < 10) {
        errors.push({
          folderUrl,
          message: err?.message ?? String(err),
        });
      }
      return [];
    }
  });

  const rows = rowsNested.flat();
  stats.rowsEmitted = rows.length;

  rows.sort((a, b) => (b.signalScore - a.signalScore) || ((b.value ?? 0) - (a.value ?? 0)));
  const trimmed = rows.slice(0, limit);

  cache.ts = now();
  cache.key = key;
  cache.data = trimmed;

  if (debug) {
    return {
      data: trimmed,
      debug: stats,
      errorsSample: errors,
      note:
        "If data is empty, check debug.foldersDerived and errorsSample. If foldersDerived=0 your Atom links didn’t match the regex. If errorsSample shows 403, it’s SEC blocking/UA/rate limiting.",
    };
  }

  return trimmed;
}
