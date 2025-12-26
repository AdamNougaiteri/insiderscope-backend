import { XMLParser } from "fast-xml-parser";

const SEC_FEED =
  "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=40&output=atom";

const HEADERS = {
  "User-Agent": "InsiderScope demo contact@example.com",
  Accept: "application/xml",
};

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
});

// -------------------- helpers --------------------

function cleanText(v) {
  if (!v) return "";
  return String(v).replace(/\s+/g, " ").trim();
}

function parseNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function extractCompanyAndTicker(raw) {
  if (!raw) return { companyName: "Unknown", ticker: "UNKNOWN" };

  // Examples:
  // "4 - ONDAS HOLDINGS INC (0001646188)"
  // "4 - IonQ, Inc. (0001824920)"

  const cleaned = cleanText(raw);

  // remove leading "4 -"
  const noPrefix = cleaned.replace(/^4\s*-\s*/i, "");

  // remove (CIK)
  const companyOnly = noPrefix.replace(/\(\d+\)/g, "").trim();

  return {
    companyName: companyOnly || "Unknown",
    ticker: "UNKNOWN", // real ticker resolution comes later
  };
}

// -------------------- handler --------------------

export default async function handler(req, res) {
  try {
    const r = await fetch(SEC_FEED, { headers: HEADERS });
    if (!r.ok) throw new Error(`SEC error ${r.status}`);

    const xml = await r.text();
    const feed = parser.parse(xml);

    const entries = Array.isArray(feed.feed.entry)
      ? feed.feed.entry
      : [feed.feed.entry];

    const transactions = [];

    for (const e of entries) {
      const title = cleanText(e.title);
      const summary = cleanText(e.summary);

      const { companyName, ticker } = extractCompanyAndTicker(title);

      const sharesMatch = summary.match(/Shares:\s*([\d,]+)/i);
      const priceMatch = summary.match(/Price:\s*\$?([\d.]+)/i);

      const shares = parseNumber(
        sharesMatch ? sharesMatch[1].replace(/,/g, "") : 0
      );
      const pricePerShare = parseNumber(priceMatch ? priceMatch[1] : 0);
      const totalValue = shares * pricePerShare;

      transactions.push({
        id: e.id || crypto.randomUUID(),
        companyName,
        ticker,
        insiderName: cleanText(e.author?.name),
        insiderTitle: "Insider",
        shares,
        pricePerShare,
        totalValue,
        transactionDate: e.updated,
      });
    }

    // sort by largest dollar value
    transactions.sort((a, b) => b.totalValue - a.totalValue);

    res.status(200).json(transactions);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to load insider buys" });
  }
}
