import { XMLParser } from "fast-xml-parser";

const FEED_URL =
  "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=20&output=atom";

const HEADERS = {
  "User-Agent": "InsiderScope demo contact@example.com",
  Accept: "application/xml",
};

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
});

async function fetchXML(url) {
  const r = await fetch(url, { headers: HEADERS });
  if (!r.ok) throw new Error(`Fetch failed ${r.status}`);
  return r.text();
}

export default async function handler(req, res) {
  try {
    const feedXML = await fetchXML(FEED_URL);
    const feed = parser.parse(feedXML);

    const entries = Array.isArray(feed.feed.entry)
      ? feed.feed.entry
      : [feed.feed.entry];

    const results = [];

    for (const entry of entries) {
      const filingUrl = entry.link?.href;
      if (!filingUrl) continue;

      // Load filing page
      const filingPage = await fetchXML(filingUrl);

      // Find XML document link
      const xmlMatch = filingPage.match(
        /href="([^"]+\.xml)"/i
      );
      if (!xmlMatch) continue;

      const xmlUrl = `https://www.sec.gov${xmlMatch[1]}`;
      const formXML = await fetchXML(xmlUrl);
      const form = parser.parse(formXML);

      const issuer = form?.ownershipDocument?.issuer;
      const reportingOwner =
        form?.ownershipDocument?.reportingOwner?.reportingOwnerId;

      const transactions =
        form?.ownershipDocument?.nonDerivativeTable?.nonDerivativeTransaction;

      const txns = Array.isArray(transactions)
        ? transactions
        : transactions
        ? [transactions]
        : [];

      for (const t of txns) {
        if (t.transactionCoding?.transactionCode !== "P") continue;

        const shares = Number(
          t.transactionAmounts?.transactionShares?.value || 0
        );
        const price = Number(
          t.transactionAmounts?.transactionPricePerShare?.value || 0
        );

        results.push({
          id: entry.id,
          companyName: issuer?.issuerName || "Unknown",
          ticker: issuer?.issuerTradingSymbol || "UNKNOWN",
          insiderName:
            reportingOwner?.rptOwnerName || "Unknown",
          insiderTitle: "Insider",
          shares,
          pricePerShare: price,
          totalValue: shares * price,
          transactionDate: entry.updated,
        });
      }
    }

    results.sort((a, b) => b.totalValue - a.totalValue);
    res.status(200).json(results);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to load Form 4 data" });
  }
}
