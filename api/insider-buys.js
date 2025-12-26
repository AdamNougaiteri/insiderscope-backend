import { XMLParser } from "fast-xml-parser";

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
});

const SEC_HEADERS = {
  "User-Agent": "InsiderScope demo contact@example.com",
  "Accept": "application/xml,text/xml",
};

export default async function handler(req, res) {
  try {
    const feedUrl =
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=40&output=atom";

    const feedRes = await fetch(feedUrl, { headers: SEC_HEADERS });
    const feedText = await feedRes.text();
    const feed = parser.parse(feedText);

    const entries = Array.isArray(feed.feed.entry)
      ? feed.feed.entry
      : [feed.feed.entry];

    const results = [];

    for (const entry of entries.slice(0, 15)) {
      const accessionUrl = entry.link.href.replace("-index.htm", ".xml");

      try {
        const filingRes = await fetch(accessionUrl, { headers: SEC_HEADERS });
        const filingText = await filingRes.text();
        const filing = parser.parse(filingText);

        const doc = filing.ownershipDocument;
        if (!doc) continue;

        const issuer = doc.issuer || {};
        const owner = doc.reportingOwner || {};
        const txns =
          doc.nonDerivativeTable?.nonDerivativeTransaction || [];

        const txnList = Array.isArray(txns) ? txns : [txns];

        for (const t of txnList) {
          if (
            t.transactionCoding?.transactionCode !== "P" ||
            t.transactionAmounts?.transactionAcquiredDisposedCode?.value !== "A"
          ) {
            continue;
          }

          const shares = Number(
            t.transactionAmounts?.transactionShares?.value || 0
          );
          const price = Number(
            t.transactionAmounts?.transactionPricePerShare?.value || 0
          );

          if (shares <= 0 || price <= 0) continue;

          results.push({
            id: entry.id,
            companyName: issuer.issuerName || "Unknown",
            ticker: issuer.issuerTradingSymbol || "UNKNOWN",
            insiderName:
              owner.reportingOwnerId?.rptOwnerName || "Unknown",
            insiderTitle:
              owner.reportingOwnerRelationship?.officerTitle ||
              "Insider",
            shares,
            pricePerShare: price,
            totalValue: shares * price,
            transactionDate:
              t.transactionDate?.value || entry.updated,
          });
        }
      } catch {
        continue;
      }
    }

    res.status(200).json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
