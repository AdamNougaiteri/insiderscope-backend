// /api/insider-buys.js
// Global Form 4 insider BUY parser (debug-friendly)

export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 90;
    const now = new Date();
    const cutoff = new Date(now.getTime() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000);

    const indexUrl = "https://data.sec.gov/submissions/CIK0000000000.json";

    // SEC index of recent filings
    const feedRes = await fetch(
      "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
      {
        headers: {
          "User-Agent": "InsiderScope (contact: you@example.com)",
          Accept: "application/json",
        },
      }
    );

    // Use daily master index instead (simpler + reliable)
    const masterRes = await fetch(
      "https://www.sec.gov/Archives/edgar/daily-index/master.idx",
      {
        headers: {
          "User-Agent": "InsiderScope (contact: you@example.com)",
        },
      }
    );

    const text = await masterRes.text();
    const lines = text.split("\n").slice(11); // skip header

    const results = [];

    for (const line of lines) {
      if (!line.includes("|4|")) continue;

      const parts = line.split("|");
      const cik = parts[0];
      const date = new Date(parts[3]);
      const path = parts[4];

      if (date < cutoff) continue;

      const xmlUrl = `https://www.sec.gov/Archives/${path.replace(
        ".txt",
        "/primary_doc.xml"
      )}`;

      const xmlRes = await fetch(xmlUrl, {
        headers: {
          "User-Agent": "InsiderScope (contact: you@example.com)",
          Accept: "application/xml",
        },
      });

      if (!xmlRes.ok) continue;

      const xml = await xmlRes.text();

      if (!xml.includes("<transactionCode>P</transactionCode>")) continue;

      const get = (tag) => {
        const m = xml.match(new RegExp(`<${tag}>(.*?)</${tag}>`));
        return m ? m[1] : null;
      };

      const shares = parseFloat(get("transactionShares")) || 0;
      const price = parseFloat(get("transactionPricePerShare")) || 0;

      results.push({
        cik,
        companyName: get("issuerName") || "Unknown",
        ticker: get("issuerTradingSymbol") || "—",
        insiderName: get("rptOwnerName") || "Unknown",
        insiderTitle: get("officerTitle") || "Insider",
        shares,
        pricePerShare: price,
        totalValue: shares * price,
        transactionDate: get("transactionDate") || parts[3],
      });

      if (results.length >= 25) break; // safety limit
    }

    res.status(200).json(results);
  } catch (err) {
    res.status(500).json({
      error: "Failed to fetch Form 4 data",
      message: err.message,
    });
  }
}
