// /api/insider-buys.js
// REAL Form 4 insider BUY parser (Phase A1)

export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 7;
    const now = new Date();
    const cutoff = new Date(now.getTime() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000);

    // Step 1: Get recent Form 4 filings
    const submissionsUrl =
      "https://data.sec.gov/submissions/CIK0000320193.json"; // Apple as seed (SEC allows this)

    const submissionsRes = await fetch(submissionsUrl, {
      headers: {
        "User-Agent": "InsiderScope (contact: you@example.com)",
        Accept: "application/json",
      },
    });

    const submissions = await submissionsRes.json();
    const recent = submissions.filings.recent;

    const results = [];

    for (let i = 0; i < recent.accessionNumber.length; i++) {
      if (recent.form[i] !== "4") continue;

      const filingDate = new Date(recent.filingDate[i]);
      if (filingDate < cutoff) continue;

      const accession = recent.accessionNumber[i].replace(/-/g, "");
      const cik = submissions.cik;
      const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accession}/xslF345X03/primary_doc.xml`;

      const xmlRes = await fetch(xmlUrl, {
        headers: {
          "User-Agent": "InsiderScope (contact: you@example.com)",
          Accept: "application/xml",
        },
      });

      if (!xmlRes.ok) continue;

      const xml = await xmlRes.text();

      // Step 2: Only BUY transactions (P)
      if (!xml.includes("<transactionCode>P</transactionCode>")) continue;

      const get = (tag) => {
        const match = xml.match(new RegExp(`<${tag}>(.*?)</${tag}>`));
        return match ? match[1] : null;
      };

      const shares = parseFloat(get("transactionShares")) || 0;
      const price = parseFloat(get("transactionPricePerShare")) || 0;

      results.push({
        id: accession,
        companyName: get("issuerName") || "Unknown",
        ticker: get("issuerTradingSymbol") || "—",
        insiderName: get("rptOwnerName") || "Unknown",
        insiderTitle: get("officerTitle") || "Insider",
        shares,
        pricePerShare: price,
        totalValue: shares * price,
        transactionDate: get("transactionDate") || recent.filingDate[i],
      });
    }

    res.status(200).json(results);
  } catch (err) {
    res.status(500).json({
      error: "Failed to fetch Form 4 data",
      message: err.message,
    });
  }
}
