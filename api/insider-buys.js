// /api/insider-buys.js
// Vercel-safe Form 4 BUY parser using SEC submissions API

const HEADERS = {
  "User-Agent": "InsiderScope (contact: you@example.com)",
  Accept: "application/json",
};

// High-volume companies to prove data pipe works
const CIKS = [
  "0000320193", // Apple
  "0000789019", // Microsoft
  "0001652044", // Alphabet
  "0001018724", // Amazon
  "0001318605", // Tesla
];

export default async function handler(req, res) {
  try {
    const results = [];

    for (const cik of CIKS) {
      const padded = cik.padStart(10, "0");

      const submissionsUrl = `https://data.sec.gov/submissions/CIK${padded}.json`;
      const subRes = await fetch(submissionsUrl, { headers: HEADERS });

      if (!subRes.ok) continue;

      const subData = await subRes.json();
      const recent = subData.filings?.recent;
      if (!recent) continue;

      const count = recent.form.length;

      for (let i = 0; i < count; i++) {
        if (recent.form[i] !== "4") continue;

        const accession = recent.accessionNumber[i].replace(/-/g, "");
        const filingDate = recent.filingDate[i];
        const baseUrl = `https://www.sec.gov/Archives/edgar/data/${parseInt(
          cik,
          10
        )}/${accession}`;

        const xmlUrl = `${baseUrl}/${recent.primaryDocument[i]}`;

        const xmlRes = await fetch(xmlUrl, {
          headers: {
            "User-Agent": HEADERS["User-Agent"],
            Accept: "application/xml",
          },
        });

        if (!xmlRes.ok) continue;

        const xml = await xmlRes.text();

        // Only BUY transactions
        if (!xml.includes("<transactionCode>P</transactionCode>")) continue;

        const get = (tag) => {
          const m = xml.match(new RegExp(`<${tag}>(.*?)</${tag}>`));
          return m ? m[1] : null;
        };

        const shares = parseFloat(get("transactionShares")) || 0;
        const price = parseFloat(get("transactionPricePerShare")) || 0;

        results.push({
          cik,
          company: get("issuerName"),
          ticker: get("issuerTradingSymbol"),
          insider: get("rptOwnerName"),
          title: get("officerTitle") || "Insider",
          shares,
          price,
          value: shares * price,
          date: filingDate,
        });

        if (results.length >= 20) break;
      }
    }

    res.status(200).json(results);
  } catch (err) {
    res.status(500).json({
      error: "Backend failure",
      message: err.message,
    });
  }
}
