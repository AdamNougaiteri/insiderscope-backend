export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 7; // change to 365 later if you want

    const headers = {
      "User-Agent": "InsiderScope research app (contact@example.com)",
      "Accept-Encoding": "gzip, deflate",
    };

    const now = new Date();
    const cutoff = new Date(now.getTime() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000);

    const cikListUrl =
      "https://www.sec.gov/files/company_tickers.json";
    const cikResp = await fetch(cikListUrl, { headers });
    const cikData = await cikResp.json();

    const results = [];
    let filingsChecked = 0;

    for (const key of Object.keys(cikData)) {
      if (results.length >= 20) break; // keep runtime safe

      const cik = cikData[key].cik_str.toString().padStart(10, "0");

      const submissionsUrl = `https://data.sec.gov/submissions/CIK${cik}.json`;
      const subResp = await fetch(submissionsUrl, { headers });
      if (!subResp.ok) continue;

      const sub = await subResp.json();
      const recent = sub.filings?.recent;
      if (!recent) continue;

      for (let i = 0; i < recent.form.length; i++) {
        if (recent.form[i] !== "4") continue;

        const filingDate = new Date(recent.filingDate[i]);
        if (filingDate < cutoff) continue;

        filingsChecked++;

        const accession = recent.accessionNumber[i].replace(/-/g, "");
        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${parseInt(
          cik
        )}/${accession}/${recent.primaryDocument[i]}`;

        const xmlResp = await fetch(xmlUrl, { headers });
        if (!xmlResp.ok) continue;

        const xml = await xmlResp.text();

        // only open market buys
        if (!xml.includes("<transactionCode>P</transactionCode>")) continue;

        const sharesMatch = xml.match(
          /<transactionShares>[\s\S]*?<value>(.*?)<\/value>/
        );
        const priceMatch = xml.match(
          /<transactionPricePerShare>[\s\S]*?<value>(.*?)<\/value>/
        );
        const nameMatch = xml.match(/<issuerName>(.*?)<\/issuerName>/);
        const insiderMatch = xml.match(/<rptOwnerName>(.*?)<\/rptOwnerName>/);

        const shares = sharesMatch ? Number(sharesMatch[1]) : null;
        const price = priceMatch ? Number(priceMatch[1]) : null;

        if (!shares || !price) continue;

        results.push({
          id: accession,
          companyName: nameMatch ? nameMatch[1] : "Unknown",
          ticker: sub.tickers?.[0] || "UNKNOWN",
          insiderName: insiderMatch ? insiderMatch[1] : "Unknown",
          insiderTitle: "Insider",
          shares,
          pricePerShare: price,
          totalValue: shares * price,
          transactionDate: recent.filingDate[i],
        });
      }
    }

    return res.status(200).json({
      lookbackDays: LOOKBACK_DAYS,
      filingsChecked,
      count: results.length,
      results,
    });
  } catch (err) {
    return res.status(500).json({
      error: err.message,
      stack: err.stack,
    });
  }
}
