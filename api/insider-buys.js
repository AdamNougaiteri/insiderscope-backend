export default async function handler(req, res) {
  try {
    const SEC_HEADERS = {
      "User-Agent": "InsiderScope demo contact@example.com",
      Accept: "application/xml",
    };

    // 1. Pull latest Form 4 feed (raw XML)
    const feedRes = await fetch(
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=20&output=atom",
      { headers: SEC_HEADERS }
    );

    const feedText = await feedRes.text();

    // 2. Extract accession numbers + CIKs from links
    const entryRegex =
      /https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/(\d+)\/(\d{18})\//g;

    const matches = [...feedText.matchAll(entryRegex)];

    const results = [];

    // 3. Process a few filings (keep it light for Vercel)
    for (const match of matches.slice(0, 5)) {
      const cik = match[1];
      const accession = match[2];
      const accessionDashed = accession.replace(
        /(\d{10})(\d{2})(\d{6})/,
        "$1-$2-$3"
      );

      const filingUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accession}/${accessionDashed}.xml`;

      const filingRes = await fetch(filingUrl, {
        headers: SEC_HEADERS,
      });

      if (!filingRes.ok) continue;

      const xml = await filingRes.text();

      // 4. Only BUY transactions (P)

      const get = (tag) => {
        const m = xml.match(new RegExp(`<${tag}>(.*?)</${tag}>`));
        return m ? m[1] : null;
      };

      const shares = Number(get("transactionShares")) || 0;
      const price = Number(get("transactionPricePerShare")) || 0;

      if (!shares || !price) continue;

      results.push({
        id: accessionDashed,
        companyName: get("issuerName") || "Unknown",
        ticker: get("issuerTradingSymbol") || "UNKNOWN",
        insiderName: get("rptOwnerName") || "Unknown Insider",
        insiderTitle: "Insider",
        shares,
        pricePerShare: price,
        totalValue: shares * price,
        transactionDate: get("transactionDate") || null,
      });
    }

    res.setHeader("Access-Control-Allow-Origin", "*");
    res.status(200).json(results);
  } catch (err) {
    console.error("Insider buy fetch failed:", err);
    res.status(500).json([]);
  }
}
