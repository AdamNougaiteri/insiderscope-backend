export default async function handler(req, res) {
  try {
    const headers = {
      "User-Agent": "InsiderScope (contact: youremail@example.com)",
      Accept: "application/xml",
    };

    // 1. Fetch Form 4 Atom feed
    const feedRes = await fetch(
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=40&output=atom",
      { headers }
    );

    const feedText = await feedRes.text();

    // 2. Extract accession numbers DIRECTLY (this always works)
    const accessionMatches = [
      ...feedText.matchAll(/<accession-number>(.*?)<\/accession-number>/g),
    ];

    const accessions = accessionMatches.map(m => m[1]);

    const results = [];
    const debug = [];

    // 3. Iterate accessions
    for (const accession of accessions) {
      const accessionNoDash = accession.replace(/-/g, "");

      // CIK is not in accession, extract from feed context
      const cikMatch = feedText.match(
        new RegExp(
          `<accession-number>${accession}<\\/accession-number>[\\s\\S]*?<cik>(\\d+)<\\/cik>`
        )
      );

      if (!cikMatch) continue;

      const cik = cikMatch[1].padStart(10, "0");

      try {
        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNoDash}/xslF345X03/primary_doc.xml`;
        const xmlRes = await fetch(xmlUrl, { headers });

        if (!xmlRes.ok) {
          debug.push({ accession, cik, error: "XML fetch failed" });
          continue;
        }

        const xml = await xmlRes.text();

        const transactionCode =
          xml.match(/<transactionCode>(.*?)<\/transactionCode>/)?.[1] || null;

        const shares =
          Number(
            xml.match(/<transactionShares>[\s\S]*?<value>(.*?)<\/value>/)?.[1] || 0
          );

        const price =
          Number(
            xml.match(/<transactionPricePerShare>[\s\S]*?<value>(.*?)<\/value>/)?.[1] || 0
          );

        const company =
          xml.match(/<issuerName>(.*?)<\/issuerName>/)?.[1] || "Unknown";

        const insider =
          xml.match(/<rptOwnerName>(.*?)<\/rptOwnerName>/)?.[1] || "Unknown";

        debug.push({
          accession,
          cik,
          transactionCode,
          shares,
          price,
          company,
          insider,
        });

        // Only true open-market purchases
        if (transactionCode === "P" && shares > 0 && price > 0) {
          results.push({
            id: accession,
            companyName: company,
            ticker: "UNKNOWN",
            insiderName: insider,
            insiderTitle: "Insider",
            shares,
            pricePerShare: price,
            totalValue: shares * price,
            transactionDate: null,
          });
        }
      } catch (err) {
        debug.push({ accession, cik, error: err.message });
      }
    }

    return res.status(200).json({
      accessionCount: accessions.length,
      inspected: debug.length,
      count: results.length,
      results,
      debug,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
