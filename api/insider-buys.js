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

    // 2. Extract accession numbers from <id> tags
    const accessionMatches = [
      ...feedText.matchAll(/accession-number=([0-9-]+)/g),
    ];

    const accessionNumbers = accessionMatches.map(m => m[1]);

    const results = [];
    const debug = [];

    for (const accession of accessionNumbers) {
      try {
        const accessionNoDash = accession.replace(/-/g, "");

        // CIK is the first 10 digits of the accession folder path
        const cikMatch = feedText.match(
          new RegExp(`${accession}.*?CIK=(\\d+)`, "s")
        );
        if (!cikMatch) continue;

        const cik = cikMatch[1].padStart(10, "0");

        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNoDash}/xslF345X03/primary_doc.xml`;

        const xmlRes = await fetch(xmlUrl, { headers });
        if (!xmlRes.ok) continue;

        const xml = await xmlRes.text();

        const transactionCode =
          xml.match(/<transactionCode>(.*?)<\/transactionCode>/)?.[1] || null;
        const shares =
          Number(
            xml.match(/<transactionShares><value>(.*?)<\/value>/)?.[1] || 0
          );
        const price =
          Number(
            xml.match(/<transactionPricePerShare><value>(.*?)<\/value>/)?.[1] || 0
          );

        const company =
          xml.match(/<issuerName>(.*?)<\/issuerName>/)?.[1] || "Unknown";
        const insider =
          xml.match(/<rptOwnerName>(.*?)<\/rptOwnerName>/)?.[1] || "Unknown";

        // Always push debug so we can see what’s happening
        debug.push({
          accession,
          company,
          insider,
          transactionCode,
          shares,
          price,
        });

        // Strict buy filter (can loosen later)
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
        debug.push({ accession, error: err.message });
      }
    }

    return res.status(200).json({
      accessionCount: accessionNumbers.length,
      count: results.length,
      results,
      debug,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
