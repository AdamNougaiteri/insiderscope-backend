export default async function handler(req, res) {
  try {
    const headers = {
      "User-Agent": "InsiderScope (contact: youremail@example.com)",
      Accept: "application/xml",
    };

    const feedRes = await fetch(
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=20&output=atom",
      { headers }
    );

    const feedText = await feedRes.text();
    const accessionMatches = [...feedText.matchAll(/accession-number=(\d+-\d+-\d+)/g)];
    const accessionNumbers = accessionMatches.map(m => m[1]);

    const results = [];
    const debug = [];

    for (const accession of accessionNumbers) {
      try {
        const cikMatch = feedText.match(
          new RegExp(`${accession}.*?CIK=(\\d+)`, "s")
        );
        if (!cikMatch) continue;

        const cik = cikMatch[1].padStart(10, "0");
        const accessionNoDash = accession.replace(/-/g, "");
        const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNoDash}/xslF345X03/primary_doc.xml`;

        const xmlRes = await fetch(xmlUrl, { headers });
        if (!xmlRes.ok) continue;

        const xml = await xmlRes.text();

        const transactionCode =
          xml.match(/<transactionCode>(.*?)<\/transactionCode>/)?.[1] || null;
        const shares =
          Number(xml.match(/<transactionShares><value>(.*?)<\/value>/)?.[1] || 0);
        const price =
          Number(xml.match(/<transactionPricePerShare><value>(.*?)<\/value>/)?.[1] || 0);

        const company =
          xml.match(/<issuerName>(.*?)<\/issuerName>/)?.[1] || "Unknown";
        const insider =
          xml.match(/<rptOwnerName>(.*?)<\/rptOwnerName>/)?.[1] || "Unknown";

        debug.push({
          accession,
          company,
          insider,
          transactionCode,
          shares,
          price,
        });

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
      } catch (e) {
        debug.push({ accession, error: e.message });
      }
    }

    return res.status(200).json({
      count: results.length,
      results,
      debug,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
