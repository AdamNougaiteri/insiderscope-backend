export default async function handler(req, res) {
  try {
    const headers = {
      "User-Agent": "InsiderScope (contact: youremail@example.com)",
      Accept: "application/xml",
    };

    // 1. Pull latest Form 4 filings from SEC Atom feed
    const feedRes = await fetch(
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=40&output=atom",
      { headers }
    );

    const feedText = await feedRes.text();

    // 2. Extract accession numbers
    const accessionMatches = [...feedText.matchAll(/accession-number=(\d+-\d+-\d+)/g)];
    const accessionNumbers = accessionMatches.map(m => m[1]);

    const results = [];

    // 3. Fetch and parse each Form 4 XML
    for (const accession of accessionNumbers) {
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

      // 4. Extract transaction data
      const transactionCode = xml.match(/<transactionCode>(.*?)<\/transactionCode>/)?.[1];
      const shares = Number(xml.match(/<transactionShares><value>(.*?)<\/value>/)?.[1] || 0);
      const price = Number(xml.match(/<transactionPricePerShare><value>(.*?)<\/value>/)?.[1] || 0);

      // Keep ONLY open-market buys
      if (transactionCode !== "P") continue;
      if (shares <= 0 || price <= 0) continue;

      const company =
        xml.match(/<issuerName>(.*?)<\/issuerName>/)?.[1] || "Unknown Company";
      const insider =
        xml.match(/<rptOwnerName>(.*?)<\/rptOwnerName>/)?.[1] || "Unknown Insider";
      const title =
        xml.match(/<officerTitle>(.*?)<\/officerTitle>/)?.[1] || "Insider";
      const date =
        xml.match(/<transactionDate><value>(.*?)<\/value>/)?.[1] || null;

      results.push({
        id: accession,
        companyName: company,
        ticker: "UNKNOWN",
        insiderName: insider,
        insiderTitle: title,
        shares,
        pricePerShare: price,
        totalValue: shares * price,
        transactionDate: date,
      });
    }

    return res.status(200).json(results);
  } catch (err) {
    console.error("INSIDER BUY ERROR:", err);
    return res.status(500).json({ error: "Internal error" });
  }
}
