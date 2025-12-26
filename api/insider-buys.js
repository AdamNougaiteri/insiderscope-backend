export default async function handler(req, res) {
  try {
    const headers = {
      "User-Agent": "InsiderScope research (contact@insiderscope.app)",
      "Accept-Encoding": "gzip, deflate",
    };

    // 1. Get latest insider submissions
    const submissionsRes = await fetch(
      "https://data.sec.gov/submissions/CIK0000320193.json",
      { headers }
    );

    if (!submissionsRes.ok) {
      return res.status(500).json({ error: "Failed to fetch submissions" });
    }

    const submissions = await submissionsRes.json();
    const recentForms = submissions.filings.recent;

    const form4s = [];
    for (let i = 0; i < recentForms.form.length; i++) {
      if (recentForms.form[i] === "4") {
        form4s.push({
          accession: recentForms.accessionNumber[i],
          filingDate: recentForms.filingDate[i],
        });
      }
    }

    // Limit for safety
    const limited = form4s.slice(0, 5);
    const results = [];

    // 2. Fetch each Form 4 XML and extract issuer + owner
    for (const f of limited) {
      const acc = f.accession.replace(/-/g, "");
      const cik = "0000320193"; // Apple CIK for testing

      const xmlUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${acc}/${f.accession}.xml`;
      const xmlRes = await fetch(xmlUrl, { headers });

      if (!xmlRes.ok) continue;
      const xml = await xmlRes.text();

      // VERY loose parsing — just prove data exists
      const issuerMatch = xml.match(/<issuerName>(.*?)<\/issuerName>/);
      const ownerMatch = xml.match(/<rptOwnerName>(.*?)<\/rptOwnerName>/);

      results.push({
        id: f.accession,
        companyName: issuerMatch ? issuerMatch[1] : "Unknown Issuer",
        insiderName: ownerMatch ? ownerMatch[1] : "Unknown Insider",
        filingDate: f.filingDate,
        source: "SEC Form 4 XML",
      });
    }

    return res.status(200).json(results);

  } catch (err) {
    console.error("INSIDER API ERROR:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
}
