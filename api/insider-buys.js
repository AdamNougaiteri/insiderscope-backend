export default async function handler(req, res) {
  try {
    const CIK = "0000320193"; // Apple (example)
    const SUBMISSIONS_URL = `https://data.sec.gov/submissions/CIK${CIK}.json`;

    const response = await fetch(SUBMISSIONS_URL, {
      headers: {
        "User-Agent": "InsiderScope demo contact@example.com",
        Accept: "application/json",
      },
    });

    const data = await response.json();
    const recent = data.filings?.recent;

    if (!recent) {
      return res.status(200).json([]);
    }

    // Find Form 4 filings
    const form4Indexes = recent.form
      .map((form, i) => (form === "4" ? i : null))
      .filter((i) => i !== null);

    const form4s = form4Indexes.map((i) => ({
      accessionNumber: recent.accessionNumber[i],
      filingDate: recent.filingDate[i],
      reportDate: recent.reportDate[i],
    }));

    return res.status(200).json({
      count: form4s.length,
      form4s: form4s.slice(0, 5), // limit for now
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
