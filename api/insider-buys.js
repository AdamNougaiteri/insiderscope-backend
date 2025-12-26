export default async function handler(req, res) {
  try {
    // ⚠️ REQUIRED BY SEC — MUST LOOK LIKE A REAL HUMAN
    const headers = {
      "User-Agent": "InsiderScope Research (email: hello@insiderscope.dev)",
      "Accept": "application/json",
    };

    const CIK = "0000320193"; // Apple
    const LOOKBACK_DAYS = 7;

    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - LOOKBACK_DAYS);

    // 1️⃣ FETCH SUBMISSIONS
    const submissionsUrl = `https://data.sec.gov/submissions/CIK${CIK}.json`;
    const submissionsRes = await fetch(submissionsUrl, { headers });

    const rawText = await submissionsRes.text();

    // 🔍 FORCE DEBUG — RETURN RAW SEC RESPONSE
    if (!rawText.startsWith("{")) {
      return res.status(200).json({
        error: "SEC did not return JSON",
        status: submissionsRes.status,
        preview: rawText.slice(0, 500),
      });
    }

    const submissions = JSON.parse(rawText);

    const recent = submissions?.filings?.recent;
    if (!recent) {
      return res.status(200).json({
        error: "No recent filings object",
        keys: Object.keys(submissions),
      });
    }

    // 2️⃣ FILTER FORM 4s
    const accessions = [];

    for (let i = 0; i < recent.form.length; i++) {
      if (recent.form[i] !== "4") continue;

      const filingDate = new Date(recent.filingDate[i]);
      if (filingDate < cutoff) continue;

      accessions.push({
        accession: recent.accessionNumber[i],
        filingDate: recent.filingDate[i],
      });
    }

    // 3️⃣ RETURN WHAT WE FOUND (NO XML YET)
    return res.status(200).json({
      cik: CIK,
      lookbackDays: LOOKBACK_DAYS,
      accessionCount: accessions.length,
      sample: accessions.slice(0, 3),
    });

  } catch (err) {
    return res.status(500).json({
      error: "Unhandled error",
      message: err.message,
      stack: err.stack,
    });
  }
}
