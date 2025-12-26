export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 7; // change to 365 later
    const today = new Date();
    const cutoff = new Date(today);
    cutoff.setDate(today.getDate() - LOOKBACK_DAYS);

    // Step 1: Get recent Form 4 submissions index
    const submissionsRes = await fetch(
      "https://data.sec.gov/submissions/CIK0000320193.json",
      {
        headers: {
          "User-Agent": "InsiderScope dev (contact: you@example.com)",
          "Accept-Encoding": "gzip, deflate",
        },
      }
    );

    if (!submissionsRes.ok) {
      throw new Error("Failed to fetch SEC submissions index");
    }

    const submissions = await submissionsRes.json();

    // Step 2: Pull recent Form 4 filings only
    const recentForm4s = submissions.filings.recent.accessionNumber
      .map((acc, i) => ({
        accessionNumber: acc,
        filingDate: submissions.filings.recent.filingDate[i],
        form: submissions.filings.recent.form[i],
      }))
      .filter(f => f.form === "4")
      .filter(f => new Date(f.filingDate) >= cutoff)
      .slice(0, 25); // hard cap for safety

    // Step 3: Return raw filings (NO XML parsing yet)
    res.status(200).json({
      ok: true,
      lookbackDays: LOOKBACK_DAYS,
      filingsFound: recentForm4s.length,
      filings: recentForm4s,
      note: "Phase 1: returning Form 4 filings only (no transaction parsing yet)",
    });

  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      stack: err.stack,
    });
  }
}
