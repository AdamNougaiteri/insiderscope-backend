export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 7; // change to 365 later

    const today = new Date();
    const filings = [];

    for (let i = 0; i < LOOKBACK_DAYS; i++) {
      const d = new Date(today);
      d.setDate(today.getDate() - i);

      const year = d.getFullYear();
      const quarter = `QTR${Math.floor(d.getMonth() / 3) + 1}`;
      const yyyymmdd = d.toISOString().slice(0, 10).replace(/-/g, "");

      const indexUrl = `https://www.sec.gov/Archives/edgar/daily-index/${year}/${quarter}/form.${yyyymmdd}.idx`;

      const idxRes = await fetch(indexUrl, {
        headers: {
          "User-Agent": "InsiderScope dev (contact: you@example.com)",
        },
      });

      if (!idxRes.ok) continue;

      const text = await idxRes.text();
      const lines = text.split("\n");

      for (const line of lines) {
        if (line.includes("|4|")) {
          const parts = line.split("|");
          filings.push({
            cik: parts[0],
            companyName: parts[1],
            form: parts[2],
            filingDate: parts[3],
            path: `https://www.sec.gov/Archives/${parts[4]}`,
          });
        }
      }
    }

    res.status(200).json({
      ok: true,
      lookbackDays: LOOKBACK_DAYS,
      filingsFound: filings.length,
      filings: filings.slice(0, 50),
      note: "Global Form 4 filings (Phase 1, no transaction parsing yet)",
    });

  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
}
