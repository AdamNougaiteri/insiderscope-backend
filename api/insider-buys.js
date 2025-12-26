// api/insider-buys.js

export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 365;
    const today = new Date();
    const startDate = new Date();
    startDate.setDate(today.getDate() - LOOKBACK_DAYS);

    const headers = {
      "User-Agent": "InsiderScope/1.0 (contact@example.com)",
      "Accept-Encoding": "gzip, deflate",
      "Host": "data.sec.gov",
    };

    // Get recent Form 4 filings from SEC RSS
    const rssUrl =
      "https://www.sec.gov/Archives/edgar/usgaap.rss.xml";

    const rssResponse = await fetch(rssUrl, { headers });
    if (!rssResponse.ok) {
      throw new Error("Failed to fetch SEC RSS feed");
    }

    const rssText = await rssResponse.text();

    // Very simple Form 4 extraction (intentionally loose)
    const form4Links = Array.from(
      rssText.matchAll(/https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/.*?\.txt/g)
    )
      .map((m) => m[0])
      .slice(0, 50); // limit for safety

    const results = [];

    for (const filingUrl of form4Links) {
      try {
        const filingRes = await fetch(filingUrl, { headers });
        if (!filingRes.ok) continue;

        const filingText = await filingRes.text();

        if (!filingText.includes("FORM 4")) continue;

        results.push({
          filingUrl,
          detected: true,
        });
      } catch {
        continue;
      }
    }

    return res.status(200).json(results);
  } catch (error) {
    console.error("Backend error:", error);
    return res.status(200).json([]); // NEVER crash frontend
  }
}
