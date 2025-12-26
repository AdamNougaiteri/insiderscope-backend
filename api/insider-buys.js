// api/insider-buys.js

export default async function handler(req, res) {
  try {
    // Required by SEC
    const headers = {
      "User-Agent": "InsiderScope research (contact: research@insiderscope.ai)",
      Accept: "application/json",
    };

    // SEC Recent Filings feed (this one actually works)
    const response = await fetch(
      "https://data.sec.gov/submissions/filings.json",
      { headers }
    );

    if (!response.ok) {
      throw new Error("SEC fetch failed");
    }

    const data = await response.json();

    const filings = data?.filings?.recent;
    if (!filings) {
      return res.status(200).json([]);
    }

    const results = [];

    // Only scan first 100 filings to avoid timeouts
    for (let i = 0; i < Math.min(100, filings.form.length); i++) {
      if (filings.form[i] !== "4") continue;

      results.push({
        cik: filings.cik[i],
        accessionNumber: filings.accessionNumber[i],
        filingDate: filings.filingDate[i],
        issuer: filings.primaryDocDescription[i] || "Unknown",
      });
    }

    return res.status(200).json(results);
  } catch (error) {
    console.error("Insider buys error:", error);
    // IMPORTANT: Always return JSON, never crash
    return res.status(200).json([]);
  }
}
