export default async function handler(req, res) {
  try {
    const SEC_URL =
      "https://data.sec.gov/submissions/CIK0000320193.json"; // example CIK (Apple)

    const response = await fetch(SEC_URL, {
      headers: {
        "User-Agent": "InsiderScope demo contact@example.com",
        Accept: "application/json",
      },
    });

    const json = await response.json();

    // TEMP: return raw SEC payload for inspection
    return res.status(200).json({
      debug: true,
      source: "SEC submissions API",
      keys: Object.keys(json),
      recent: json?.filings?.recent ?? null,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
