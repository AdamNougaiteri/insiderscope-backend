export default async function handler(req, res) {
  try {
    // TEMP: stable hardcoded response to unblock frontend
    // (We will replace this with real SEC parsing next)
    const data = [
      {
        id: "test-1",
        companyName: "Test Company",
        ticker: "TEST",
        insiderName: "Test Insider",
        insiderTitle: "Director",
        shares: 10000,
        pricePerShare: 100,
        totalValue: 1000000,
        transactionDate: new Date().toISOString().split("T")[0],
      },
    ];

    res.setHeader("Access-Control-Allow-Origin", "*");
    res.status(200).json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch insider buys" });
  }
}
