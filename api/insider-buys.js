// /api/insider-buys.js

import { XMLParser } from "fast-xml-parser";

const parser = new XMLParser({ ignoreAttributes: false });

const SEC_HEADERS = {
  "User-Agent": "InsiderScope demo contact@example.com",
  Accept: "application/xml",
};

export default async function handler(req, res) {
  try {
    const feedUrl =
      "https://www.sec.gov/Archives/edgar/usgaap.rss.xml";

    const feedResponse = await fetch(feedUrl, {
      headers: SEC_HEADERS,
    });

    if (!feedResponse.ok) {
      throw new Error("Failed to fetch SEC feed");
    }

    const xml = await feedResponse.text();
    const parsed = parser.parse(xml);

    const items = parsed?.rss?.channel?.item || [];

    const results = items
      .filter((item) =>
        item?.title?.toLowerCase().includes("form 4")
      )
      .slice(0, 25)
      .map((item, index) => ({
        id: `sec-${index}`,
        companyName: item?.title || "Unknown Company",
        ticker: "UNKNOWN",
        insiderName: "Insider",
        insiderTitle: "Insider",
        shares: 0,
        pricePerShare: 0,
        totalValue: 0,
        transactionDate: item?.pubDate || new Date().toISOString(),
      }));

    res.status(200).json(results);
  } catch (error) {
    console.error("SEC fetch error:", error);
    res.status(200).json([]); // NEVER crash frontend
  }
}
