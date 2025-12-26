// api/insider-buys.js

export default function handler(req, res) {
  return res.status(200).json([
    {
      cik: "0000320193",
      ticker: "AAPL",
      company: "Apple Inc.",
      insider: "Timothy D. Cook",
      role: "CEO",
      shares: 50000,
      price: 189.42,
      value: 9471000,
      filingDate: "2025-12-22",
      signalScore: 92
    },
    {
      cik: "0000789019",
      ticker: "MSFT",
      company: "Microsoft Corp.",
      insider: "Satya Nadella",
      role: "CEO",
      shares: 25000,
      price: 412.15,
      value: 10303750,
      filingDate: "2025-12-21",
      signalScore: 88
    },
    {
      cik: "0001318605",
      ticker: "TSLA",
      company: "Tesla Inc.",
      insider: "Kimbal Musk",
      role: "Director",
      shares: 100000,
      price: 248.31,
      value: 24831000,
      filingDate: "2025-12-20",
      signalScore: 81
    }
  ]);
}
