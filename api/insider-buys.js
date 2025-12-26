// api/insider-buys.js

export default async function handler(req, res) {
  return res.status(200).json([
    {
      signalScore: 92,
      purchasedTicker: "NKE",
      purchasedCompany: "Nike Inc.",
      employerTicker: "AAPL",
      employerCompany: "Apple Inc.",
      insider: "Timothy D. Cook",
      role: "CEO",
      shares: 50000,
      price: 103.25,
      value: 5162500,
      filingDate: "2025-12-22",
      purchaseType: "external"
    },
    {
      signalScore: 88,
      purchasedTicker: "MSFT",
      purchasedCompany: "Microsoft Corp.",
      employerTicker: "MSFT",
      employerCompany: "Microsoft Corp.",
      insider: "Satya Nadella",
      role: "CEO",
      shares: 25000,
      price: 412.15,
      value: 10303750,
      filingDate: "2025-12-21",
      purchaseType: "own-company"
    }
  ]);
}
