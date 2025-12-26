// api/insider-buys.js

export default function handler(req, res) {
  const data = [
    {
      id: "cook-nke-2025-12-22",

      insiderName: "Timothy D. Cook",
      insiderTitle: "CEO",

      employerTicker: "AAPL",
      employerCompany: "Apple Inc.",

      purchasedTicker: "NKE",
      purchasedCompany: "Nike Inc.",

      shares: 50000,
      pricePerShare: 103.25,
      totalValue: 5162500,

      transactionDate: "2025-12-22",

      signalScore: 92,
      purchaseType: "external",
    },
    {
      id: "nadella-msft-2025-12-21",

      insiderName: "Satya Nadella",
      insiderTitle: "CEO",

      employerTicker: "MSFT",
      employerCompany: "Microsoft Corp.",

      purchasedTicker: "MSFT",
      purchasedCompany: "Microsoft Corp.",

      shares: 25000,
      pricePerShare: 412.15,
      totalValue: 10303750,

      transactionDate: "2025-12-21",

      signalScore: 88,
      purchaseType: "own-company",
    },
  ];

  res.status(200).json(data);
}
