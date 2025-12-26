// api/insider-buys.js

import { XMLParser } from "fast-xml-parser";

const SEC_BASE = "https://data.sec.gov";
const HEADERS = {
  "User-Agent": process.env.SEC_USER_AGENT,
  "Accept-Encoding": "gzip, deflate",
  Accept: "application/xml"
};

export default async function handler(req, res) {
  try {
    const atomUrl =
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=40&output=atom";

    const atomResp = await fetch(atomUrl, { headers: HEADERS });
    const atomText = await atomResp.text();

    const parser = new XMLParser({ ignoreAttributes: false });
    const atom = parser.parse(atomText);

    const entries = atom.feed.entry || [];
    const results = [];

    for (const entry of entries) {
      const filingUrl = entry.link?.["@_href"];
      if (!filingUrl) continue;

      const filingResp = await fetch(filingUrl, { headers: HEADERS });
      const filingText = await filingResp.text();

      const filing = parser.parse(filingText);
      const ownership = filing?.ownershipDocument;
      if (!ownership) continue;

      const reportingOwner =
        ownership.reportingOwner?.reportingOwnerId;

      const insiderName =
        reportingOwner?.rptOwnerName || "Unknown";

      const insiderAffiliationCompany =
        ownership.issuer?.issuerName || "Unknown";

      const transactions =
        ownership.nonDerivativeTable?.nonDerivativeTransaction;

      if (!transactions) continue;

      const txList = Array.isArray(transactions)
        ? transactions
        : [transactions];

      for (const tx of txList) {
        const code =
          tx.transactionCoding?.transactionCode;

        if (code !== "A") continue; // only acquisitions

        const purchasedCompany =
          ownership.issuer?.issuerName || "Unknown";

        const purchasedTicker =
          ownership.issuer?.issuerTradingSymbol || "N/A";

        const shares =
          Number(tx.transactionAmounts?.transactionShares?.value || 0);

        const price =
          Number(tx.transactionAmounts?.transactionPricePerShare?.value || 0);

        const filingDate =
          ownership.periodOfReport || null;

        results.push({
          insider: insiderName,
          insiderAffiliationCompany,
          purchasedCompany,
          purchasedTicker,
          shares,
          price,
          value: Math.round(shares * price),
          filingDate,
          isAffiliatedPurchase:
            insiderAffiliationCompany === purchasedCompany
        });
      }
    }

    return res.status(200).json(results);
  } catch (err) {
    return res.status(500).json({
      error: err.message || "Unknown error"
    });
  }
}
