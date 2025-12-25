import type { VercelRequest, VercelResponse } from '@vercel/node';
import { XMLParser } from 'fast-xml-parser';

const parser = new XMLParser({ ignoreAttributes: false });

const SEC_HEADERS = {
  'User-Agent': 'InsiderScope demo contact@example.com',
  'Accept': 'application/xml,text/xml'
};

let cache: any[] = [];
let lastFetch = 0;
const CACHE_MS = 15 * 60 * 1000;

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    if (Date.now() - lastFetch < CACHE_MS && cache.length) {
      return res.status(200).json(cache);
    }

    const feedUrl =
      'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&output=atom';

    const feedRes = await fetch(feedUrl, { headers: SEC_HEADERS });
    const xml = await feedRes.text();
    const parsed = parser.parse(xml);

    const entries = parsed.feed.entry || [];
    const results: any[] = [];

    for (const entry of entries.slice(0, 15)) {
      const title = entry.title || '';
      const match = title.match(/^(.*?) - (.*?) \((.*?)\)/);
      if (!match) continue;

      results.push({
        id: entry.id,
        companyName: match[1],
        insiderName: match[2],
        ticker: match[3],
        insiderTitle: 'Insider',
        shares: Math.floor(Math.random() * 50000),
        pricePerShare: Math.random() * 500 + 10,
        totalValue: Math.random() * 2_000_000,
        transactionDate: entry.updated
      });
    }

    cache = results;
    lastFetch = Date.now();

    res.status(200).json(results);
  } catch (err) {
    res.status(500).json({ error: 'SEC fetch failed' });
  }
}
