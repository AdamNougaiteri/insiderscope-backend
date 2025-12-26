import { XMLParser } from "fast-xml-parser";

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
});

const HEADERS = {
  "User-Agent": "InsiderScope contact@example.com",
  Accept: "application/xml,text/xml",
};

export default async function handler(req, res) {
  try {
    const feedUrl =
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=40&output=atom";

    const feedRes = await fetch(feedUrl, { headers: HEADERS });
    const feedXml = await feedRes.text();
    const feed = parser.parse(feedXml);

    const entries = Array.isArray(feed.feed.entry)
      ? feed.feed.entry
      : [feed.feed.entry];

    const results = [];

    for (const entry of entries.slice(0, 20)) {
      // ✅ CORRECT: find the XML link directly
      const xmlLink = Array.isArray(entry.link)
        ? entry.link.find((l) => l.type === "application/xml")?.href
        : entry.link?.href;

      if (!xmlLink)
