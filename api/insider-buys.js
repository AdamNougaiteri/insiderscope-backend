import { XMLParser } from "fast-xml-parser";

const parser = new XMLParser({ ignoreAttributes: false });

const SEC_HEADERS = {
  "User-Agent": "InsiderScope demo contact@example.com",
  Accept: "application/xml",
};

export default async function handler(req, res) {
  try {
    // 1. Get latest Form 4 filings
    const feedRes = await fetch(
      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&count=40&output=atom",
      {
