const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

const CSV_PATH = path.resolve(process.cwd(), "loto7-history.csv");
const TMP_CSV_PATH = path.resolve(process.cwd(), "loto7-history.csv.tmp");

const SOURCE_URLS = [
  "https://www.mizuhobank.co.jp/takarakuji/check/loto/loto7/index.html",
  "https://www.mizuhobank.co.jp/takarakuji/check/loto/loto7/backnumber/index.html"
];

const CSV_HEADERS = [
  "drawNumber",
  "drawDate",
  "n1",
  "n2",
  "n3",
  "n4",
  "n5",
  "n6",
  "n7",
  "b1",
  "b2"
];

function log(level, message) {
  const now = new Date().toISOString();
  console.log(`[${now}] ${level}: ${message}`);
}

async function fetchHtml(url) {
  log("INFO", `fetching ${url}`);
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (compatible; LunaSystem/1.0; +https://github.com/)"
    }
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} while fetching ${url}`);
  }

  return await res.text();
}

function normalizeWhitespace(text) {
  return text
    .replace(/\r/g, " ")
    .replace(/\n/g, " ")
    .replace(/\t/g, " ")
    .replace(/\u3000/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function toIsoDateFromJa(text) {
  const m = text.match(/(\d{4})\D+(\d{1,2})\D+(\d{1,2})/);
  if (!m) return null;
  const year = m[1];
  const month = pad2(m[2]);
  const day = pad2(m[3]);
  return `${year}-${month}-${day}`;
}

function escapeCsv(value) {
  const s = String(value ?? "");
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function parseCsvLine(line) {
  const result = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"' && inQuotes && next === '"') {
      current += '"';
      i++;
      continue;
    }

    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (ch === "," && !inQuotes) {
      result.push(current);
      current = "";
      continue;
    }

    current += ch;
  }

  result.push(current);
  return result;
}

function loadExistingCsv() {
  if (!fs.existsSync(CSV_PATH)) {
    throw new Error(`CSV file not found: ${CSV_PATH}`);
  }

  const raw = fs.readFileSync(CSV_PATH, "utf8").replace(/^\uFEFF/, "");
  const lines = raw.split(/\r?\n/).filter(line => line.trim() !== "");

  if (lines.length === 0) {
    return [];
  }

  const header = parseCsvLine(lines[0]);
  const expected = CSV_HEADERS.join(",");
  const actual = header.join(",");

  if (actual !== expected) {
    throw new Error(
      `CSV header mismatch.\nExpected: ${expected}\nActual:   ${actual}`
    );
  }

  const records = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    if (cols.length < CSV_HEADERS.length) continue;

    const rec = {};
    CSV_HEADERS.forEach((h, idx) => {
      rec[h] = (cols[idx] || "").trim();
    });
    records.push(rec);
  }

  return records;
}

function writeCsv(records) {
  const lines = [
    CSV_HEADERS.join(","),
    ...records.map(rec =>
      CSV_HEADERS.map(h => escapeCsv(rec[h] ?? "")).join(",")
    )
  ];

  fs.writeFileSync(TMP_CSV_PATH, lines.join("\n") + "\n", "utf8");
  fs.renameSync(TMP_CSV_PATH, CSV_PATH);
}

function validateRecord(record) {
  const drawNumber = Number(record.drawNumber);
  if (!Number.isInteger(drawNumber) || drawNumber <= 0) {
    throw new Error(`Invalid drawNumber: ${record.drawNumber}`);
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(record.drawDate)) {
    throw new Error(`Invalid drawDate: ${record.drawDate}`);
  }

  const nums = [
    record.n1, record.n2, record.n3, record.n4, record.n5, record.n6, record.n7,
    record.b1, record.b2
  ];

  for (const n of nums) {
    if (!/^\d{2}$/.test(n)) {
      throw new Error(`Invalid number format: ${n}`);
    }
    const num = Number(n);
    if (num < 1 || num > 37) {
      throw new Error(`Number out of range: ${n}`);
    }
  }

  const mainNums = [record.n1, record.n2, record.n3, record.n4, record.n5, record.n6, record.n7];
  const mainSet = new Set(mainNums);
  if (mainSet.size !== 7) {
    throw new Error(`Duplicate main numbers detected: ${mainNums.join(",")}`);
  }
}

function normalizeRecord(record) {
  const normalized = {
    drawNumber: String(Number(record.drawNumber)),
    drawDate: record.drawDate,
    n1: pad2(record.n1),
    n2: pad2(record.n2),
    n3: pad2(record.n3),
    n4: pad2(record.n4),
    n5: pad2(record.n5),
    n6: pad2(record.n6),
    n7: pad2(record.n7),
    b1: pad2(record.b1),
    b2: pad2(record.b2)
  };

  validateRecord(normalized);
  return normalized;
}

function extractCandidatesFromText(text) {
  const candidates = [];
  const roundRegex = /第\s*(\d+)\s*回/g;
  let match;

  while ((match = roundRegex.exec(text)) !== null) {
    const drawNumber = match[1];
    const startIdx = match.index;
    const slice = text.slice(startIdx, startIdx + 700);

    const dateMatch = slice.match(/(\d{4}\D{0,3}\d{1,2}\D{0,3}\d{1,2}\D{0,3})/);
    const drawDate = dateMatch ? toIsoDateFromJa(dateMatch[1]) : null;

    const numMatches = [...slice.matchAll(/\b(\d{1,2})\b/g)].map(m => m[1]);

    const filteredNums = numMatches
      .map(v => Number(v))
      .filter(v => v >= 1 && v <= 37)
      .map(v => pad2(v));

    if (!drawDate) continue;

    const uniqueWindowNums = [];
    for (const n of filteredNums) {
      if (uniqueWindowNums.length >= 9) break;
      uniqueWindowNums.push(n);
    }

    if (uniqueWindowNums.length < 9) continue;

    candidates.push({
      drawNumber: String(Number(drawNumber)),
      drawDate,
      n1: uniqueWindowNums[0],
      n2: uniqueWindowNums[1],
      n3: uniqueWindowNums[2],
      n4: uniqueWindowNums[3],
      n5: uniqueWindowNums[4],
      n6: uniqueWindowNums[5],
      n7: uniqueWindowNums[6],
      b1: uniqueWindowNums[7],
      b2: uniqueWindowNums[8]
    });
  }

  return candidates;
}

function pickBestCandidate(candidates, existingRecords) {
  if (!candidates.length) {
    throw new Error("No candidate record found from fetched source");
  }

  const existingMax = existingRecords.reduce((max, rec) => {
    const n = Number(rec.drawNumber || 0);
    return n > max ? n : max;
  }, 0);

  const normalizedCandidates = [];
  for (const c of candidates) {
    try {
      normalizedCandidates.push(normalizeRecord(c));
    } catch (err) {
      log("WARN", `candidate rejected: ${err.message}`);
    }
  }

  if (!normalizedCandidates.length) {
    throw new Error("All parsed candidates were invalid");
  }

  normalizedCandidates.sort((a, b) => Number(b.drawNumber) - Number(a.drawNumber));

  const newer = normalizedCandidates.find(c => Number(c.drawNumber) > existingMax);
  if (newer) return newer;

  return normalizedCandidates[0];
}

function parseLoto7RecordFromHtml(html, existingRecords) {
  const $ = cheerio.load(html);
  const bodyText = normalizeWhitespace($("body").text());

  const candidates = extractCandidatesFromText(bodyText);
  return pickBestCandidate(candidates, existingRecords);
}

function recordsEqual(a, b) {
  return CSV_HEADERS.every(h => String(a[h] ?? "") === String(b[h] ?? ""));
}

function mergeRecord(existingRecords, incomingRecord) {
  const byDrawNumber = new Map(
    existingRecords.map(rec => [String(rec.drawNumber), rec])
  );

  const existing = byDrawNumber.get(String(incomingRecord.drawNumber));
  if (existing) {
    if (!recordsEqual(existing, incomingRecord)) {
      throw new Error(
        `drawNumber ${incomingRecord.drawNumber} already exists but data differs`
      );
    }
    return { updated: false, records: existingRecords };
  }

  const merged = [...existingRecords, incomingRecord].sort(
    (a, b) => Number(a.drawNumber) - Number(b.drawNumber)
  );

  return { updated: true, records: merged };
}

async function fetchLatestRecord(existingRecords) {
  let lastError = null;

  for (const url of SOURCE_URLS) {
    try {
      const html = await fetchHtml(url);
      const record = parseLoto7RecordFromHtml(html, existingRecords);
      log(
        "INFO",
        `parsed candidate drawNumber=${record.drawNumber}, drawDate=${record.drawDate}`
      );
      return record;
    } catch (err) {
      lastError = err;
      log("WARN", `failed on ${url}: ${err.message}`);
    }
  }

  throw lastError || new Error("Failed to fetch from all source URLs");
}

async function main() {
  try {
    log("INFO", "start update");

    const existingRecords = loadExistingCsv();
    log("INFO", `loaded existing CSV rows: ${existingRecords.length}`);

    const incomingRecord = await fetchLatestRecord(existingRecords);
    const { updated, records } = mergeRecord(existingRecords, incomingRecord);

    if (!updated) {
      log("INFO", `no update: drawNumber ${incomingRecord.drawNumber} already exists`);
      return;
    }

    writeCsv(records);
    log("INFO", `success: appended drawNumber ${incomingRecord.drawNumber}`);
  } catch (err) {
    log("ERROR", err.stack || err.message);
    process.exitCode = 1;
  }
}

main();
