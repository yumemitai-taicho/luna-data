const fs = require("fs");
const path = require("path");

const CSV_PATH = path.resolve(process.cwd(), "loto7-history.csv");
const TMP_CSV_PATH = path.resolve(process.cwd(), "loto7-history.csv.tmp");

const SOURCE_URL = "https://takarakuji.rakuten.co.jp/backnumber/loto7/";

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
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "accept-language": "ja,en-US;q=0.9,en;q=0.8",
      "cache-control": "no-cache",
      "pragma": "no-cache"
    }
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} while fetching ${url}`);
  }

  return await res.text();
}

function normalizeWhitespace(text) {
  return String(text || "")
    .replace(/\r/g, " ")
    .replace(/\n/g, " ")
    .replace(/\t/g, " ")
    .replace(/\u3000/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripTags(html) {
  return normalizeWhitespace(
    String(html || "")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/&#160;/gi, " ")
      .replace(/&amp;/gi, "&")
  );
}

function pad2(value) {
  return String(Number(value)).padStart(2, "0");
}

function toIsoDateFromSlash(text) {
  const m = String(text).match(/(\d{4})\/(\d{1,2})\/(\d{1,2})/);
  if (!m) return null;
  return `${m[1]}-${pad2(m[2])}-${pad2(m[3])}`;
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

function sortRecordsNewestFirst(records) {
  return [...records].sort((a, b) => Number(b.drawNumber) - Number(a.drawNumber));
}

function loadExistingCsv() {
  if (!fs.existsSync(CSV_PATH)) {
    throw new Error(`CSV file not found: ${CSV_PATH}`);
  }

  const raw = fs.readFileSync(CSV_PATH, "utf8").replace(/^\uFEFF/, "");
  const lines = raw.split(/\r?\n/).filter(line => line.trim() !== "");

  if (lines.length === 0) return [];

  const header = parseCsvLine(lines[0]).map(v => v.trim());
  const expected = CSV_HEADERS.join(",");
  const actual = header.join(",");

  if (actual !== expected) {
    throw new Error(`CSV header mismatch.\nExpected: ${expected}\nActual:   ${actual}`);
  }

  const records = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    if (cols.length < CSV_HEADERS.length) continue;

    const rec = {};
    CSV_HEADERS.forEach((h, idx) => {
      rec[h] = String(cols[idx] || "").trim();
    });
    records.push(rec);
  }

  return sortRecordsNewestFirst(records);
}

function writeCsv(records) {
  const sorted = sortRecordsNewestFirst(records);

  const lines = [
    CSV_HEADERS.join(","),
    ...sorted.map(rec => CSV_HEADERS.map(h => escapeCsv(rec[h] ?? "")).join(","))
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
  if (new Set(mainNums).size !== 7) {
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

function recordsEqual(a, b) {
  return CSV_HEADERS.every(h => String(a[h] ?? "") === String(b[h] ?? ""));
}

function mergeRecord(existingRecords, incomingRecord) {
  const byDrawNumber = new Map(existingRecords.map(rec => [String(rec.drawNumber), rec]));
  const existing = byDrawNumber.get(String(incomingRecord.drawNumber));

  if (existing) {
    if (!recordsEqual(existing, incomingRecord)) {
      throw new Error(`drawNumber ${incomingRecord.drawNumber} already exists but data differs`);
    }
    return { updated: false, records: sortRecordsNewestFirst(existingRecords) };
  }

  const merged = [...existingRecords, incomingRecord];
  return { updated: true, records: sortRecordsNewestFirst(merged) };
}

function parseRakutenPage(html, existingRecords) {
  const text = stripTags(html);
  log("INFO", `rakuten text snippet: ${text.slice(0, 1000)}`);

  const regex =
    /回号\s*第0*(\d+)回\s*抽せん日\s*(\d{4}\/\d{1,2}\/\d{1,2})\s*本数字\s*([0-9\s]+?)\s*ボーナス数字\s*\((\d{1,2})\)\s*\((\d{1,2})\)/g;

  const candidates = [];
  let match;

  while ((match = regex.exec(text)) !== null) {
    const drawNumber = String(Number(match[1]));
    const drawDate = toIsoDateFromSlash(match[2]);
    const mainNums = String(match[3])
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .map(v => pad2(v));

    const bonusNums = [pad2(match[4]), pad2(match[5])];

    if (!drawDate || mainNums.length !== 7) {
      continue;
    }

    try {
      candidates.push(
        normalizeRecord({
          drawNumber,
          drawDate,
          n1: mainNums[0],
          n2: mainNums[1],
          n3: mainNums[2],
          n4: mainNums[3],
          n5: mainNums[4],
          n6: mainNums[5],
          n7: mainNums[6],
          b1: bonusNums[0],
          b2: bonusNums[1]
        })
      );
    } catch (_) {
      // skip invalid candidate
    }
  }

  if (!candidates.length) {
    throw new Error("Could not parse Rakuten Loto7 page");
  }

  const sortedCandidates = sortRecordsNewestFirst(candidates);

  const existingMax = existingRecords.reduce((max, rec) => {
    const n = Number(rec.drawNumber || 0);
    return n > max ? n : max;
  }, 0);

  return sortedCandidates.find(c => Number(c.drawNumber) > existingMax) || sortedCandidates[0];
}

async function fetchLatestRecord(existingRecords) {
  const html = await fetchHtml(SOURCE_URL);
  const record = parseRakutenPage(html, existingRecords);
  log("INFO", `parsed candidate drawNumber=${record.drawNumber}, drawDate=${record.drawDate}`);
  return record;
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
      writeCsv(records);
      return;
    }

    writeCsv(records);
    log("INFO", `success: appended drawNumber ${incomingRecord.drawNumber}`);
  } catch (err) {
    const message = String(err?.message || err);

    if (message.includes("HTTP 403")) {
      log("WARN", "source blocked request with HTTP 403; skip update");
      return;
    }

    log("ERROR", err.stack || err.message);
    process.exitCode = 1;
  }
}

main();
