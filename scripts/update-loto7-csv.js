const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

const CSV_PATH = path.resolve(process.cwd(), "loto7-history.csv");
const TMP_CSV_PATH = path.resolve(process.cwd(), "loto7-history.csv.tmp");

const SOURCE_URLS = [
  "https://www.paypay-bank.co.jp/lottery/loto/winning_no.html",
  "https://www.paypay-bank.co.jp/lottery/loto/loto7recent.html"
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
      "user-agent": "Mozilla/5.0 (compatible; LunaSystem/1.0; +https://github.com/)",
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

function pad2(value) {
  return String(Number(value)).padStart(2, "0");
}

function toIsoDateFromJa(text) {
  const m = String(text).match(/(\d{4})\D+(\d{1,2})\D+(\d{1,2})/);
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

  return records;
}

function writeCsv(records) {
  const lines = [
    CSV_HEADERS.join(","),
    ...records.map(rec => CSV_HEADERS.map(h => escapeCsv(rec[h] ?? "")).join(","))
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
    return { updated: false, records: existingRecords };
  }

  const merged = [...existingRecords, incomingRecord].sort(
    (a, b) => Number(a.drawNumber) - Number(b.drawNumber)
  );

  return { updated: true, records: merged };
}

function parseWinningNoPage(html) {
  const $ = cheerio.load(html);
  const text = normalizeWhitespace($("body").text());

  // 例:
  // ロト7 第669回 2026年03月20日（金曜日） 抽せん数字 03050607091316 ボーナス数字 1123
  const regex = /ロト7\s*第\s*(\d+)\s*回.*?(\d{4}年\d{1,2}月\d{1,2}日).*?抽せん数字\s*([0-9]{14}).*?ボーナス数字\s*([0-9]{4})/;
  const m = text.match(regex);
  if (!m) {
    throw new Error("Could not parse winning_no.html for Loto7");
  }

  const drawNumber = String(Number(m[1]));
  const drawDate = toIsoDateFromJa(m[2]);
  const mains = m[3].match(/\d{2}/g);
  const bonus = m[4].match(/\d{2}/g);

  if (!drawDate || !mains || mains.length !== 7 || !bonus || bonus.length !== 2) {
    throw new Error("Invalid parsed data from winning_no.html");
  }

  return normalizeRecord({
    drawNumber,
    drawDate,
    n1: mains[0],
    n2: mains[1],
    n3: mains[2],
    n4: mains[3],
    n5: mains[4],
    n6: mains[5],
    n7: mains[6],
    b1: bonus[0],
    b2: bonus[1]
  });
}

function parseRecentPage(html, existingRecords) {
  const $ = cheerio.load(html);
  const text = normalizeWhitespace($("body").text());

  // 例:
  // 回号 第669回 抽せん日 2026年03月20日（金曜日） 抽せん数字 本数字 03050607091316 ボーナス数字 1123
  const regex = /回号\s*第\s*(\d+)\s*回\s*抽せん日\s*(\d{4}年\d{1,2}月\d{1,2}日).*?本数字\s*([0-9]{14}).*?ボーナス数字\s*([0-9]{4})/g;

  const candidates = [];
  let m;
  while ((m = regex.exec(text)) !== null) {
    const drawNumber = String(Number(m[1]));
    const drawDate = toIsoDateFromJa(m[2]);
    const mains = m[3].match(/\d{2}/g);
    const bonus = m[4].match(/\d{2}/g);

    if (!drawDate || !mains || mains.length !== 7 || !bonus || bonus.length !== 2) {
      continue;
    }

    try {
      candidates.push(normalizeRecord({
        drawNumber,
        drawDate,
        n1: mains[0],
        n2: mains[1],
        n3: mains[2],
        n4: mains[3],
        n5: mains[4],
        n6: mains[5],
        n7: mains[6],
        b1: bonus[0],
        b2: bonus[1]
      }));
    } catch (_) {
      // skip invalid candidate
    }
  }

  if (!candidates.length) {
    throw new Error("Could not parse loto7recent.html");
  }

  candidates.sort((a, b) => Number(b.drawNumber) - Number(a.drawNumber));

  const existingMax = existingRecords.reduce((max, rec) => {
    const n = Number(rec.drawNumber || 0);
    return n > max ? n : max;
  }, 0);

  return candidates.find(c => Number(c.drawNumber) > existingMax) || candidates[0];
}

async function fetchLatestRecord(existingRecords) {
  let lastError = null;

  for (const url of SOURCE_URLS) {
    try {
      const html = await fetchHtml(url);
      const record = url.includes("winning_no.html")
        ? parseWinningNoPage(html)
        : parseRecentPage(html, existingRecords);

      log("INFO", `parsed candidate drawNumber=${record.drawNumber}, drawDate=${record.drawDate}`);
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
