// functions/src/financialTotals.js
"use strict";

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");
if (admin.apps.length === 0) admin.initializeApp();
const db = admin.firestore();
const Timestamp = admin.firestore.Timestamp;

/* ─────────────────────────── date helpers ─────────────────────────── */

function parseIsoOrTimestamp(v) {
  if (!v) return null;
  if (v instanceof Timestamp) return v;
  if (typeof v === "string") {
    const d = new Date(v);
    if (!isNaN(d.getTime())) return Timestamp.fromDate(d);
  }
  return null;
}

function startOfTodayPK() {
  // Asia/Karachi (UTC+5)
  const now = new Date();
  const pk = new Date(now.getTime() + 5 * 60 * 60 * 1000);
  const startPk = new Date(Date.UTC(pk.getUTCFullYear(), pk.getUTCMonth(), pk.getUTCDate(), 0, 0, 0, 0));
  const startUtc = new Date(startPk.getTime() - 5 * 60 * 60 * 1000);
  return Timestamp.fromDate(startUtc);
}

function addDays(ts, days) {
  const d = ts.toDate();
  d.setUTCDate(d.getUTCDate() + days);
  return Timestamp.fromDate(d);
}

function monthWindowFromKey(key /* "YYYY-MM" */) {
  const [y, m] = key.split("-").map(Number);
  if (!y || !m) throw new Error("Invalid month key");
  // Convert Karachi month edges to UTC
  const startK = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0, 0));
  const endK   = new Date(Date.UTC(y, m,     1, 0, 0, 0, 0));
  const startU = new Date(startK.getTime() - 5 * 60 * 60 * 1000);
  const endU   = new Date(endK.getTime()   - 5 * 60 * 60 * 1000);
  return { start: Timestamp.fromDate(startU), end: Timestamp.fromDate(endU) };
}

function resolveRange(input) {
  if (!input || input === "allTime") return { start: null, end: null };

  if (input === "today") {
    const start = startOfTodayPK();
    const end = addDays(start, 1);
    return { start, end };
  }
  if (input === "last7Days") {
    const end = Timestamp.now();
    const start = Timestamp.fromDate(new Date(end.toDate().getTime() - 7 * 86400000));
    return { start, end };
  }
  if (input === "last30Days") {
    const end = Timestamp.now();
    const start = Timestamp.fromDate(new Date(end.toDate().getTime() - 30 * 86400000));
    return { start, end };
  }
  if (typeof input === "string" && input.startsWith("month:")) {
    const key = input.slice("month:".length);
    return monthWindowFromKey(key);
  }
  if (typeof input === "object") {
    const start = parseIsoOrTimestamp(input.start);
    const end   = parseIsoOrTimestamp(input.end);
    if (!start || !end) throw new HttpsError("invalid-argument", "Invalid custom date range");
    return { start, end };
  }
  throw new HttpsError("invalid-argument", "Unknown range format");
}

/* ───────────────────── amount + range helpers ───────────────────── */

function toNumberSafe(v) {
  if (typeof v === "number") return v;
  if (typeof v === "string") return Number.parseFloat(v) || 0;
  return Number(v || 0);
}

function pickFirst(doc, fields) {
  for (const f of fields) {
    const v = doc.get(f);
    if (v !== undefined && v !== null) return v;
  }
  return undefined;
}

// Normalize mixed timestamp field types (Timestamp, epoch ms/sec, ISO string)
function normalizeToDate(anyTs) {
  if (!anyTs && anyTs !== 0) return null;
  if (anyTs instanceof Timestamp) return anyTs.toDate();
  if (anyTs instanceof Date) return isNaN(anyTs.getTime()) ? null : anyTs;
  if (typeof anyTs === "number") {
    const ms = anyTs >= 1e12 ? anyTs : anyTs * 1000; // sec→ms heuristic
    const d = new Date(ms);
    return isNaN(d.getTime()) ? null : d;
  }
  if (typeof anyTs === "string") {
    const d = new Date(anyTs);
    return isNaN(d.getTime()) ? null : d;
  }
  return null;
}

/** Decide which amount field actually exists in the result set. */
async function chooseAmountFieldForAggregate(q, candidates) {
  try {
    const probe = await q.limit(50).get();
    if (!probe.empty) {
      for (const c of candidates) {
        const found = probe.docs.some(d => d.get(c) !== undefined && d.get(c) !== null);
        if (found) return c;
      }
    }
  } catch (_) { /* ignore */ }
  return candidates[0] || "amount";
}

/** NEW: pick a time field that actually EXISTS on docs before applying DB-side range. */
async function chooseUsableTimeField(qBase, candidates) {
  try {
    const probe = await qBase.limit(50).get();
    if (probe.empty) return null;
    for (const field of candidates) {
      const anyHas = probe.docs.some(d => d.get(field) !== undefined && d.get(field) !== null);
      if (anyHas) return field;
    }
  } catch (_) { /* ignore */ }
  return null;
}

/**
 * Try SUM via AggregateField on a real existing amount field.
 * Falls back to manual paging where it picks the first present amount field per doc.
 */
async function sumQuery(q, amountFieldCandidates = ["amount"]) {
  try {
    const primary = await chooseAmountFieldForAggregate(q, amountFieldCandidates);
    const agg = q.aggregate({ total: admin.firestore.AggregateField.sum(primary) });
    const snap = await agg.get();
    const v = snap.data().total;
    return Number(v || 0);
  } catch (e) {
    logger.warn(`Aggregate SUM fallback: ${e.message}`);
  }

  let total = 0;
  let snap = await q.limit(500).get();
  while (!snap.empty) {
    for (const d of snap.docs) {
      const amt = pickFirst(d, amountFieldCandidates);
      total += (typeof amt === "number") ? amt : (parseFloat(amt) || 0);
    }
    const last = snap.docs[snap.docs.length - 1];
    snap = await q.startAfter(last).limit(500).get();
  }
  return total;
}

/**
 * Apply range on a candidate time field that BOTH:
 *  - exists on the docs, and
 *  - has a usable index (probe read).
 * Otherwise, return base query so we can do manual date filtering.
 */
async function withRangeSmart(qBase, start, end, candidates) {
  if (!start && !end) return { q: qBase, applied: false, field: null };

  const field = await chooseUsableTimeField(qBase, candidates);
  if (!field) {
    logger.warn("[withRangeSmart] No candidate time field present on docs; will manual-filter.");
    return { q: qBase, applied: false, field: null };
  }

  try {
    let q = qBase;
    if (start) q = q.where(field, ">=", start);
    if (end)   q = q.where(field, "<",  end);
    await q.limit(1).get(); // probe index
    return { q, applied: true, field };
  } catch (e) {
    logger.warn(`[withRangeSmart] Field "${field}" not indexable now (${e.message}); manual-filtering.`);
    return { q: qBase, applied: false, field: null };
  }
}

// Manual sum with in-code time filtering (exact; handles mixed time types)
async function sumQueryManualTimeFilter(qBase, start, end, timeFields, amountFields = ["amount"]) {
  let total = 0;
  let snap = await qBase.limit(500).get();
  const startD = start ? start.toDate() : null;
  const endD   = end   ? end.toDate()   : null;

  while (!snap.empty) {
    for (const d of snap.docs) {
      const tsRaw = pickFirst(d, timeFields);
      const t = normalizeToDate(tsRaw);
      let inRange = true;
      if (startD || endD) {
        if (!t) inRange = false;
        else {
          if (startD && t < startD) inRange = false;
          if (endD && t >= endD)    inRange = false;
        }
      }
      if (!inRange) continue;

      const amt = pickFirst(d, amountFields);
      total += (typeof amt === "number") ? amt : (parseFloat(amt) || 0);
    }
    const last = snap.docs[snap.docs.length - 1];
    snap = await qBase.startAfter(last).limit(500).get();
  }
  return total;
}

// Use DB-side range if safely applicable; else manual filter to stay correct.
async function sumWithRangeSmart(qBase, start, end, timeFields, amountFields) {
  const { q, applied } = await withRangeSmart(qBase, start, end, timeFields);
  try {
    const sum = await sumQuery(q, amountFields);
    if ((start || end) && !applied) {
      return await sumQueryManualTimeFilter(qBase, start, end, timeFields, amountFields);
    }
    return sum;
  } catch {
    if (start || end) {
      return await sumQueryManualTimeFilter(qBase, start, end, timeFields, amountFields);
    }
    throw e;
  }
}

/* ───────────────────────────── callable ───────────────────────────── */

exports.getFinancialTotals = onCall({ region: "us-central1", timeoutSeconds: 60, memory: "1GiB" },
  async (req) => {
    try {
      const range = req.data?.range || "allTime";
      const { start, end } = resolveRange(range);

      // TIME FIELDS — ordered by how your code updates them:
      // deposits: created with "timestamp"; IPN later may set "updatedAt"
      // We'll consider both, but ONLY apply DB-side filtering if the field exists on docs.
      const TIME_DEPOSITS = ["updatedAt", "timestamp", "createdAt"];

      // withdrawals: created with "createdAt"; completion flips status and sets "updatedAt"
      const TIME_WITHDRAWALS_COMPLETED = ["updatedAt", "createdAt", "timestamp"];

      // transactions (general): adminTopUp writes "timestamp"; rank reward uses "createdAt"
      const TIME_TX_GENERAL = ["timestamp", "createdAt", "created", "time"];

      /* 1) Organic Deposits — deposits.status == "approved"
            Prefer approval time (updatedAt) if present; sum 'amount' */
      const qDepositOrganic = db.collection("deposits")
        .where("status", "==", "approved");
      const totalDepositOrganic = await sumWithRangeSmart(
        qDepositOrganic,
        start, end,
        TIME_DEPOSITS,
        ["amount"]
      );

      /* 2) Admin Deposits — transactions: type==deposit,status==approved,source==admin */
      const qDepositAdmin = db.collection("transactions")
        .where("type", "==", "deposit")
        .where("status", "==", "approved")
        .where("source", "==", "admin");
      const totalDepositAdmin = await sumWithRangeSmart(
        qDepositAdmin,
        start, end,
        TIME_TX_GENERAL,
        ["amount"]
      );

      /* 3) Withdrawals (completed) — sum NET, filter by completion time (updatedAt) */
      const qWithdraw = db.collection("withdrawals")
        .where("status", "==", "completed");
      const totalWithdrawn = await sumWithRangeSmart(
        qWithdraw,
        start, end,
        TIME_WITHDRAWALS_COMPLETED,
        ["amountNet", "netAmount", "amount", "amountUsd", "amountUSD", "usd", "netUsd", "netUSD", "amountGross"]
      );

      /* 4) Salary — transactions.type == "salary" */
      const qSalary = db.collection("transactions")
        .where("type", "==", "salary");
      const totalSalary = await sumWithRangeSmart(
        qSalary,
        start, end,
        TIME_TX_GENERAL,
        ["amount"]
      );

      /* 5) ROI — transactions.type == "dailyRoi", status == "collected" */
      const qRoi = db.collection("transactions")
        .where("type", "==", "dailyRoi")
        .where("status", "==", "collected");
      const totalRoi = await sumWithRangeSmart(
        qRoi,
        start, end,
        TIME_TX_GENERAL,
        ["amount"]
      );

      /* 6) Team Profit — transactions.type == "teamProfit", status == "collected" */
      const qTeam = db.collection("transactions")
        .where("type", "==", "teamProfit")
        .where("status", "==", "collected");
      const totalTeamProfit = await sumWithRangeSmart(
        qTeam,
        start, end,
        TIME_TX_GENERAL,
        ["amount"]
      );

      /* 7) Rank Rewards — transactions.type == "rank-reward", status == "success"
            prefer createdAt first for rank rewards */
      const qRank = db.collection("transactions")
        .where("type", "==", "rank-reward")
        .where("status", "==", "success");
      const totalRankRewards = await sumWithRangeSmart(
        qRank,
        start, end,
        ["createdAt", "timestamp", "created"],
        ["amount", "reward", "rewardAmount"]
      );

      /* 8) Direct Income — transactions.type == "Direct Profit" */
      const qDirect = db.collection("transactions")
        .where("type", "==", "Direct Profit");
      const totalDirectIncome = await sumWithRangeSmart(
        qDirect,
        start, end,
        TIME_TX_GENERAL,
        ["amount"]
      );

      const out = {
        totalDepositOrganic,
        totalDepositAdmin,
        totalWithdrawn,
        totalSalary,
        totalRoi,
        totalTeamProfit,
        totalRankRewards,
        totalDirectIncome,
        updatedAt: new Date().toISOString(),
        range: range === "allTime" ? { start: null, end: null } : { start, end }
      };

      logger.info("[getFinancialTotals] OK", out);
      return out;
    } catch (e) {
      logger.error("[getFinancialTotals] error", e);
      throw new HttpsError("internal", e.message || "Failed to compute totals");
    }
  }
);
