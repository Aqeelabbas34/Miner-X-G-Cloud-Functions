// index.js
// ─────────────────────────────────────────────────────────────
// STAR SALARY PROGRAM (calendar-month, Asia/Karachi, live preview)
// ─────────────────────────────────────────────────────────────
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { logger } = require("firebase-functions");
const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const { initializeApp, getApps } = require("firebase-admin/app");
const { getFirestore, Timestamp, FieldValue } = require("firebase-admin/firestore");

if (!getApps().length) initializeApp();
const { getMessaging } = require("firebase-admin/messaging");
const messaging = getMessaging();
const db = getFirestore();

/* ───────────────────────── Config ───────────────────────── */
// Karachi is +05:00 year-round (no DST)
const KARACHI_OFFSET_MIN = 300;

// Star table — EXACT per your screenshot
const STAR_TABLE = [
  { star: 1, self: 100,  direct:  1000,  indirect:  3000,  salary:  200 },
  { star: 2, self: 200,  direct:  2000,  indirect:  6000,  salary:  400 },
  { star: 3, self: 400,  direct:  4000,  indirect: 12000,  salary:  800 },
  { star: 4, self: 800,  direct:  8000,  indirect: 24000,  salary: 1600 },
  { star: 5, self: 1600, direct: 16000,  indirect: 48000,  salary: 3500 },
];

/* ─────────────────── Time helpers (Asia/Karachi) ─────────────────── */
function toKarachi(dateUtc) {
  return new Date(dateUtc.getTime() + KARACHI_OFFSET_MIN * 60 * 1000);
}
function fromKarachi(dateKarachi) {
  return new Date(dateKarachi.getTime() - KARACHI_OFFSET_MIN * 60 * 1000);
}
function pad2(n) { return String(n).padStart(2, "0"); }

/** Month key (YYYY-MM) in Asia/Karachi from a Firestore Timestamp */
function monthKeyFromTs(ts) {
  const dK = toKarachi(ts.toDate());
  return `${dK.getUTCFullYear()}-${pad2(dK.getUTCMonth() + 1)}`;
}

/** Current Karachi month key from "now" (UTC Date) */
function currentMonthKeyFromNow(nowUtc = new Date()) {
  const dK = toKarachi(nowUtc);
  return `${dK.getUTCFullYear()}-${pad2(dK.getUTCMonth() + 1)}`;
}

/** Last month Karachi month key from "now" (UTC Date) */
function lastMonthKeyFromNow(nowUtc = new Date()) {
  const dK = toKarachi(nowUtc);
  let y = dK.getUTCFullYear();
  let m = dK.getUTCMonth(); // 0..11 current month index
  // last month
  m = m - 1;
  if (m < 0) { m = 11; y -= 1; }
  return `${y}-${pad2(m + 1)}`;
}

/** Window [startUTC, endUTC) for a Karachi monthKey */
function monthWindowUtc(monthKey /* "YYYY-MM" */) {
  const [y, m] = monthKey.split("-").map(Number);
  const startKarachi = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0, 0)); // K-midnight
  const endKarachi   = new Date(Date.UTC(y, m,     1, 0, 0, 0, 0)); // next K-midnight
  const startUtc = fromKarachi(startKarachi);
  const endUtc   = fromKarachi(endKarachi);
  return {
    start: Timestamp.fromDate(startUtc),
    end:   Timestamp.fromDate(endUtc),
  };
}

/* ─────────────────── Star helpers ─────────────────── */
function computeStar(self, direct, indirect) {
  // Highest star for which all thresholds are met
  let best = { star: 0, salary: 0, req: null };
  for (const row of STAR_TABLE) {
    if (self >= row.self && direct >= row.direct && indirect >= row.indirect) {
      if (row.star > best.star) best = { star: row.star, salary: row.salary, req: row };
    }
  }
  return best;
}

function remainingToNextStar(self, direct, indirect) {
  // First star above current best; if at max, all zeros
  let next = null;
  for (const row of STAR_TABLE) {
    if (self < row.self || direct < row.direct || indirect < row.indirect) {
      next = row;
      break;
    }
  }
  if (!next) return { self: 0, direct: 0, indirect: 0, nextStar: null, nextSalary: 0 };
  return {
    self: Math.max(0, next.self - self),
    direct: Math.max(0, next.direct - direct),
    indirect: Math.max(0, next.indirect - indirect),
    nextStar: next.star,
    nextSalary: next.salary,
  };
}

/* ─────────────────── Ancestry (referral chain) ─────────────────── */
/**
 * Returns array of ancestor UIDs starting from parent (L1) upward: [p1, p2, p3, ...]
 * Uses users.{ uid, referralCode }, terminates on missing/loop, max depth 20.
 */
async function getAncestors(userId) {
  const out = [];
  const seen = new Set();
  let current = userId;
  for (let depth = 0; depth < 20; depth++) {
    const qs = await db.collection("users").where("uid", "==", current).limit(1).get();
    if (qs.empty) break;
    const userDoc = qs.docs[0];
    const parent = userDoc.get("referralCode");
    if (!parent || seen.has(parent)) break;
    out.push(parent);
    seen.add(parent);
    current = parent;
  }
  return out; // [L1, L2, ...]
}

/* ─────────────────── Aggregator Trigger (race-free) ───────────────────
   On each Plan Purchase, atomically increment monthlyVolumes for buyer (self),
   parent (direct), and all higher ancestors (indirect). Idempotent via aggLocks/{txId}.
----------------------------------------------------------------*/
exports.aggregatePlanPurchase = onDocumentCreated("transactions/{txId}", async (event) => {
  const snap = event.data;
  if (!snap) return;

  const tx = snap.data();
  if (!tx || tx.type !== "Plan Purchase") return;

  const txId = snap.id;
  const userId = tx.userId;
  const amount = Number(tx.amount || 0);
  const ts = tx.timestamp instanceof Timestamp ? tx.timestamp : Timestamp.now();

  if (!userId || !amount || amount <= 0) return;

  // Derive Karachi month key from tx timestamp
  const monthKey = monthKeyFromTs(ts);

  // Fetch ancestors OUTSIDE the transaction (stable, low contention)
  const ancestors = await getAncestors(userId); // [L1, L2, L3, ...]

  const lockRef = db.collection("aggLocks").doc(txId);

  // 🔒 Single transaction: (check lock) → (all increments) → (write lock)
  await db.runTransaction(async (tr) => {
    const seen = await tr.get(lockRef);
    if (seen.exists) {
      // Already processed; no-op
      return;
    }

    // 1) Self increment for buyer
    const selfRef = db.doc(`monthlyVolumes/${monthKey}/users/${userId}`);
    tr.set(selfRef, {
      self: FieldValue.increment(amount),
      direct: FieldValue.increment(0),
      indirect: FieldValue.increment(0),
      lastUpdatedAt: FieldValue.serverTimestamp(),
    }, { merge: true });

    // 2) Direct for L1 (if any)
    if (ancestors[0]) {
      const p1Ref = db.doc(`monthlyVolumes/${monthKey}/users/${ancestors[0]}`);
      tr.set(p1Ref, {
        self: FieldValue.increment(0),
        direct: FieldValue.increment(amount),
        indirect: FieldValue.increment(0),
        lastUpdatedAt: FieldValue.serverTimestamp(),
      }, { merge: true });
    }

    // 3) Indirect for all L2+
    for (let i = 1; i < ancestors.length; i++) {
      const ancUid = ancestors[i];
      const aRef = db.doc(`monthlyVolumes/${monthKey}/users/${ancUid}`);
      tr.set(aRef, {
        self: FieldValue.increment(0),
        direct: FieldValue.increment(0),
        indirect: FieldValue.increment(amount),
        lastUpdatedAt: FieldValue.serverTimestamp(),
      }, { merge: true });
    }

    // 4) Idempotency lock (marks the whole aggregation as done)
    tr.set(lockRef, {
      monthKey,
      userId,
      amount,
      createdAt: FieldValue.serverTimestamp(),
      done: true,
    }, { merge: true });
  });

  logger.info(`aggregatePlanPurchase: committed tx=${txId} uid=${userId} mk=${monthKey} amt=${amount}`);
});

/* ─────────────────── Fallback helpers (read-only compute) ─────────────────── */

const MAX_DESC_NODES = 2000;     // safety cap for very large teams
const IN_CHUNK = 10;             // Firestore "in" limit

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// Prefer 'uid' field, fall back to doc.id if needed
function uidOf(doc) {
  return doc.get && doc.get("uid") ? doc.get("uid") : (doc.id || null);
}

/** Get direct children (L1) UIDs of a parent */
async function getDirectChildrenUids(parentUid) {
  const snap = await db.collection("users")
    .where("referralCode", "==", parentUid)
    .get();
  if (snap.empty) return [];
  return snap.docs.map(uidOf).filter(Boolean);
}

/** Get full descendant sets via BFS: { direct: L1[], indirect: L2+[] } */
async function getDescendantsUids(rootUid) {
  const direct = await getDirectChildrenUids(rootUid);
  if (direct.length === 0) return { direct: [], indirect: [] };

  const indirect = [];
  let frontier = [...direct];
  let visited = new Set(frontier);
  let total = frontier.length;

  while (frontier.length > 0 && total < MAX_DESC_NODES) {
    // Query children of the current frontier in chunks of 10 (referralCode IN ...)
    const next = [];
    const chunks = chunk(frontier, IN_CHUNK);
    const promises = chunks.map(async (ids) => {
      const q = await db.collection("users")
        .where("referralCode", "in", ids)
        .get();
      for (const d of q.docs) {
        const childUid = uidOf(d);
        if (childUid && !visited.has(childUid)) {
          next.push(childUid);
          visited.add(childUid);
        }
      }
    });
    await Promise.all(promises);

    // Accumulate newly found nodes
    for (const u of next) {
      indirect.push(u);
    }
    total += next.length;
    frontier = next;
  }
  return { direct, indirect };
}

/** Sum "Plan Purchase" transactions for a set of userIds within [start, end) */
async function sumPlanPurchasesForUsers(userIds, startTs, endTs) {
  if (!userIds || userIds.length === 0) return 0;
  let total = 0;

  const batches = chunk(userIds, IN_CHUNK);
  for (const ids of batches) {
    try {
      const q = await db.collection("transactions")
        .where("type", "==", "Plan Purchase")
        .where("userId", "in", ids)
        .where("timestamp", ">=", startTs)
        .where("timestamp", "<", endTs)
        .get();

      for (const d of q.docs) {
        const amt = Number(d.get("amount") || 0);
        if (amt > 0) total += amt;
      }
    } catch (e) {
      // On index errors or "in" issues, fall back to per-user reads
      for (const uid of ids) {
        const q2 = await db.collection("transactions")
          .where("type", "==", "Plan Purchase")
          .where("userId", "==", uid)
          .where("timestamp", ">=", startTs)
          .where("timestamp", "<", endTs)
          .get();
        for (const d of q2.docs) {
          const amt = Number(d.get("amount") || 0);
          if (amt > 0) total += amt;
        }
      }
    }
  }
  return total;
}

/** Compute month-to-date totals for self/direct/indirect (read-only) */
async function computeLiveMonthTotals(userId, monthKey) {
  const { start, end } = monthWindowUtc(monthKey);

  // Self
  const self = await sumPlanPurchasesForUsers([userId], start, end);

  // Direct + Indirect via referral tree (L1 vs L2+)
  const { direct: L1, indirect: L2plus } = await getDescendantsUids(userId);

  const direct = await sumPlanPurchasesForUsers(L1, start, end);
  const indirect = await sumPlanPurchasesForUsers(L2plus, start, end);

  return { self, direct, indirect };
}

/* ─────────────────── UI Preview (callable) ───────────────────
   getStarPreview({ userId, monthKey? })
   - If monthKey provided: returns locked snapshot from monthlySalaries (if exists).
   - Else: returns current month-to-date from monthlyVolumes; if missing, computes live totals read-only.
----------------------------------------------------------------*/
exports.getStarPreview = onCall({ region: "us-central1" }, async (req) => {
  const userId = req.data && req.data.userId;
  const requestedMonthKey = req.data && req.data.monthKey;
  if (!userId) throw new HttpsError("invalid-argument", "userId is required");

  if (requestedMonthKey) {
    // Historical snapshot
    const msRef = db.doc(`monthlySalaries/${userId}_${requestedMonthKey}`);
    const ms = await msRef.get();
    if (!ms.exists) {
      // If snapshot not found, return zeros but include the window for clarity
      const w = monthWindowUtc(requestedMonthKey);
      return {
        mode: "snapshot",
        monthKey: requestedMonthKey,
        window: { start: w.start.toDate().toISOString(), end: w.end.toDate().toISOString(), tz: "Asia/Karachi" },
        self: 0, direct: 0, indirect: 0, star: 0, salary: 0, locked: false, paidAt: null,
      };
    }
    const d = ms.data();
    return {
      mode: "snapshot",
      monthKey: requestedMonthKey,
      window: d.window || null,
      self: d.self || 0,
      direct: d.direct || 0,
      indirect: d.indirect || 0,
      star: d.star || 0,
      salary: d.salary || 0,
      locked: true,
      paidAt: d.paidAt || null,
      userStatusAtLock: d.userStatusAtLock || null,
    };
  }

  // Live preview (current month-to-date)
  const mk = currentMonthKeyFromNow(new Date());
  const w = monthWindowUtc(mk);

  // 1) Try fast path from monthlyVolumes
  const mvRef = db.doc(`monthlyVolumes/${mk}/users/${userId}`);
  const mv = await mvRef.get();
  let self = 0, direct = 0, indirect = 0;

  if (mv.exists) {
    self = Number(mv.get("self") || 0);
    direct = Number(mv.get("direct") || 0);
    indirect = Number(mv.get("indirect") || 0);
  }

  // 2) Fallback path: if missing OR obviously uninitialized, compute on demand (read-only)
  const looksEmpty = (!mv.exists) || ((self + direct + indirect) === 0);
  if (looksEmpty) {
    try {
      const totals = await computeLiveMonthTotals(userId, mk);
      self = totals.self;
      direct = totals.direct;
      indirect = totals.indirect;
    } catch (e) {
      // If anything fails, keep zeros but still show targets
      logger.warn(`getStarPreview fallback compute failed for uid=${userId} mk=${mk}`, e);
    }
  }

  // 3) Compute star & remaining from the obtained numbers (even if zeros)
  const best = computeStar(self, direct, indirect);
  const remaining = remainingToNextStar(self, direct, indirect);

  return {
    mode: "preview",
    monthKey: mk,
    window: { start: w.start.toDate().toISOString(), end: w.end.toDate().toISOString(), tz: "Asia/Karachi" },
    self, direct, indirect,
    starIfLockedNow: best.star,
    salaryIfLockedNow: best.salary,
    remainingToNextStar: remaining,
  };
});

/* ─────────────────── Scheduler (daily @ 00:10 Asia/Karachi) ───────────────────
   Locks last month, computes star, and pays salary once (idempotent on transactions doc).
----------------------------------------------------------------*/


// Helpers to read tokens exactly like your other file
function extractTokensFromUserSnap(userSnap) {
  const raw1 = userSnap.get("deviceToken");
  const raw2 = userSnap.get("fcmToken");
  const toArr = (v) =>
    Array.isArray(v) ? v : (typeof v === "string" && v.trim() ? [v.trim()] : []);
  // unique + non-empty
  return [...new Set([...toArr(raw1), ...toArr(raw2)].filter(Boolean))];
}

async function resolveUserDoc(uid) {
  // Try doc id == uid
  const byId = await db.collection("users").doc(uid).get();
  if (byId.exists) return byId;
  // Fallback where uid == ...
  const q = await db.collection("users").where("uid", "==", uid).limit(1).get();
  if (!q.empty) return q.docs[0];
  return null;
}

async function sendFcmToUid(uid, payload) {
  const userSnap = await resolveUserDoc(uid);
  if (!userSnap) return;
  const tokens = extractTokensFromUserSnap(userSnap);
  if (!tokens.length) return;

  const res = await messaging.sendEachForMulticast({
    tokens,
    notification: payload.notification ?? undefined,
    data: payload.data ?? undefined,
  });

  // Optional logging; harmless if you keep it
  logger.info(`[FCM] salary uid=${uid} success=${res.successCount} failure=${res.failureCount}`);
}

exports.starSalaryScheduler = onSchedule(
  { schedule: "every day 00:10", timeZone: "Asia/Karachi", region: "us-central1" },
  async () => {
    const now = new Date(); // UTC now
    const monthKey = lastMonthKeyFromNow(now);
    const { start, end } = monthWindowUtc(monthKey);

    logger.info(`starSalaryScheduler: locking & paying month ${monthKey} [${start.toDate().toISOString()}..${end.toDate().toISOString()})`);

    // Users with any activity last month
    const mvCol = db.collection(`monthlyVolumes/${monthKey}/users`);
    const mvSnap = await mvCol.get();
    if (mvSnap.empty) {
      logger.info(`No monthlyVolumes for ${monthKey}`);
      return;
    }

    for (const doc of mvSnap.docs) {
      const userId = doc.id;
      const self = doc.get("self") || 0;
      const direct = doc.get("direct") || 0;
      const indirect = doc.get("indirect") || 0;

      // Admin approval required
      const reqDocId = `${userId}_${monthKey}`;
      const reqRef = db.collection("salaryRewardRequests").doc(reqDocId);
      const reqSnap = await reqRef.get();
      if (!reqSnap.exists || reqSnap.get("status") !== "APPROVED") {
        logger.info(`User ${userId} skipped: no approved salary request for ${monthKey}`);
        continue;
      }

      // Lock snapshot (and audit)
      const userSnap = await db.collection("users").where("uid", "==", userId).limit(1).get();
      let activeAtLock = false;
      let blockedAtLock = false;
      if (!userSnap.empty) {
        const u = userSnap.docs[0];
        activeAtLock = (u.get("status") === "active");
        blockedAtLock = !!u.get("blocked");
      }

      const best = computeStar(self, direct, indirect);
      const msRef = db.doc(`monthlySalaries/${userId}_${monthKey}`);
      await msRef.set({
        userId, monthKey,
        self, direct, indirect,
        star: best.star, salary: best.salary,
        lockedAt: FieldValue.serverTimestamp(),
        userStatusAtLock: activeAtLock ? "active" : "inactive_or_missing",
        blockedAtLock,
        window: { start: start.toDate().toISOString(), end: end.toDate().toISOString(), tz: "Asia/Karachi" },
      }, { merge: true });

      // Eligibility: must be active & not blocked at lock AND at payout
      if (!activeAtLock || blockedAtLock || best.salary <= 0) {
        logger.info(`User ${userId} not eligible at lock or no salary (star=${best.star}, salary=${best.salary}).`);
        // Optional: reflect skip on request
        await reqRef.set({
          payout: {
            status: "SKIPPED",
            reason: best.salary <= 0 ? "no_salary_for_month" : "user_inactive_or_blocked",
            checkedAt: FieldValue.serverTimestamp(),
          }
        }, { merge: true });
        continue;
      }

      const salaryTxId = `salaryStar_${userId}_${monthKey}`;
      const txRef = db.collection("transactions").doc(salaryTxId);

      let paidAmount = 0;
      let paidStar = 0;

      await db.runTransaction(async (tr) => {
        const existing = await tr.get(txRef);
        if (existing.exists) return; // idempotent: already paid

        // Double-check active at payout moment
        const userSnap2 = await db.collection("users").where("uid", "==", userId).limit(1).get();
        let activeAtPayout = false;
        let blockedAtPayout = false;
        if (!userSnap2.empty) {
          const u2 = userSnap2.docs[0];
          activeAtPayout = (u2.get("status") === "active");
          blockedAtPayout = !!u2.get("blocked");
        }
        if (!activeAtPayout || blockedAtPayout) {
          // Update snapshot with reason; skip pay
          tr.set(msRef, {
            payoutSkipped: true,
            payoutSkipReason: "user_inactive_or_blocked_at_payout",
            checkedAt: FieldValue.serverTimestamp(),
          }, { merge: true });
          // Mirror on request doc
          tr.set(reqRef, {
            payout: {
              status: "SKIPPED",
              reason: "user_inactive_or_blocked_at_payout",
              checkedAt: FieldValue.serverTimestamp(),
            }
          }, { merge: true });
          return;
        }

        // Credit earnings wallet (nested under earnings)
        const accSnap = await db.collection("accounts").where("userId", "==", userId).limit(1).get();
        let accRef;
        if (accSnap.empty) {
          // Create minimal accounts doc
          accRef = db.collection("accounts").doc();
          tr.set(accRef, { userId, earnings: { salaryIncome: 0, totalEarned: 0, totalEarnedToDate: 0 } }, { merge: true });
        } else {
          accRef = accSnap.docs[0].ref;
        }

        tr.set(accRef, {
          earnings: {
            salaryIncome: FieldValue.increment(best.salary),
            totalEarned: FieldValue.increment(best.salary),
            totalEarnedToDate: FieldValue.increment(best.salary),
          }
        }, { merge: true });

        // Ledger transaction (id = salaryStar_{uid}_{YYYY-MM}, serves as idempotency)
        tr.set(txRef, {
          type: "salary",
          userId,
          amount: best.salary,
          periodKey: monthKey,
          breakdown: { self, direct, indirect, star: best.star },
          source: "Star Program",
          timestamp: FieldValue.serverTimestamp()
        });

        // Mark monthlySalaries as paid
        tr.set(msRef, { paidAt: FieldValue.serverTimestamp() }, { merge: true });

        // Also mark on the request for admin visibility
        tr.set(reqRef, {
          payout: {
            status: "PAID",
            txId: salaryTxId,
            amount: best.salary,
            star: best.star,
            paidAt: FieldValue.serverTimestamp(),
          }
        }, { merge: true });

        paidAmount = best.salary;
        paidStar = best.star;
      });

      // If paid successfully this run (not skipped / not already existed) → notify
      if (paidAmount > 0) {
        try {
          await sendFcmToUid(userId, {
            notification: {
              title: "🎉 Salary credited",
              body:  `Your Star ${paidStar} salary $${paidAmount} has been added to your wallet.`
            },
            data: {
              type: "star-salary",
              monthKey,
              star: String(paidStar),
              amount: String(paidAmount)
            }
          });
        } catch (e) {
          logger.warn(`[FCM] Failed to notify uid=${userId} for salary credit`, e);
        }
        logger.info(`Paid salary for ${userId} month=${monthKey} amount=${paidAmount}`);
      }
    }
  }
);


// ─────────────────────────────────────────────────────────────
// NEW: Apply endpoint (last 7 days of month, Asia/Karachi)
// ─────────────────────────────────────────────────────────────
function isInLast7Days(now = new Date()) {
  const dK = toKarachi(now);                  // shift to Karachi
  const y = dK.getUTCFullYear();
  const m = dK.getUTCMonth();                 // 0-based month in Karachi
  const daysInMonth = new Date(y, m + 1, 0).getUTCDate();
  const today = dK.getUTCDate();
  // last 7 days inclusive → (daysInMonth-6 ... daysInMonth)
  return today >= (daysInMonth - 6) && today <= daysInMonth;
}

// ─────────────────────────────────────────────────────────────
// Apply endpoint (last 7 days of month, Asia/Karachi) — uses CUSTOM UID
// ─────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────
// Apply endpoint (last 7 days of month, Asia/Karachi) — uses CUSTOM UID
// Stores star + salary snapshot for admin review
// ─────────────────────────────────────────────────────────────
exports.applyForStarSalary = onCall({ region: "us-central1" }, async (req) => {
  const userId = (req.data && String(req.data.userId || "").trim());
  if (!userId) {
    throw new HttpsError("invalid-argument", "userId (custom) is required");
  }

  // 1) Window guard (server-side)
  if (!isInLast7Days()) {
    throw new HttpsError(
      "failed-precondition",
      "Apply window closed (only last 7 days of the month, Asia/Karachi)."
    );
  }

  // 2) Build id & compute snapshot for admin
  const monthKey = currentMonthKeyFromNow(new Date());
  const docId = `${userId}_${monthKey}`;
  const ref = db.collection("salaryRewardRequests").doc(docId);

  // Compute live month-to-date + star & salary
  const totals = await computeLiveMonthTotals(userId, monthKey);     // { self, direct, indirect }
  const best   = computeStar(totals.self, totals.direct, totals.indirect); // { star, salary }
  const rem    = remainingToNextStar(totals.self, totals.direct, totals.indirect);
  const w      = monthWindowUtc(monthKey);

  // 3) Transaction → create only if missing
  await db.runTransaction(async (tr) => {
    const snap = await tr.get(ref);
    if (snap.exists) {
      throw new HttpsError("already-exists", "You already applied this month.");
    }

    tr.set(ref, {
      userId,                 // custom UID (from your SharedPrefs)
      monthKey,
      status: "PENDING",
      createdAt: FieldValue.serverTimestamp(),

      // 🔎 Top-level for quick filtering in admin lists:
      starAtApply:   best.star,
      salaryAtApply: best.salary,

      // 📌 Full snapshot for admin detail
      applySnapshot: {
        self: totals.self,
        direct: totals.direct,
        indirect: totals.indirect,
        star: best.star,
        salary: best.salary,
        remainingToNextStar: rem,
        window: {
          start: w.start.toDate().toISOString(),
          end:   w.end.toDate().toISOString(),
          tz: "Asia/Karachi"
        },
        computedAt: FieldValue.serverTimestamp()
      }
    });
  });

  return { ok: true, id: docId, monthKey, star: best.star, salary: best.salary };
});


