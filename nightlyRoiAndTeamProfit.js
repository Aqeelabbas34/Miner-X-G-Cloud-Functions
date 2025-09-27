"use strict";

/**
 * MXG– Nightly ROI + Team Profit (HYPER-LOGGED & TXN-SAFE)
 * - Phase A: ROI first (sets accounts.earnings.dailyProfit = today's ROI total + lastRoiDate=today)
 * - Phase B: Team Profit second (uses downlines' dailyProfit ONLY if lastRoiDate===today)
 * - Plan expiry by totalAccumulated >= totalPayoutAmount
 * - Idempotent with roiLogs (per user+day), roiPlanLogs (per plan+day), teamLogs (per user+day)
 * - All queries happen outside transactions. Inside tx: gather all doc reads first via tx.get(...),
 *   then perform writes (no tx.get after any write).
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");
const pLimit = require("p-limit");

admin.initializeApp();
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;
const Timestamp = admin.firestore.Timestamp;

/* ───────────────────────────────────────────────────────────────
   Time helpers (Asia/Karachi)
   ─────────────────────────────────────────────────────────────── */
const PK_OFFSET_MS = 5 * 60 * 60 * 1000;

function ymdPK(d = new Date()) {
  const pk = new Date(d.getTime() + PK_OFFSET_MS);
  return pk.toISOString().slice(0, 10); // YYYY-MM-DD
}
// ───────────────────────────────────────────────────────────────
// FCM helpers
// ───────────────────────────────────────────────────────────────
// Replace any previous token lookup with this:
async function getFcmTokensForUid(uid) {
  // Find the users doc whose *field* uid == <uid>
  const q = await db.collection("users").where("uid", "==", uid).limit(1).get();
  if (q.empty) {
    logger.warn(`🔕 No users doc with field uid='${uid}'`);
    return { tokens: [], userRef: null };
  }
  const userDoc = q.docs[0];
  const token = (userDoc.get("deviceToken") || "").trim();
  const tokens = token ? [token] : [];
  if (!tokens.length) logger.warn(`🔕 users/${userDoc.id} has empty deviceToken (uid='${uid}')`);
  return { tokens, userRef: userDoc.ref };
}


async function pruneBadTokens(userRef, tokens, responses) {
  let shouldClear = false;
  responses.forEach((res) => {
    if (!res || res.success) return;
    const code = res.error?.code || "";
    if (
      code.includes("messaging/invalid-registration-token") ||
      code.includes("messaging/registration-token-not-registered")
    ) {
      shouldClear = true;
    }
  });
  if (shouldClear) {
    await userRef.update({ deviceToken: FieldValue.delete() }); // or set: ""
    logger.info(`🧹 Cleared invalid deviceToken for ${userRef.path}`);
  }
}

async function sendEarningsNotification(uid, type, amount, meta = {}) {
  const { tokens, userRef } = await getFcmTokensForUid(uid);
  if (!tokens.length) return;

  const title = type === "dailyRoi" ? "Daily ROI credited" : "Team Profit credited";
  const body  = type === "dailyRoi"
    ? `You received $${Number(amount).toFixed(2)} ROI today.`
    : `You received $${Number(amount).toFixed(2)} team profit today.`;

  const message = {
    tokens,
    notification: { title, body },
    data: {
      type,
      amount: String(amount || 0),
      ...(meta?.address ? { address: meta.address } : {}),
      ...(meta?.dateKey ? { dateKey: meta.dateKey } : {}),
      ...(meta?.txId ? { txId: meta.txId } : {}),
      screen: type === "dailyRoi" ? "WalletScreen" : "TeamScreen",
    },
    android: { priority: "high", notification: { channelId: "earnings", sound: "default" } },
    apns: { payload: { aps: { sound: "default", category: "EARNINGS" } } },
  };

  const resp = await admin.messaging().sendEachForMulticast(message);
  logger.info(`FCM → uid=${uid} type=${type} success=${resp.successCount} failure=${resp.failureCount}`);
  await pruneBadTokens(userRef, tokens, resp.responses);
}
// === NEW: plan-expiry push (idempotent per plan+day) =========================
async function notifyPlanExpiries(uid, planIds, extra = {}) {
  if (!Array.isArray(planIds) || planIds.length === 0) return;

  const dateKey = ymdPK();
  const newlyNotified = [];

  // Try to create a per-plan-per-day notif log; if it already exists, skip
  await Promise.all(
    planIds.map(async (planId) => {
      const ref = db.collection("planExpiryNotifyLogs").doc(`${uid}_${planId}_${dateKey}`);
      try {
        await ref.create({
          userId: uid,
          planId,
          date: dateKey,
          notifiedAt: Timestamp.now(),
        }); // throws if already exists
        newlyNotified.push(planId);
      } catch (e) {
        // ALREADY_EXISTS = 6 in Firestore gRPC codes; skip silently
        if (e?.code !== 6) logger.warn(`planExpiryNotifyLogs create failed uid=${uid} plan=${planId} err=${e.message}`);
      }
    })
  );

  if (!newlyNotified.length) return;

  const { tokens, userRef } = await getFcmTokensForUid(uid);
  if (!tokens.length) return;

  const title = newlyNotified.length === 1 ? "Plan expired" : "Multiple plans expired";
  const body =
    newlyNotified.length === 1
      ? "One of your investment plans has just expired."
      : `${newlyNotified.length} of your investment plans have just expired.`;

  const message = {
    tokens,
    notification: { title, body },
    data: {
      type: "planExpired",
      dateKey,
      planIds: newlyNotified.join(","), // comma-separated for client
      ...(extra?.source ? { source: extra.source } : {}),
    },
    android: { priority: "high", notification: { channelId: "earnings", sound: "default" } },
    apns: { payload: { aps: { sound: "default", category: "EARNINGS" } } },
  };

  const resp = await admin.messaging().sendEachForMulticast(message);
  logger.info(`FCM → uid=${uid} type=planExpired success=${resp.successCount} failure=${resp.failureCount} plans=${newlyNotified.length}`);
  await pruneBadTokens(userRef, tokens, resp.responses);
}


/* ───────────────────────────────────────────────────────────────
   Utils
   ─────────────────────────────────────────────────────────────── */
const chunk = (arr, n) =>
  [...Array(Math.ceil(arr.length / n))].map((_, i) => arr.slice(i * n, i * n + n));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Build a compact "Lv 1–3,5,7–8" label from levels that actually contributed (>0 share)
function compactLevelRanges(levels) {
  if (!levels.length) return "";
  levels.sort((a, b) => a - b);
  const parts = [];
  let start = levels[0], prev = levels[0];
  for (let i = 1; i < levels.length; i++) {
    const cur = levels[i];
    if (cur === prev + 1) {
      prev = cur;
      continue;
    }
    parts.push(start === prev ? `${start}` : `${start}–${prev}`);
    start = prev = cur;
  }
  parts.push(start === prev ? `${start}` : `${start}–${prev}`);
  return parts.join(",");
}

async function runTxn(fn, max = 8) {
  let attempt = 0;
  while (true) {
    attempt++;
    logger.debug(`↻ runTxn attempt #${attempt}`);
    try {
      return await db.runTransaction(fn, { maxAttempts: 1 });
    } catch (e) {
      const retryable = e.code === 10 || e.code === 4; // ABORTED / DEADLINE_EXCEEDED
      logger.warn(
        `⚠️ runTxn failure (attempt=${attempt}) code=${e.code || "?"} message=${e.message}`
      );
      if (!retryable || attempt >= max) {
        logger.error(`⛔ runTxn giving up after ${attempt} attempts`, e);
        throw e;
      }
      const backoff = Math.min(1500, 75 * 2 ** attempt);
      logger.info(`⏳ runTxn backoff ${backoff}ms then retry`);
      await sleep(backoff);
    }
  }
}

/* ───────────────────────────────────────────────────────────────
   Phase A — ROI (per user)
   ─────────────────────────────────────────────────────────────── */
/**
 * Idempotency:
 *   - One roiLog per user/day prevents double-credit for the user on same day.
 *   - Additionally, we create roiPlanLogs per plan/day (for analytics); we DO NOT read them.
 */
/**
 * ROI credit (with FCM notification; core logic unchanged)
 */
async function creditRoiForUser(userDoc) {
  const uid = userDoc.get("uid");
  const status = (userDoc.get("status") || "").toLowerCase();
  const dateKey = ymdPK();

  // ✨ capture result for post-tx FCM
  let _roiResult = { credited: false, amount: 0, txId: null, dateKey };

  logger.info(`▶ [ROI] user=${uid} enter status=${status}`);
  if (!uid) {
    logger.warn(`⛔ [ROI] missing uid on user docId=${userDoc.id}`);
    return;
  }
  if (status !== "active") {
    logger.debug(`⏭ [ROI] skip user=${uid} (not active)`);
    return;
  }

  // Load account (outside tx)
  const acctSnap = await db.collection("accounts").where("userId", "==", uid).limit(1).get();
  if (acctSnap.empty) {
    logger.warn(`⛔ [ROI] user=${uid} account not found`);
    return;
  }
  const acctRef = acctSnap.docs[0].ref;
  logger.debug(`ℹ️ [ROI] user=${uid} accountRef=${acctRef.path}`);

  // Load active plans now (outside tx)
  const plansSnap = await db
    .collection("userPlans")
    .where("userId", "==", uid)
    .where("status", "==", "active")
    .get();

  logger.info(`ℹ️ [ROI] user=${uid} activePlans=${plansSnap.size}`);
  if (plansSnap.empty) {
    logger.debug(`⏭ [ROI] user=${uid} no active plans`);
    return;
  }

  // Per-user daily log (not per-plan): ensures we never double-credit ROI in same PK day.
  const roiLogRef = db.collection("roiLogs").doc(`${uid}_${dateKey}`);

  // Pre-create the transaction doc ref (so we can set it inside tx without generating an ID there)
  const roiTxnRef = db.collection("transactions").doc();

  // Prepare refs to read inside transaction
  const planRefs = plansSnap.docs.map((d) => d.ref);

  const _expiredPlanIds = [];
  await runTxn(async (tx) => {
    // ---- READS FIRST --------------------------------------------------------
    const [acctTxnSnap, roiLogSnap] = await Promise.all([tx.get(acctRef), tx.get(roiLogRef)]);

    if (roiLogSnap.exists) {
      logger.debug(`🔁 [ROI] user=${uid} already credited today (${roiLogRef.id})`);
      return;
    }

    // Read all plan docs within tx
    const planTxnSnaps = [];
    for (const pRef of planRefs) {
      planTxnSnaps.push(await tx.get(pRef));
    }

    // ---- COMPUTE IN MEMORY --------------------------------------------------
    const earnings = acctTxnSnap.get("earnings") || {};
    const invest = acctTxnSnap.get("investment") || {};
    logger.debug(
      `📒 [ROI] user=${uid} pre-account earnings.dailyProfit=${earnings.dailyProfit || 0}, totalEarned=${earnings.totalEarned || 0}, balances={cur:${invest.currentBalance || 0}, rem:${invest.remainingBalance || 0}}`
    );

    let totalRoiToday = 0;
    let anyPlanUpdated = false;
    let processed = 0;
    let credited = 0;
    let expiredNow = 0;

    // We will update all active plans by adding their roiAmount to totalAccumulated and lastCollectedDate.
    // While iterating, compute which plans will expire to decide user deactivation later.
    // Include roiAmount for per-plan log.
    const planUpdates = []; // { ref, roiAmount, newAccum, expire, expireOnly }

    for (const pSnap of planTxnSnaps) {
      processed++;
      const planId = pSnap.id;
      const planStatus = pSnap.get("status");
      const roiAmount = Number(pSnap.get("roiAmount") || 0);
      const currentAccum = Number(pSnap.get("totalAccumulated") || 0);
      const cap = Number(pSnap.get("totalPayoutAmount") || Infinity);

      logger.debug(
        `• [ROI] user=${uid} plan=${planId} status=${planStatus} roiAmount=${roiAmount} totalAccumulated=${currentAccum} cap=${cap}`
      );

      if (planStatus !== "active") {
        logger.debug(`  ⏭ [ROI] user=${uid} plan=${planId} not active at txn time`);
        continue;
      }

      // If already at/over cap, expire WITHOUT crediting more
      if (currentAccum >= cap) {
        planUpdates.push({ ref: pSnap.ref, roiAmount: 0, expire: true, expireOnly: true });
        expiredNow++;
        _expiredPlanIds.push(pSnap.ref.id);
        logger.info(
          `  ⏹ [ROI] user=${uid} plan=${planId} already >= cap (${currentAccum}/${cap}) → EXPIRE (no ROI credit)`
        );
        continue;
      }

      if (roiAmount <= 0) {
        logger.debug(`  ⏭ [ROI] user=${uid} plan=${planId} roiAmount<=0`);
        continue;
      }

      const newAccum = currentAccum + roiAmount;
      const expire = newAccum >= cap;

      planUpdates.push({
        ref: pSnap.ref,
        roiAmount,
        newAccum,
        expire,
        expireOnly: false,
      });
      if (expire) _expiredPlanIds.push(pSnap.ref.id);
      totalRoiToday += roiAmount;
      anyPlanUpdated = true;
      credited++;

      logger.info(
        `  ✅ [ROI] user=${uid} plan=${planId} +${roiAmount} → totalAccumulated=${newAccum}${expire ? " (EXPIRE)" : ""}`
      );
      if (expire) expiredNow++;
    }

    logger.info(
      `Σ [ROI] user=${uid} processed=${processed}, credited=${credited}, expiredNow=${expiredNow}, totalRoiToday=${totalRoiToday}`
    );

    // If nothing to credit, we may still have expireOnly updates (plans hit cap). Handle them.
    if (!anyPlanUpdated || totalRoiToday <= 0) {
      const nowTs = Timestamp.now();

      // Apply expireOnly updates if any
      for (const { ref, expireOnly } of planUpdates) {
        if (expireOnly) {
          tx.update(ref, { status: "expired", lastCollectedDate: nowTs });
        }
      }

      // If *all* active plans just expired via expireOnly, deactivate the user
      const hadActive = planTxnSnaps.some((s) => (s.get("status") || "") === "active");
      if (hadActive) {
        const anyStillActive = planTxnSnaps.some((s) => {
          if ((s.get("status") || "") !== "active") return false;
          const upd = planUpdates.find((u) => u.ref.path === s.ref.path);
          if (!upd) return true; // untouched active plan remains active
          return !upd.expire; // non-expiring update remains active
        });
        if (!anyStillActive) {
          tx.update(userDoc.ref, { status: "inactive" });
          logger.info(`📉 [ROI] user=${uid} deactivated (plans hit cap without ROI credit)`);
        }
      }

      logger.debug(`⏭ [ROI] user=${uid} nothing to credit`);
      return;
    }

    // Determine if any active plan remains after our updates
    let anyActiveAfter = false;
    for (const pSnap of planTxnSnaps) {
      const wasActive = (pSnap.get("status") || "") === "active";
      if (!wasActive) continue;

      const upd = planUpdates.find((u) => u.ref.path === pSnap.ref.path);
      if (!upd) {
        anyActiveAfter = true;
        break;
      }
      if (!upd.expire) {
        anyActiveAfter = true;
        break;
      }
    }

    // ---- WRITES (NO MORE READS) --------------------------------------------
    const nowTs = Timestamp.now();

    // 1) Update account (ROI → earnings only; DO NOT touch investment balances)
    tx.update(acctRef, {
      "earnings.dailyProfit": totalRoiToday,                 // replace (today's ROI total)
      "earnings.lastRoiDate": dateKey,                       // keep last ROI day
      "earnings.totalEarned": FieldValue.increment(totalRoiToday),
      "earnings.totalRoi": FieldValue.increment(totalRoiToday),
      "earnings.totalEarnedToDate": FieldValue.increment(totalRoiToday), // NEW field
    });

    // 2) Update each plan and create per-plan daily logs for credited plans
    for (const { ref, roiAmount, newAccum, expire, expireOnly } of planUpdates) {
      if (expireOnly) {
        tx.update(ref, {
          status: "expired",
          lastCollectedDate: nowTs,
        });
      } else {
        tx.update(ref, {
          totalAccumulated: newAccum,
          lastCollectedDate: nowTs,
          ...(expire ? { status: "expired" } : null),
        });

        // Per-plan per-day log (idempotent by deterministic ID; we don't read it)
        const planId = ref.id;
        const roiPlanLogRef = db.collection("roiPlanLogs").doc(`${uid}_${planId}_${dateKey}`);
        tx.set(roiPlanLogRef, {
          userId: uid,
          planId,
          date: dateKey,
          amount: roiAmount,
          creditedAt: nowTs,
          expiredNow: !!expire,
          type: "roi",
        });
      }
    }

    // 3) Single transaction record for ROI (all plans)
    tx.set(roiTxnRef, {
      transactionId: roiTxnRef.id,
      userId: uid,
      type: "dailyRoi",
      amount: totalRoiToday,
      address: "Daily ROI (all active plans)",
      status: "collected",
      balanceUpdated: true,
      timestamp: nowTs,
    });
    logger.info(
      `💸 [ROI] user=${uid} account updated; transactionId=${roiTxnRef.id} amount=${totalRoiToday}`
    );

    // 4) Idempotency log (per user/day)
    tx.set(roiLogRef, { userId: uid, date: dateKey, creditedAt: nowTs });

    // 5) Possibly deactivate user now if no active plans remain
    if (!anyActiveAfter) {
      tx.update(userDoc.ref, { status: "inactive" });
      logger.info(`📉 [ROI] user=${uid} deactivated (no active plans remain)`);
    }

    // ✨ set result for FCM (inside the same successful commit path)
    _roiResult.credited = true;
    _roiResult.amount = totalRoiToday;
    _roiResult.txId = roiTxnRef.id;
  });

  // ✨ After txn committed: send FCM only if we actually credited (>0)
  if (_roiResult.credited && _roiResult.amount > 0) {
    try {
      await sendEarningsNotification(uid, "dailyRoi", _roiResult.amount, {
        dateKey,
        txId: _roiResult.txId,
        address: "Daily ROI (all active plans)",
      });
    } catch (e) {
      logger.warn(`🔔 [ROI] FCM send failed uid=${uid} err=${e.message}`);
    }
  }
try {
    if (_expiredPlanIds.length) {
      // de-dup just in case
      await notifyPlanExpiries(uid, Array.from(new Set(_expiredPlanIds)), { source: "ROI" });
    }
  } catch (e) {
    logger.warn(`plan-expiry notify (ROI) failed uid=${uid} err=${e.message}`);
 }
  logger.info(`◀ [ROI] user=${uid} done`);
}

// ───────────────────────────────────────────────────────────────
// Phase B — Team Profit (MXG, L1-controlled with $50 qualifier)
// L1 always open. For L>=2, unlock if count(qualifying L1 directs) >= requiredMembers.
// Qualifying L1 direct: status=='active' AND isBlocked!=true AND
//                       has ANY ACTIVE plan with principal >= MIN_INVEST_USD (single plan).
// Profit base per depth = today's ROI only (lastRoiDate===today).
// Accounting: earnings.* only (incl. totalEarnedToDate); do NOT touch investment.*
// Ignore levels beyond L5.
// ───────────────────────────────────────────────────────────────
const MIN_INVEST_USD = 50;

async function creditTeamForUser(userDoc) {
  const uid = userDoc.get("uid");
  const status = (userDoc.get("status") || "").toLowerCase();
  const isBlocked = userDoc.get("isBlocked") === true;
  const dateKey = ymdPK();

  // ✨ capture result for post-tx FCM (no core logic changes)
  let _teamResult = { credited: false, amount: 0, txId: null, dateKey };
  let _txAddressForNotif = null;

  logger.info(`▶ [TEAM] (L1+$50 gate) user=${uid} status=${status}`);
  if (!uid) return;
  if (status !== "active" || isBlocked) {
    logger.debug(`⏭ [TEAM] user=${uid} not eligible (inactive/blocked)`);
    return;
  }

  // Idempotency (outside tx)
  const teamLogRef = db.collection("teamLogs").doc(`${uid}_${dateKey}`);
  if ((await teamLogRef.get()).exists) {
    logger.debug(`🔁 [TEAM] user=${uid} already credited today (${teamLogRef.id})`);
    return;
  }

  // Load team settings (ordered, capped to 5 levels)
  const settingsSnap = await db.collection("teamSettings").orderBy("level").get();
  let settings = settingsSnap.docs.map((d) => d.data());
  settings = settings
    .map((s) => ({
      level: Number(s.level),
      profitPercentage: Number(s.profitPercentage || 0),
      requiredMembers:
        s.requiredMembers !== undefined && s.requiredMembers !== null
          ? Number(s.requiredMembers)
          : Math.max(0, Number(s.level) - 1), // default L2:1, L3:2, ...
    }))
    .filter((s) => s.level >= 1 && s.level <= 5)
    .sort((a, b) => a.level - b.level);

  if (!settings.length) {
    logger.warn(`⛔ [TEAM] user=${uid} teamSettings empty`);
    return;
  }

  // BFS frontier always contains ACTIVE & not-blocked user ids of the previous depth.
  let frontier = [uid];

  // L1 stats
  let l1ActiveUids = [];
  let level1QualifyingCount = 0;

  let totalTeamProfit = 0;
  const levelSummaries = [];

  for (const cfg of settings) {
    const level = cfg.level;
    const pct = cfg.profitPercentage;
    const req = cfg.requiredMembers;

    logger.debug(`• [TEAM] L${level} frontier=${frontier.length} req=${req} pct=${pct}`);

    // 1) Gather ACTIVE & not-blocked children at this depth
    const childDocs = [];
    for (const parentChunk of chunk(frontier, 10)) {
      if (!parentChunk.length) continue;
      const q = await db
        .collection("users")
        .where("referralCode", "in", parentChunk)
        .where("status", "==", "active")
        .get();
      q.docs.forEach((d) => {
        if (d.get("isBlocked") === true) return; // exclude blocked
        childDocs.push(d);
      });
    }
    const childUids = childDocs.map((d) => d.get("uid")).filter(Boolean);

    // 2) On L1, compute qualifying directs (single plan >= $50, ACTIVE)
    if (level === 1) {
      l1ActiveUids = childUids.slice();

      if (l1ActiveUids.length) {
        let qualifying = 0;

        for (const uidChunk of chunk(l1ActiveUids, 10)) {
          const plansSnap = await db
            .collection("userPlans")
            .where("userId", "in", uidChunk)
            .where("status", "==", "active")
            .get();

          // Mark users that have at least one ACTIVE plan with principal >= $50
          const qualified = new Set();
          plansSnap.forEach((p) => {
            const u = String(p.get("userId") || "");
            if (!u || qualified.has(u)) return;
            const principal = Number(p.get("principal") || 0);
            if (principal >= MIN_INVEST_USD) qualified.add(u);
          });

          qualifying += uidChunk.filter((u) => qualified.has(u)).length;
        }

        level1QualifyingCount = qualifying;
      }

      logger.info(
        `📌 [TEAM] user=${uid} L1 active=${l1ActiveUids.length}, ` +
          `qualifying(≥$${MIN_INVEST_USD})=${level1QualifyingCount}`
      );
    }

    // 3) Profit base at this depth from today's ROI only
    let levelDailyProfit = 0;
    if (childUids.length) {
      for (const uidChunk of chunk(childUids, 10)) {
        const accSnap = await db.collection("accounts").where("userId", "in", uidChunk).get();
        accSnap.forEach((a) => {
          const earn = a.get("earnings") || {};
          const d = Number(earn.dailyProfit || 0);
          const last = (earn.lastRoiDate || "").toString();
          if (last === dateKey) levelDailyProfit += d;
        });
      }
    }

    // 4) Unlock decision
    const unlocked = level === 1 ? true : level1QualifyingCount >= req;

    // 5) Share and accumulate
    const share = unlocked ? (levelDailyProfit * pct) / 100 : 0;
    totalTeamProfit += share;

    levelSummaries.push({
      level,
      depthActive: childUids.length,
      levelDailyProfit,
      pct,
      share,
      unlockedBy: "L1QualifyingDirects(≥ $" + MIN_INVEST_USD + ")",
      l1Active: l1ActiveUids.length,
      l1Qualifying: level1QualifyingCount,
      req,
    });

    logger.info(
      `  Σ [TEAM] user=${uid} L${level} active=${childUids.length} ` +
        `base=${levelDailyProfit} unlocked(${level1QualifyingCount} ≥ ${req})=${unlocked} share=${share}`
    );

    // Advance to next depth
    frontier = childUids;
  }

  logger.info(
    `Σ [TEAM] user=${uid} totalTeamProfit=${totalTeamProfit}; detail=${JSON.stringify(
      levelSummaries
    )}`
  );

  // If nothing to credit today, exit without creating a transaction (per your rule)
  if (totalTeamProfit <= 0) {
    logger.debug(`⏭ [TEAM] user=${uid} nothing to credit`);
    return;
  }

  // Load this user's account + active plans (outside tx)
  const acctSnap = await db.collection("accounts").where("userId", "==", uid).limit(1).get();
  if (acctSnap.empty) {
    logger.warn(`⛔ [TEAM] user=${uid} account not found`);
    return;
  }
  const acctRef = acctSnap.docs[0].ref;

  const activePlansSnap = await db
    .collection("userPlans")
    .where("userId", "==", uid)
    .where("status", "==", "active")
    .get();

  const teamTxnRef = db.collection("transactions").doc();
  const planRefs = activePlansSnap.docs.map((d) => d.ref);

  const _expiredPlanIds = [];
  await runTxn(async (tx) => {
    // READS
    const [acctTxnSnap, teamLogSnap] = await Promise.all([tx.get(acctRef), tx.get(teamLogRef)]);
    if (teamLogSnap.exists) {
      logger.debug(`🔁 [TEAM] user=${uid} log appeared during txn; skip credit`);
      return;
    }

    const planTxnSnaps = [];
    for (const pRef of planRefs) planTxnSnaps.push(await tx.get(pRef));

    // Build plan updates for THIS user's active plans
    const planUpdates = []; // { ref, newAccum, expire, expireOnly }
    let bumped = 0;
    let expiredNow = 0;

    for (const pSnap of planTxnSnaps) {
      if ((pSnap.get("status") || "") !== "active") continue;

      const currentAccum = Number(pSnap.get("totalAccumulated") || 0);
      const cap = Number(pSnap.get("totalPayoutAmount") || Infinity);

      if (currentAccum >= cap) {
        planUpdates.push({ ref: pSnap.ref, expire: true, expireOnly: true });
        expiredNow++;
        _expiredPlanIds.push(pSnap.ref.id);
        continue;
      }

      const newAccum = currentAccum + totalTeamProfit;
      const expire = newAccum >= cap;

      planUpdates.push({ ref: pSnap.ref, newAccum, expire, expireOnly: false });
      bumped++;
     if (expire) { expiredNow++; _expiredPlanIds.push(pSnap.ref.id); } 
    }

    // Any plan remains active after updates?
    let anyActiveAfter = false;
    for (const pSnap of planTxnSnaps) {
      if ((pSnap.get("status") || "") !== "active") continue;
      const upd = planUpdates.find((u) => u.ref.path === pSnap.ref.path);
      if (!upd || !upd.expire) {
        anyActiveAfter = true;
        break;
      }
    }

    // WRITES
    const nowTs = Timestamp.now();

    // Earnings only (do not touch dailyProfit or investments)
    tx.update(acctRef, {
      "earnings.teamProfit": FieldValue.increment(totalTeamProfit),
      "earnings.totalEarned": FieldValue.increment(totalTeamProfit),
      "earnings.totalEarnedToDate": FieldValue.increment(totalTeamProfit),
      "earnings.teamDaily": totalTeamProfit,
      "earnings.lastTeamDate": dateKey,
    });

    for (const { ref, newAccum, expire, expireOnly } of planUpdates) {
      if (expireOnly) {
        tx.update(ref, { status: "expired", lastCollectedDate: nowTs });
      } else {
        tx.update(ref, {
          totalAccumulated: newAccum,
          lastCollectedDate: nowTs,
          ...(expire ? { status: "expired" } : null),
        });
      }
    }

    // Only create a transaction when >0 total (we're in that branch)
    const contributingLevels = levelSummaries
      .filter((l) => (l.share || 0) > 0)
      .map((l) => l.level);
    const levelLabel = compactLevelRanges(contributingLevels);
    const txAddress = levelLabel ? `Lv ${levelLabel}` : "Lv —";

    tx.set(teamTxnRef, {
      transactionId: teamTxnRef.id,
      userId: uid,
      type: "teamProfit",
      amount: totalTeamProfit,
      address: txAddress,
      status: "collected",
      balanceUpdated: true,
      timestamp: nowTs,
      meta: {
        unlockRule: `L1 qualifying directs (active & not blocked & ≥$${MIN_INVEST_USD}); L1 always open`,
        l1Active: l1ActiveUids.length,
        l1Qualifying: level1QualifyingCount,
        requiredMembersByLevel: settings.map((s) => ({
          level: s.level,
          req: s.requiredMembers,
        })),
        contributingLevels,
        detail: levelSummaries,
      },
    });

    // Idempotency stamp
    tx.set(teamLogRef, { userId: uid, date: dateKey, creditedAt: nowTs });

    // Possibly deactivate user if no active plans remain
    if (!anyActiveAfter) tx.update(userDoc.ref, { status: "inactive" });

    // ✨ capture results for FCM (no core logic changes)
    _teamResult.credited = true;
    _teamResult.amount = totalTeamProfit;
    _teamResult.txId = teamTxnRef.id;
    _txAddressForNotif = txAddress;

    logger.debug(`  ✔ [TEAM] user=${uid} plansBumped=${bumped}, expiredNow=${expiredNow}`);
  });

  // ✨ After txn committed: send FCM only if we actually credited (>0)
  if (_teamResult.credited && _teamResult.amount > 0) {
    try {
      await sendEarningsNotification(uid, "teamProfit", _teamResult.amount, {
        dateKey,
        txId: _teamResult.txId,
        address: _txAddressForNotif || "Lv —",
      });
    } catch (e) {
      logger.warn(`🔔 [TEAM] FCM send failed uid=${uid} err=${e.message}`);
    }
  }
 // ✨ After txn: notify plan expiries (idempotent per plan+day)
  try {
    if (_expiredPlanIds.length) {
      const unique = Array.from(new Set(_expiredPlanIds));
      await notifyPlanExpiries(uid, unique, { source: "TEAM" });
    }
  } catch (e) {
   logger.warn(`plan-expiry notify (TEAM) failed uid=${uid} err=${e.message}`);
  }
  logger.info(`◀ [TEAM] user=${uid} done`);
}



/* ───────────────────────────────────────────────────────────────
   Orchestration
   ─────────────────────────────────────────────────────────────── */
async function runPhase(name, perUser, concurrency = 40) {
  logger.info(`???? Phase START: ${name} (concurrency=${concurrency}) ????`);
  const limit = pLimit(concurrency);

  const PAGE = 500;
  let last = null;
  let page = 0;
  let usersTotal = 0;

  while (true) {
    let q = db
      .collection("users")
      .where("status", "in", ["active"]) // exclude inactive/blocked implicitly
      .orderBy("__name__")
      .limit(PAGE);

    if (last) q = q.startAfter(last);
    const snap = await q.get();

    if (snap.empty) {
      logger.info(`? Phase ${name}: no more users (page=${page})`);
      break;
    }

    page += 1;
    usersTotal += snap.size;
    logger.info(`? Phase ${name}: page=${page} batchSize=${snap.size}`);

    await Promise.all(
      snap.docs.map((u, idx) =>
        limit(async () => {
          const start = Date.now();
          logger.debug(`  ▶ ${name} user#${idx + 1}/${snap.size} docId=${u.id} uid=${u.get("uid")}`);
          try {
            await perUser(u);
          } catch (e) {
            logger.error(`  ❌ ${name} uid=${u.get("uid")} docId=${u.id} error=${e.message}`, e);
          } finally {
            const ms = Date.now() - start;
            logger.debug(`  ◀ ${name} uid=${u.get("uid")} elapsedMs=${ms}`);
          }
        })
      )
    );

    last = snap.docs[snap.docs.length - 1];
  }

  logger.info(`???? Phase DONE: ${name} usersSeen=${usersTotal} ????`);
}

exports.nightlyRoiAndTeam = onSchedule(
  {
    schedule: "0 0 * * *", // 00:00 PKT daily
    timeZone: "Asia/Karachi",
    timeoutSeconds: 540,
    memory: "1GiB",
    retryConfig: { retryCount: 3, minBackoffSeconds: 120 },
  },
  async () => {
    const jobStart = new Date();
    logger.info(
      `???????? Nightly job START at ${jobStart.toISOString()} (PK-date=${ymdPK(jobStart)}) ????????`
    );

    try {
      await runPhase("ROI", creditRoiForUser, 50); // Phase A
    } catch (e) {
      logger.error(`❌ Phase ROI crashed: ${e.message}`, e);
    }

    try {
      await runPhase("Team Profit", creditTeamForUser, 30); // Phase B
    } catch (e) {
      logger.error(`❌ Phase Team Profit crashed: ${e.message}`, e);
    }

    const jobEnd = new Date();
    logger.info(
      `???????? Nightly job DONE at ${jobEnd.toISOString()} elapsedMs=${jobEnd - jobStart} ?????????`
    );
  }
);