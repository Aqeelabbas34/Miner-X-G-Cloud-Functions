// functions/ranks.js
"use strict";

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");


if (admin.apps.length === 0) admin.initializeApp();

const { getMessaging } = require("firebase-admin/messaging");
const messaging = getMessaging();
const { getFirestore, FieldValue } = require("firebase-admin/firestore");

const db = getFirestore();

/** Ranks in strict order (sequential-claiming enforced) */
const RANKS = [
  { id: "SILVER",   title: "Silver",   direct:   500,   indirect:    3000,   reward:    100 },
  { id: "GOLD",     title: "Gold",     direct:  2000,   indirect:    8000,   reward:    250 },
  { id: "PLATINUM", title: "Platinum", direct:  5000,   indirect:   15000,   reward:    600 },
  { id: "DIAMOND",  title: "Diamond",  direct: 10000,   indirect:   30000,   reward:   1500 },
  { id: "MASTER",   title: "Master",   direct: 25000,   indirect:  100000,   reward:   5000 },
  { id: "GRANDAM",  title: "Grandam",  direct: 60000,   indirect:  200000,   reward:  10000 },
  { id: "ELITE",    title: "Elite",    direct:150000,   indirect:  500000,   reward:  30000 },
  { id: "LEGEND",   title: "Legend",   direct:500000,   indirect: 2000000,   reward: 150000 },
];

const MIN_SELF_INVEST_USD = 10; // user must have ≥ $10 invested in at least one ACTIVE self plan

function chunk10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}


/** Read device tokens from a user doc (supports `deviceToken` or `fcmToken`; string or array). */
function extractTokensFromUserSnap(userSnap) {
  const raw1 = userSnap.get("deviceToken");
  const raw2 = userSnap.get("fcmToken");
  const toArr = (v) =>
    Array.isArray(v) ? v
    : (typeof v === "string" && v.trim() ? [v.trim()] : []);
  // de-dup + drop empties
  return [...new Set([...toArr(raw1), ...toArr(raw2)].filter(Boolean))];
}

/** Resolve a user doc for a given uid (docId == uid, otherwise where uid == ...) */
async function resolveUserDoc(uid) {
  const byId = await db.collection("users").doc(uid).get();
  if (byId.exists) return byId;
  const q = await db.collection("users").where("uid", "==", uid).limit(1).get();
  if (!q.empty) return q.docs[0];
  throw new HttpsError("not-found", "User not found.");
}

/** Get all device tokens for a uid. */
async function getDeviceTokensForUid(uid) {
  const snap = await resolveUserDoc(uid);
  return extractTokensFromUserSnap(snap);
}

/**
 * Send an FCM to all devices for a user.
 * Usage:
 *    await sendFcmToUid(uid, {
 *      notification: { title: "Hi", body: "Message" },
 *      data: { type: "rank-claim", rankId: "GOLD" } // strings only
 *    });
 */
async function sendFcmToUid(uid, payload) {
  const tokens = await getDeviceTokensForUid(uid);
  if (!tokens.length) {
    logger.warn(`[FCM] No tokens for uid=${uid}`);
    return { successCount: 0, failureCount: 0 };
  }

  const res = await messaging.sendEachForMulticast({
    tokens,
    notification: payload.notification ?? undefined,
    data: payload.data ?? undefined,
    android: payload.android ?? undefined,
    apns: payload.apns ?? undefined,
  });

  logger.info(`[FCM] uid=${uid} tokens=${tokens.length} success=${res.successCount} failure=${res.failureCount}`);

  // Optional: log invalid tokens (you can also delete them here if you store arrays)
  res.responses.forEach((r, i) => {
    if (!r.success) logger.warn(`[FCM] token[${i}] failed: ${r.error?.code} ${r.error?.message}`);
  });

  return res;
}
/* ───────────────────────────────────────────────────────────────
   Helpers
   ─ Counts should ignore user/account status (as requested)
   ─ Your schema: child.users.referralCode == parent UID
   ─ Accounts: random docId, field userId == uid
   ─ Business source: investment.totalInvestedInPlans (with safe fallbacks)
─────────────────────────────────────────────────────────────── */

function readLifetimeInvested(accSnap) {
  const inv = accSnap.get("investment") || {};
  const v = Number(
    inv.totalInvestedInPlans ??
    inv.totalDeposit ??
    inv.totalInvestment ??
    0
  );
  return Number.isFinite(v) ? v : 0;
}

async function sumDepositsForUids(uids) {
  let total = 0;

  for (const uChunk of chunk10(uids)) {
    if (!uChunk.length) continue;

    // Primary: accounts with random docId but userId == uid
    const q = await db.collection("accounts")
      .where("userId", "in", uChunk)
      .get();

    const seen = new Set();
    q.forEach(d => {
      total += readLifetimeInvested(d);
      seen.add(d.get("userId") || d.id);
    });

    // Fallback: accounts/{uid} exists (future-proof)
    const missing = uChunk.filter(uid => !seen.has(uid));
    if (missing.length) {
      const refs = missing.map(uid => db.collection("accounts").doc(uid));
      const snaps = await db.getAll(...refs);
      for (const s of snaps) if (s.exists) total += readLifetimeInvested(s);
    }
  }

  return total;
}

/**
 * Fetch immediate children for a frontier of parent UIDs.
 * Your schema: users.referralCode stores the **parent UID**.
 * We also try `referredBy` for extra robustness if present.
 */
async function findChildrenUids(frontierUids) {
  const collected = new Map();

  for (const parentChunk of chunk10(frontierUids)) {
    if (!parentChunk.length) continue;

    // Main (your schema): child.referralCode == parentUid
    const byReferralCode = await db.collection("users")
      .where("referralCode", "in", parentChunk)
      .get();
    byReferralCode.docs.forEach(d => collected.set(d.id, d));

    // Extra tolerant (if some docs used 'referredBy')
    const byReferredBy = await db.collection("users")
      .where("referredBy", "in", parentChunk)
      .get();
    byReferredBy.docs.forEach(d => collected.set(d.id, d));
  }

  // Prefer explicit 'uid' field; fallback to doc id
  const uids = Array.from(collected.values())
    .map(d => d.get("uid") || d.id)
    .filter(Boolean);

  return uids;
}

/**
 * Computes direct/indirect business for a root user across depth<=maxDepth.
 * - No status filter (active/inactive both)
 * - Depth 1 → direct; Depth >=2 → indirect
 */
async function computeBusinessAll(rootUid, maxDepth = 15) {
  const MAX = Math.min(Number(maxDepth ?? 15), 15);
  let frontier = [rootUid];

  let directBusiness = 0;
  let indirectBusiness = 0;

  for (let depth = 1; depth <= MAX; depth++) {
    const childUids = await findChildrenUids(frontier);
    if (!childUids.length) break;

    const deposit = await sumDepositsForUids(childUids);
    if (depth === 1) directBusiness += deposit;
    else indirectBusiness += deposit;

    frontier = childUids;
  }

  return { directBusiness, indirectBusiness };
}

/* Small helpers for claim path */
function readPlanInvested(pSnap) {
  return Number(
    pSnap.get("investedAmount") ??
    pSnap.get("amount") ??
    pSnap.get("principal") ??
    pSnap.get("buyAmount") ??
    0
  ) || 0;
}

// async function resolveUserDoc(uid) {
//   // Try docId == uid first
//   const dirRef = db.collection("users").doc(uid);
//   const dirSnap = await dirRef.get();
//   if (dirSnap.exists) return dirSnap;

//   // Fallback: where uid == ...
//   const q = await db.collection("users").where("uid", "==", uid).limit(1).get();
//   if (!q.empty) return q.docs[0];

//   throw new HttpsError("not-found", "User not found.");
// }

/* ───────────────────────────────────────────────────────────────
   Public callable (read-only)
─────────────────────────────────────────────────────────────── */
exports.computeRanksBusiness = onCall(
  { region: "us-central1", timeoutSeconds: 60, memory: "1GiB" },
  async (req) => {
    const rootUid = req.data && req.data.userId;
    if (!rootUid) throw new HttpsError("invalid-argument", "userId is required");
    const maxDepth = req.data?.maxDepth ?? 15;

    const { directBusiness, indirectBusiness } = await computeBusinessAll(rootUid, maxDepth);
    logger.info(
      `[Ranks] user=${rootUid} direct=${directBusiness} indirect=${indirectBusiness}`
    );
    return { directBusiness, indirectBusiness };
  }
);

/* ───────────────────────────────────────────────────────────────
   Resolve account ref for award (supports both schemas)
─────────────────────────────────────────────────────────────── */
async function resolveAccountRef(uid) {
  // 1) Try docId == uid (future-proof / partial migrations)
  const directRef = db.collection("accounts").doc(uid);
  const directSnap = await directRef.get();
  if (directSnap.exists) return directRef;

  // 2) Current schema: random docId + userId == uid
  const q = await db.collection("accounts")
    .where("userId", "==", uid)
    .limit(1)
    .get();
  if (!q.empty) return q.docs[0].ref;

  throw new HttpsError("not-found", "Account not found for user.");
}

/* ───────────────────────────────────────────────────────────────
   Public callable (server-validated claim)
   - Recomputes business on server
   - Enforces sequential claiming
   - Idempotent (txn checks existing claim)
   - Requires: user.status == 'active' AND ≥1 active plan with invested ≥ $50
   - Credits:
       * Account: teamProfit, totalEarned, totalEarnedToDate, dailyProfit += reward
       * Plans: add FULL reward to EACH active plan; clamp to cap; DO NOT expire
─────────────────────────────────────────────────────────────── */
exports.claimRank = onCall(
  { region: "us-central1", timeoutSeconds: 60, memory: "1GiB" },
  async (req) => {
    const uid = req.data?.userId;
    const rankId = String(req.data?.rankId || "");

    if (!uid)    throw new HttpsError("invalid-argument", "userId is required");
    if (!rankId) throw new HttpsError("invalid-argument", "rankId is required");

    const rank = RANKS.find(r => r.id === rankId);
    if (!rank) throw new HttpsError("invalid-argument", "Unknown rankId");

    // Recompute business (server-truth)
    const { directBusiness, indirectBusiness } = await computeBusinessAll(uid, 15);
    logger.info(`[Claim] uid=${uid} rank=${rankId} direct=${directBusiness} indirect=${indirectBusiness}`);

    // Threshold check
    if (directBusiness < rank.direct || indirectBusiness < rank.indirect) {
      throw new HttpsError("failed-precondition", "Requirements not met for this rank.");
    }

    // Enforce sequential: all earlier ranks must be claimed
    const rankIndex = RANKS.findIndex(r => r.id === rankId);
    const lowerRanks = RANKS.slice(0, rankIndex).map(r => r.id);

    const claimRefs = await Promise.all(
      lowerRanks.map(rid => db.collection("rankClaims").doc(`${uid}_${rid}`).get())
    );
    const missing = lowerRanks.filter((rid, i) => !claimRefs[i].exists);
    if (missing.length > 0) {
      throw new HttpsError("failed-precondition", `You must claim previous ranks first: ${missing.join(", ")}`);
    }

    // Resolve user, account, and gather active plans (outside txn)
    const userSnap = await resolveUserDoc(uid);
    const userStatus = String(userSnap.get("status") || "").toLowerCase();
    if (userStatus !== "active") {
      throw new HttpsError("failed-precondition", "User must be active to claim rank.");
    }

    const activePlansSnap = await db.collection("userPlans")
      .where("userId", "==", uid)
      .where("status", "==", "active")
      .get();

    if (activePlansSnap.empty) {
      throw new HttpsError("failed-precondition", "You have no active plans.");
    }

    let hasQualifyingSelfPlan = false;
    activePlansSnap.forEach(p => {
      if (readPlanInvested(p) >= MIN_SELF_INVEST_USD) hasQualifyingSelfPlan = true;
    });
    if (!hasQualifyingSelfPlan) {
      throw new HttpsError("failed-precondition", `At least one active plan with invested ≥ $${MIN_SELF_INVEST_USD} is required.`);
    }

    const claimRef   = db.collection("rankClaims").doc(`${uid}_${rankId}`);
    const accountRef = await resolveAccountRef(uid);
    const txnRef     = db.collection("transactions").doc(); // auto-id
    const planRefs   = activePlansSnap.docs.map(d => d.ref);
    const reward     = Number(rank.reward || 0);
    const now        = admin.firestore.Timestamp.now();

    // Transaction: atomic & idempotent award
    await db.runTransaction(async (tx) => {
      const [claimSnap, accSnap] = await Promise.all([tx.get(claimRef), tx.get(accountRef)]);

      if (claimSnap.exists) {
        throw new HttpsError("already-exists", "Rank already claimed.");
      }
      if (!accSnap.exists) {
        throw new HttpsError("not-found", "Account not found for user.");
      }

      // Re-read user & plans inside txn for consistency
      const userTxnSnap = await tx.get(userSnap.ref);
      const statusNow = String(userTxnSnap.get("status") || "").toLowerCase();
      if (statusNow !== "active") {
        throw new HttpsError("failed-precondition", "User must be active to claim rank.");
      }

      const planTxnSnaps = [];
      for (const pRef of planRefs) planTxnSnaps.push(await tx.get(pRef));

      // Filter ACTIVE plans and re-check qualifying plan ≥ $50
      let hasQualifying = false;
      const activePlanTxnSnaps = [];
      for (const p of planTxnSnaps) {
        if ((p.get("status") || "") === "active") {
          activePlanTxnSnaps.push(p);
          if (readPlanInvested(p) >= MIN_SELF_INVEST_USD) hasQualifying = true;
        }
      }
      if (!activePlanTxnSnaps.length) {
        throw new HttpsError("failed-precondition", "You have no active plans.");
      }
      if (!hasQualifying) {
        throw new HttpsError("failed-precondition", `At least one active plan with invested ≥ $${MIN_SELF_INVEST_USD} is required.`);
      }

      // If reward <= 0: log the claim & zero transaction; skip credits
      if (!(reward > 0)) {
        tx.set(claimRef, {
          userId: uid,
          rankId,
          reward: 0,
          directAtClaim:   directBusiness,
          indirectAtClaim: indirectBusiness,
          createdAt: FieldValue.serverTimestamp(),
          type: "rank-claim",
        });
        tx.set(txnRef, {
          userId: uid,
          amount: 0,
          currency: "USD",
          type: "rank-reward",
          rankId,
          status: "success",
          createdAt: FieldValue.serverTimestamp(),
          meta: { directAtClaim: directBusiness, indirectAtClaim: indirectBusiness },
        });
        return;
      }

      // Account earnings: include today's dailyProfit increment
      tx.update(accountRef, {
        "earnings.teamProfit":        FieldValue.increment(reward),
        "earnings.totalEarned":       FieldValue.increment(reward),
        "earnings.totalEarnedToDate": FieldValue.increment(reward),
        "earnings.dailyProfit":       FieldValue.increment(reward), // include in today's profit
        "updatedAt":                  FieldValue.serverTimestamp(),
      });

      // Plans: add FULL reward to EACH active plan; clamp to cap; DO NOT expire
      for (const p of activePlanTxnSnaps) {
        const curr = Number(p.get("totalAccumulated") || 0);
        const cap  = Number(p.get("totalPayoutAmount") || Infinity);
        const newAccum = Math.min(cap, curr + reward);

        tx.update(p.ref, {
          totalAccumulated: newAccum,
          lastCollectedDate: now,
          // status unchanged (no expiry here by spec)
        });
      }

      // Claim log (facts at claim time)
      tx.set(claimRef, {
        userId: uid,
        rankId,
        reward: reward,
        directAtClaim:   directBusiness,
        indirectAtClaim: indirectBusiness,
        createdAt: FieldValue.serverTimestamp(),
        type: "rank-claim",
      });

      // Transactions ledger
      tx.set(txnRef, {
        userId: uid,
        amount: reward,
        currency: "USD",
        type: "rank-reward",
        rankId,
        status: "success",
        createdAt: FieldValue.serverTimestamp(),
        meta: {
          directAtClaim:   directBusiness,
          indirectAtClaim: indirectBusiness,
          appliedToPlans: activePlanTxnSnaps.map(p => p.id),
          clampPolicy: "min(cap, curr + reward); no-expire",
        },
      });

      // Optional safety: if somehow no active plans remain (shouldn't happen since we don't expire), set user inactive
      if (!activePlanTxnSnaps.length) {
        tx.update(userTxnSnap.ref, { status: "inactive" });
      }
    });

    // Return fresh claimed set for UX
    const claimedSnaps = await db.collection("rankClaims").where("userId", "==", uid).get();
    const claimedIds = claimedSnaps.docs.map(d => d.get("rankId"));

    logger.info(`[Claim OK] uid=${uid} rank=${rankId} (reward=${rank.reward})`);
    // 🔔 Send FCM (non-blocking for UX; wrap in try/catch)
try {
  await sendFcmToUid(uid, {
    notification: {
      title: `🎉 ${rank.title} rank claimed!`,
      body:  `You received $${rank.reward} for ${rank.title}.`
    },
    data: {
      type: "rank-claim",
      rankId,
      reward: String(rank.reward),
      directAtClaim: String(directBusiness),
      indirectAtClaim: String(indirectBusiness),
    }
  });
} catch (e) {
  logger.warn(`[FCM] Failed to notify uid=${uid} after claim`, e);
}
    return { ok: true, claimedIds, directBusiness, indirectBusiness };
  }
);
