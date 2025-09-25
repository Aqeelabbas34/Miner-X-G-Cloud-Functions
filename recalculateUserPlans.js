// functions/index.js (Node.js 18+)
// npm i firebase-functions firebase-admin

"use strict";

const { onCall } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");

if (!admin.apps.length) admin.initializeApp();
const db = admin.firestore();

/** Round to 2 decimals safely */
function round2(n) {
  const x = Number(n || 0);
  return Math.round(x * 100) / 100;
}

/**
 * Callable:
 *   recalculateUserPlansForPlan({ planId, dryRun=false, pageSize=400 })
 *
 * Behavior:
 *   - Reads the plan {dailyPercentage, totalPayout}
 *   - Streams all userPlans where planId == <planId> AND status == "active"
 *   - For each doc:
 *       * newRoiPercent = plan.dailyPercentage
 *       * newRoiAmount  = principal * newRoiPercent / 100
 *       * newTotalPayoutAmount = principal * (plan.totalPayout / 100)
 *       * newPayoutPercent = plan.totalPayout
 *     - Builds a minimal update map containing ONLY fields whose values differ
 *       (roiPercent, roiAmount, totalPayoutAmount, payoutPercent).
 *     - Writes in batches of pageSize (<= 500).
 *   - Does NOT expire plans, does NOT touch principal/totalAccumulated/users/transactions/logs.
 *   - If dryRun=true, computes the updates but does NOT write.
 */
exports.recalculateUserPlansForPlan = onCall(
  { region: "asia-south1", timeoutSeconds: 540, memory: "1GiB" },
  async (req) => {
    const { planId, dryRun = false, pageSize = 400 } = req.data || {};
    if (!planId || typeof planId !== "string") {
      throw new Error("Missing/invalid 'planId'.");
    }
    if (pageSize < 1 || pageSize > 500) {
      throw new Error("pageSize must be between 1 and 500.");
    }

    // 1) Load plan
    const planSnap = await db.collection("plans").doc(planId).get();
    if (!planSnap.exists) {
      throw new Error(`Plan not found: ${planId}`);
    }
    const plan = planSnap.data() || {};
    const planRoiPct = Number(plan.dailyPercentage || 0);
    const planPayoutPct = Number(plan.totalPayout || 0);

    logger.info(
      `[MIGRATE] planId=${planId} dailyPercentage=${planRoiPct} totalPayout=${planPayoutPct} dryRun=${!!dryRun} pageSize=${pageSize}`
    );

    // 2) Page through active userPlans for this planId
    let last = null;
    let totalMatched = 0;
    let totalUpdated = 0;
    let fieldCounts = { roiPercent: 0, roiAmount: 0, totalPayoutAmount: 0, payoutPercent: 0 };
    let expiredNow = 0; // (by design we DO NOT expire; kept for visibility)
    const sampleChanges = [];

    while (true) {
      let q = db
        .collection("userPlans")
        .where("planId", "==", planId)
        .where("status", "==", "active")
        .orderBy("__name__")
        .limit(pageSize);

      if (last) q = q.startAfter(last);
      const snap = await q.get();
      if (snap.empty) break;

      totalMatched += snap.size;

      // Build a batch of writes (or simulate when dryRun=true)
      let batch = dryRun ? null : db.batch();

      for (const doc of snap.docs) {
        const up = doc.data() || {};

        const principal = Number(up.principal || 0);
        const currentRoiPercent = Number(up.roiPercent || 0);
        const currentRoiAmount = Number(up.roiAmount || 0);
        const currentTotalPayoutAmount = Number(up.totalPayoutAmount || 0);
        const currentPayoutPercent = Number(up.payoutPercent || 0);

        // Recompute targets from the plan + principal
        const newRoiPercent = planRoiPct; // mirror plan.dailyPercentage
        const newRoiAmount = round2(principal * newRoiPercent / 100);
        const newPayoutPercent = planPayoutPct; // mirror plan.totalPayout
        const newTotalPayoutAmount = round2(principal * newPayoutPercent / 100);

        // Prepare minimal update map (only changed fields)
        const updates = {};
        const epsilon = 0.0001;
        if (Math.abs(currentRoiPercent - newRoiPercent) > epsilon) {
          updates.roiPercent = newRoiPercent;
        }
        if (Math.abs(currentRoiAmount - newRoiAmount) > epsilon) {
          updates.roiAmount = newRoiAmount;
        }
        if (Math.abs(currentTotalPayoutAmount - newTotalPayoutAmount) > epsilon) {
          updates.totalPayoutAmount = newTotalPayoutAmount;
        }
        if (Math.abs(currentPayoutPercent - newPayoutPercent) > epsilon) {
          updates.payoutPercent = newPayoutPercent;
        }

        // DO NOT expire here (leave to nightly job), per your rule #3.

        const willWrite = Object.keys(updates).length > 0;
        if (willWrite) {
          // Track which fields are changing
          for (const k of Object.keys(updates)) fieldCounts[k] = (fieldCounts[k] || 0) + 1;

          // Keep a tiny sample for response (first few only)
          if (sampleChanges.length < 10) {
            sampleChanges.push({
              userPlanId: doc.id,
              principal,
              before: {
                roiPercent: currentRoiPercent,
                roiAmount: currentRoiAmount,
                totalPayoutAmount: currentTotalPayoutAmount,
                payoutPercent: currentPayoutPercent,
              },
              after: {
                roiPercent: updates.roiPercent ?? currentRoiPercent,
                roiAmount: updates.roiAmount ?? currentRoiAmount,
                totalPayoutAmount: updates.totalPayoutAmount ?? currentTotalPayoutAmount,
                payoutPercent: updates.payoutPercent ?? currentPayoutPercent,
              },
            });
          }

          if (!dryRun) {
            batch.update(doc.ref, updates);
          }
          totalUpdated += 1;
        }
      }

      if (!dryRun) {
        await batch.commit();
      }

      last = snap.docs[snap.docs.length - 1];
    }

    const summary = {
      planId,
      planDailyPercentage: planRoiPct,
      planTotalPayout: planPayoutPct,
      matchedActiveUserPlans: totalMatched,
      updatedDocs: totalUpdated,
      fieldCounts,
      expiredNow, // always 0 by design here
      dryRun: !!dryRun,
      samples: sampleChanges,
    };

    logger.info(`[MIGRATE] DONE`, summary);
    return summary;
  }
);
