/**
 * computeTeamLevelsAndCreditProfit – UI-only (READ-ONLY)
 * - L1 always unlocked
 * - L>=2 unlocked if (# ACTIVE users at that depth) >= teamSettings.requiredMembers
 * - Business source switched to: investment.totalInvestedInPlans
 * - NO writes. Only reads & returns stats for UI.
 */
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const { initializeApp, getApps } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");

if (!getApps().length) initializeApp();
const db = getFirestore();

function chunk10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}

exports.computeTeamLevelsAndCreditProfit = onCall(
  { region: "us-central1", timeoutSeconds: 60, memory: "512MiB" },
  async (req) => {
    const rootUid = req.data && req.data.userId;
    if (!rootUid) throw new HttpsError("invalid-argument", "userId is required");

    // 1) Read teamSettings (numbers please: level, profitPercentage, requiredMembers)
    const settingsSnap = await db.collection("teamSettings").orderBy("level").get();
    const settings = settingsSnap.docs
      .map((d) => d.data())
      .sort((a, b) => Number(a.level) - Number(b.level));

    // 2) Traverse depth-by-depth (BFS) building UI stats
    let frontier = [rootUid];
    const levels = [];

    for (const cfg of settings) {
      const levelNum = Number(cfg.level);
      const pct = Number(cfg.profitPercentage || 0);
      const requiredMembers = levelNum === 1 ? 0 : Number(cfg.requiredMembers ?? 1);

      // Fetch users whose referralCode is in current frontier (any status)
      const userDocs = [];
      for (const parentChunk of chunk10(frontier)) {
        if (parentChunk.length === 0) continue;
        const qs = await db.collection("users")
          .where("referralCode", "in", parentChunk)
          .get();
        userDocs.push(...qs.docs);
      }

      // Build per-user UI info
      const userList = userDocs.map((d) => ({
        uid: d.get("uid"),
        firstName: d.get("firstName") || "",
        lastName: d.get("lastName") || "",
        status: d.get("status") || "",
      }));

      const activeUids = userList.filter((u) => u.status === "active").map((u) => u.uid);

      // Sum business & dailyProfit for THIS depth (ACTIVE users only) – now using totalInvestedInPlans
      let levelDeposit = 0;
      let levelDailyProfit = 0;
      if (activeUids.length) {
        for (const uChunk of chunk10(activeUids)) {
          const accSnap = await db.collection("accounts").where("userId", "in", uChunk).get();
          accSnap.forEach((acc) => {
            const inv = acc.get("investment") || {};
            const earn = acc.get("earnings") || {};
            levelDeposit += Number(inv.totalInvestedInPlans || 0); // ← changed
            levelDailyProfit += Number(earn.dailyProfit || 0);
          });
        }
      }

      // Unlock rule: L1 always true; others need >= requiredMembers ACTIVE users at THIS depth
      const levelUnlocked = levelNum === 1 ? true : activeUids.length >= requiredMembers;

      levels.push({
        level: levelNum,
        requiredMembers,
        profitPercentage: pct,
        totalUsers: userDocs.length,
        activeUsers: activeUids.length,
        inactiveUsers: userDocs.length - activeUids.length,
        totalDeposit: levelDeposit,       // kept same field name for UI, source changed
        levelDailyProfit,
        levelUnlocked,
        users: userList,
      });

      // Go deeper only through ACTIVE users
      frontier = activeUids;
    }

    // 3) Extra figures for UI
    // Direct business (Level-1) – ALL L1 users (active + inactive), now from totalInvestedInPlans
    let directBusiness = 0;
    if (levels.length > 0) {
      const level1Uids = levels[0].users.map((u) => u.uid);
      for (const chunk of chunk10(level1Uids)) {
        if (chunk.length === 0) continue;
        const accSnap = await db.collection("accounts").where("userId", "in", chunk).get();
        accSnap.forEach((acc) => {
          const inv = acc.get("investment") || {};
          directBusiness += Number(inv.totalInvestedInPlans || 0); // ← changed
        });
      }
    }

    // Self deposit – now from totalInvestedInPlans
    let selfDeposit = 0;
    const selfAccSnap = await db.collection("accounts")
      .where("userId", "==", rootUid)
      .limit(1)
      .get();
    if (!selfAccSnap.empty) {
      const inv = selfAccSnap.docs[0].get("investment") || {};
      selfDeposit = Number(inv.totalInvestedInPlans || 0); // ← changed
    }

    logger.info(`UI-only levels for ${rootUid} built (using totalInvestedInPlans).`);

    // READ-ONLY response for UI
    return {
      levels,
      profitBooked: false,
      creditedAmount: 0,
      directBusiness,
      selfDeposit,
    };
  }
);
