/**
 * computeTeamLevelsAndCreditProfit – UI-only (READ-ONLY, current rules)
 * - L1 always unlocked
 * - L>=2 unlocked if (# of QUALIFYING L1 directs) >= teamSettings.requiredMembers
 *   qualifying L1 direct = status=='active' AND isBlocked!=true AND
 *                          has ANY ACTIVE plan with principal >= MIN_INVEST_USD
 * - Daily base uses TODAY's ROI only: earnings.lastRoiDate === dateKey
 * - Business source: investment.totalInvestedInPlans
 * - Returns: level stats + full user list per level (rich fields)
 */
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const { initializeApp, getApps } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");

if (!getApps().length) initializeApp();
const db = getFirestore();

const MIN_INVEST_USD = 50;

// Asia/Karachi yyy-mm-dd (same convention you use elsewhere)
const PK_OFFSET_MS = 5 * 60 * 60 * 1000;
function ymdPK(d = new Date()) {
  const pk = new Date(d.getTime() + PK_OFFSET_MS);
  return pk.toISOString().slice(0, 10);
}

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

    const dateKey = ymdPK();

    // 1) Read teamSettings and normalize (limit to 1..5)
    const settingsSnap = await db.collection("teamSettings").orderBy("level").get();
    let settings = settingsSnap.docs
      .map((d) => d.data())
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
      logger.warn(`computeTeamLevelsAndCreditProfit: teamSettings empty`);
      return {
        levels: [],
        profitBooked: false,
        creditedAmount: 0,
        directBusiness: 0,
        selfDeposit: 0,
        l1QualifyingCount: 0,
        requiredMembersByLevel: [],
        dateKey,
      };
    }

    // 2) BFS traversal (depth by depth)
    let frontier = [rootUid];

    // Will be computed at L1, used for unlocking L2+
    let l1QualifyingCount = 0;
    let l1UserListRaw = []; // keep for directBusiness calc

    const levels = [];

    for (const cfg of settings) {
      const levelNum = Number(cfg.level);
      const pct = Number(cfg.profitPercentage || 0);
      const requiredMembers = Number(cfg.requiredMembers || 0);

      // Users whose referralCode in current frontier (any status)
      const userDocs = [];
      for (const parentChunk of chunk10(frontier)) {
        if (!parentChunk.length) continue;
        const qs = await db
          .collection("users")
          .where("referralCode", "in", parentChunk)
          .get();
        userDocs.push(...qs.docs);
      }

      // Shape basic user info
      const usersBasic = userDocs.map((d) => ({
        uid: d.get("uid"),
        firstName: d.get("firstName") || "",
        lastName: d.get("lastName") || "",
        status: (d.get("status") || "").toLowerCase(),
        isBlocked: d.get("isBlocked") === true,
      }));

      // Active & not blocked for sums / next frontier
      const activeCleanUids = usersBasic
        .filter((u) => u.status === "active" && !u.isBlocked)
        .map((u) => u.uid)
        .filter(Boolean);

      // Collect per-user account aggregates (for ACTIVE & not blocked only)
      // We also need per-user fields to return in "users" for UI:
      //   totalInvestedInPlans, earnings.dailyProfit, earnings.lastRoiDate
      const perUserAcc = new Map(); // uid -> {totalInvestedInPlans, dailyProfit, lastRoiDate}
      if (activeCleanUids.length) {
        for (const uChunk of chunk10(activeCleanUids)) {
          const accSnap = await db.collection("accounts").where("userId", "in", uChunk).get();
          accSnap.forEach((acc) => {
            const uid = String(acc.get("userId") || "");
            const inv = acc.get("investment") || {};
            const earn = acc.get("earnings") || {};
            perUserAcc.set(uid, {
              totalInvestedInPlans: Number(inv.totalInvestedInPlans || 0),
              dailyProfit: Number(earn.dailyProfit || 0),
              lastRoiDate: (earn.lastRoiDate || "").toString(),
            });
          });
        }
      }

      // Compute level aggregates (ACTIVE & not blocked only)
      let levelDeposit = 0;
      let levelDailyProfitToday = 0;

      for (const uid of activeCleanUids) {
        const a = perUserAcc.get(uid);
        if (!a) continue;
        levelDeposit += a.totalInvestedInPlans || 0;
        if ((a.lastRoiDate || "") === dateKey) {
          levelDailyProfitToday += a.dailyProfit || 0;
        }
      }

      // L1 qualifying directs logic (ACTIVE & not blocked, with ≥$50 plan principal)
      // We only compute at L1; used for unlocking L2+
      let l1QualifiesSet = new Set();
      if (levelNum === 1 && activeCleanUids.length) {
        for (const uChunk of chunk10(activeCleanUids)) {
          // Find any ACTIVE plan per user with principal >= MIN_INVEST_USD
          const plansSnap = await db
            .collection("userPlans")
            .where("userId", "in", uChunk)
            .where("status", "==", "active")
            .get();
          const tempQualified = new Set();
          plansSnap.forEach((p) => {
            const u = String(p.get("userId") || "");
            if (!u) return;
            if (tempQualified.has(u)) return;
            const principal = Number(p.get("principal") || 0);
            if (principal >= MIN_INVEST_USD) tempQualified.add(u);
          });
          // merge
          for (const u of tempQualified) l1QualifiesSet.add(u);
        }
        l1QualifyingCount = l1QualifiesSet.size;
        l1UserListRaw = usersBasic.map((u) => u.uid).filter(Boolean);
      }

      // Determine unlock for this depth
      const levelUnlocked =
        levelNum === 1 ? true : l1QualifyingCount >= requiredMembers;

      // Build enriched user list for UI
      // For all users at this depth (including inactive/blocked), attach known account info where available.
      // Additionally, for L1 users, include "qualifies" flag.
      const usersEnriched = usersBasic.map((u) => {
        const acc = perUserAcc.get(u.uid) || {};
        const todayRoi = (acc.lastRoiDate || "") === dateKey ? Number(acc.dailyProfit || 0) : 0;
        return {
          uid: u.uid,
          firstName: u.firstName,
          lastName: u.lastName,
          status: u.status,
          isBlocked: u.isBlocked,
          totalInvestedInPlans: Number(acc.totalInvestedInPlans || 0),
          dailyProfit: Number(acc.dailyProfit || 0),
          lastRoiDate: acc.lastRoiDate || "",
          todayRoi, // ROI counted today only
          ...(levelNum === 1
            ? { qualifies: u.status === "active" && !u.isBlocked && l1QualifiesSet.has(u.uid) }
            : {}),
        };
      });

      levels.push({
        level: levelNum,
        profitPercentage: pct,
        requiredMembers,
        levelUnlocked,
        totalUsers: usersBasic.length,
        activeUsers: activeCleanUids.length,
        inactiveUsers: usersBasic.length - activeCleanUids.length,
        totalDeposit: levelDeposit,
        levelDailyProfitToday,
        users: usersEnriched, // ← full list for this level (as requested)
      });

      // Advance BFS only via ACTIVE & not blocked users
      frontier = activeCleanUids;
    }

    // 3) Extra UI figures
    // Direct business (Level-1) — ALL L1 users (active + inactive), from totalInvestedInPlans
    let directBusiness = 0;
    if (levels.length > 0) {
      const level1UidsAll = levels[0].users.map((u) => u.uid).filter(Boolean);
      for (const c of chunk10(level1UidsAll)) {
        if (!c.length) continue;
        const accSnap = await db.collection("accounts").where("userId", "in", c).get();
        accSnap.forEach((acc) => {
          const inv = acc.get("investment") || {};
          directBusiness += Number(inv.totalInvestedInPlans || 0);
        });
      }
    }

    // Self deposit — from totalInvestedInPlans
    let selfDeposit = 0;
    const selfAccSnap = await db.collection("accounts")
      .where("userId", "==", rootUid)
      .limit(1)
      .get();
    if (!selfAccSnap.empty) {
      const inv = selfAccSnap.docs[0].get("investment") || {};
      selfDeposit = Number(inv.totalInvestedInPlans || 0);
    }

    // Build requiredMembersByLevel for UI
    const requiredMembersByLevel = settings.map((s) => ({
      level: s.level,
      req: s.requiredMembers,
      pct: s.profitPercentage,
    }));

    logger.info(
      `UI-only levels for ${rootUid} built at ${dateKey} (L1 qualifiers=${l1QualifyingCount}).`
    );

    return {
      levels,                    // includes users per level
      profitBooked: false,       // UI-only
      creditedAmount: 0,         // UI-only
      directBusiness,
      selfDeposit,
      l1QualifyingCount,         // for badges/locks
      requiredMembersByLevel,    // show rules
      dateKey,                   // clarify which "today" the ROI check used
    };
  }
);
