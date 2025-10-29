// functions/src/index.js
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("firebase-admin");

admin.initializeApp();

/** Admin allow-list (no custom claims needed) */
const ADMIN_UIDS = new Set(["tfBpF2fFLeOQhoF501ipiVAs9ph2"]);
const ADMIN_EMAILS = new Set(["minerx1868@gmail.com"]);

function assertIsAdmin(auth) {
  if (!auth || !auth.uid) throw new HttpsError("unauthenticated", "Sign-in required.");
  if (ADMIN_UIDS.has(auth.uid)) return;
  const email = auth.token && auth.token.email;
  if (email && ADMIN_EMAILS.has(String(email).toLowerCase())) return;
  throw new HttpsError("permission-denied", "Admin only.");
}

exports.adminPing = onCall({ region: "us-central1" }, async (req) => {
  assertIsAdmin(req.auth);
  return { ok: true, uid: req.auth.uid, email: req.auth.token?.email || null };
});

exports.adminPreviewDeleteUser = onCall(
  { region: "us-central1", timeoutSeconds: 120, memory: "512MiB" },
  async (req) => {
    assertIsAdmin(req.auth);
    try {
      const data = req.data || {};
      const mxgUid = data.mxgUid;
      const authUidArg = data.authUid;
      if (!mxgUid) throw new HttpsError("invalid-argument", "mxgUid is required.");

      const db = admin.firestore();
      const resolvedAuthUid = await resolveAuthUid(db, mxgUid, authUidArg);
      const report = await collectTargets(db, mxgUid, resolvedAuthUid);
      return report;
    } catch (e) {
      console.error("adminPreviewDeleteUser error:", e);
      throw new HttpsError("internal", String(e && e.message || e));
    }
  }
);

exports.adminDeleteUser = onCall(
  { region: "us-central1", timeoutSeconds: 540, memory: "1GiB" },
  async (req) => {
    assertIsAdmin(req.auth);

    const db = admin.firestore();
    const auth = admin.auth();

    try {
      const data = req.data || {};
      const mxgUid = data.mxgUid;
      const authUidArg = data.authUid;
      const options = data.options || {};
      const opts = { dryRun: !!options.dryRun, archive: options.archive === true };

      if (!mxgUid) throw new HttpsError("invalid-argument", "mxgUid is required.");

      const resolvedAuthUid = await resolveAuthUid(db, mxgUid, authUidArg);
      const report = await collectTargets(db, mxgUid, resolvedAuthUid);

      if (opts.dryRun) return report;

      // 1) disable + revoke
      if (resolvedAuthUid) {
        await auth.updateUser(resolvedAuthUid, { disabled: true });
        await auth.revokeRefreshTokens(resolvedAuthUid);
        report.actions.disabledAuth = true;
        report.actions.revokedTokens = true;
      }

      // 2) optional archive
      if (opts.archive) {
        await archivePrimaryDocs(db, report);
        report.actions.archivedDocs = true;
      }

      // 3) deletes (chunked)
      await deleteDocsInChunks(db, "users", report.preview.usersDoc ? [report.preview.usersDoc] : []);
      await deleteDocsInChunks(db, "accounts", report.preview.accounts);
      await deleteDocsInChunks(db, "userPlans", report.preview.userPlans);
      await deleteDocsInChunks(db, "transactions", report.preview.transactions);
      await deleteDocsInChunks(db, "deposits", report.preview.deposits);
      await deleteDocsInChunks(db, "withdrawals", report.preview.withdrawals);
      await deleteByAbsolutePathsInChunks(db, report.preview.monthlyVolumesUserPaths);

      report.actions.deletedCounts = {
        users: report.preview.usersDoc ? 1 : 0,
        accounts: report.preview.accounts.length,
        userPlans: report.preview.userPlans.length,
        transactions: report.preview.transactions.length,
        deposits: report.preview.deposits.length,
        withdrawals: report.preview.withdrawals.length,
        monthlyVolumesUsers: report.preview.monthlyVolumesUserPaths.length,
      };

      // 4) delete auth user last
      if (resolvedAuthUid) {
        await auth.deleteUser(resolvedAuthUid);
      }

      return report;
    } catch (e) {
      console.error("adminDeleteUser error:", e);
      throw new HttpsError("internal", String(e && e.message || e));
    }
  }
);

/* ---------------- helpers ---------------- */
async function resolveAuthUid(db, mxgUid, authUid) {
  if (authUid) return authUid;
  try {
    const q = await db.collection("users").where("uid", "==", mxgUid).limit(1).get();
    return (q.docs[0] && q.docs[0].id) || null;
  } catch (e) {
    console.error("resolveAuthUid failed:", e);
    throw new Error("Failed to resolve authUid from users collection");
  }
}

async function collectTargets(db, mxgUid, authUid) {
  const preview = {
    usersDoc: null,
    accounts: [],
    userPlans: [],
    transactions: [],
    deposits: [],
    withdrawals: [],
    monthlyVolumesUserPaths: [],
  };

  if (authUid) {
    try {
      const usersDocRef = db.collection("users").doc(authUid);
      const snap = await usersDocRef.get();
      if (snap.exists) preview.usersDoc = usersDocRef.id;
    } catch (e) {
      console.error("Reading users doc failed:", e);
    }
  }

  const collectIds = async (qBase, label) => {
    try {
      const ids = [];
      let last = null;
      for (;;) {
        const q = last ? qBase.startAfter(last).limit(500) : qBase.limit(500);
        const page = await q.get();
        if (page.empty) break;
        page.docs.forEach(d => ids.push(d.id));
        last = page.docs[page.docs.length - 1];
        if (page.size < 500) break;
      }
      return ids;
    } catch (e) {
      console.error(`collectIds failed for ${label}:`, e);
      throw new Error(`Failed to read ${label} (check collection name and indexes)`);
    }
  };

  preview.accounts      = await collectIds(db.collection("accounts").where("userId", "==", mxgUid), "accounts");
  preview.userPlans     = await collectIds(db.collection("userPlans").where("userId", "==", mxgUid), "userPlans");
  preview.transactions  = await collectIds(db.collection("transactions").where("userId", "==", mxgUid), "transactions");
  preview.deposits      = await collectIds(db.collection("deposits").where("userId", "==", mxgUid), "deposits");
  preview.withdrawals   = await collectIds(db.collection("withdrawals").where("userId", "==", mxgUid), "withdrawals");

  try {
    const mv = await db.collectionGroup("users")
      .where(admin.firestore.FieldPath.documentId(), "==", mxgUid)
      .get();
    preview.monthlyVolumesUserPaths = mv.docs.map(d => d.ref.path);
  } catch (e) {
    console.error("collectionGroup(users) failed:", e);
    preview.monthlyVolumesUserPaths = [];
  }

  return {
    resolved: { mxgUid, authUid },
    preview,
    actions: {}
  };
}

async function archivePrimaryDocs(db, report) {
  const { resolved, preview } = report;

  // users/{authUid}  -> _archive_users/{authUid}
  if (preview.usersDoc) {
    const ref = db.collection("users").doc(preview.usersDoc);
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data() || {};
      const sizeApprox = Buffer.byteLength(JSON.stringify(data));
      const archiveUsers = db.collection("_archive_users");
      await archiveUsers.doc(preview.usersDoc).set(
        sizeApprox > 950000
          ? { _path: ref.path, _ts: admin.firestore.FieldValue.serverTimestamp(), _mxgUid: resolved.mxgUid, partial: true, keys: Object.keys(data) }
          : { _path: ref.path, _ts: admin.firestore.FieldValue.serverTimestamp(), _mxgUid: resolved.mxgUid, data },
        { merge: true }
      );
    }
  }

  // accounts, userPlans, transactions, deposits, withdrawals
  await archiveCollectionDocs(db, "_archive_accounts",      "accounts",      preview.accounts,      resolved.mxgUid);
  await archiveCollectionDocs(db, "_archive_userPlans",     "userPlans",     preview.userPlans,     resolved.mxgUid);
  await archiveCollectionDocs(db, "_archive_transactions",  "transactions",  preview.transactions,  resolved.mxgUid);
  await archiveCollectionDocs(db, "_archive_deposits",      "deposits",      preview.deposits,      resolved.mxgUid);
  await archiveCollectionDocs(db, "_archive_withdrawals",   "withdrawals",   preview.withdrawals,   resolved.mxgUid);

  // monthlyVolumes/*/users/{mxgUid} -> _archive_monthlyVolumesUsers/{sanitizedPath}
  const mvArchive = db.collection("_archive_monthlyVolumesUsers");
  for (const p of preview.monthlyVolumesUserPaths) {
    const ref = db.doc(p);
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data() || {};
      const sizeApprox = Buffer.byteLength(JSON.stringify(data));
      const docId = p.replace(/\//g, "__"); // sanitize
      await mvArchive.doc(docId).set(
        sizeApprox > 950000
          ? { _path: ref.path, _ts: admin.firestore.FieldValue.serverTimestamp(), _mxgUid: resolved.mxgUid, partial: true, keys: Object.keys(data) }
          : { _path: ref.path, _ts: admin.firestore.FieldValue.serverTimestamp(), _mxgUid: resolved.mxgUid, data },
        { merge: true }
      );
    }
  }

  // tombstone -> _archive_tombstones/{mxgUid}
  await db.collection("_archive_tombstones").doc(resolved.mxgUid).set({
    mxgUid: resolved.mxgUid,
    authUid: resolved.authUid,
    deletedAt: admin.firestore.FieldValue.serverTimestamp(),
    counts: {
      users: preview.usersDoc ? 1 : 0,
      accounts: preview.accounts.length,
      userPlans: preview.userPlans.length,
      transactions: preview.transactions.length,
      deposits: preview.deposits.length,
      withdrawals: preview.withdrawals.length,
      monthlyVolumesUsers: preview.monthlyVolumesUserPaths.length
    }
  }, { merge: true });
}

async function archiveCollectionDocs(db, archiveCollectionName, sourceCollection, ids, mxgUid) {
  const archiveCol = db.collection(archiveCollectionName);
  const srcCol = db.collection(sourceCollection);

  for (let i = 0; i < ids.length; i += 100) {
    const slice = ids.slice(i, i + 100);
    const snaps = await db.getAll(...slice.map(id => srcCol.doc(id)));
    const batch = db.batch();
    snaps.forEach((s) => {
      if (!s.exists) return;
      const data = s.data() || {};
      const sizeApprox = Buffer.byteLength(JSON.stringify(data));
      const docId = `${sourceCollection}__${s.ref.id}`; // stable, no slashes
      if (sizeApprox > 950000) {
        batch.set(archiveCol.doc(docId), {
          _path: s.ref.path,
          _ts: admin.firestore.FieldValue.serverTimestamp(),
          _mxgUid: mxgUid,
          partial: true,
          keys: Object.keys(data)
        }, { merge: true });
      } else {
        batch.set(archiveCol.doc(docId), {
          _path: s.ref.path,
          _ts: admin.firestore.FieldValue.serverTimestamp(),
          _mxgUid: mxgUid,
          data
        }, { merge: true });
      }
    });
    await batch.commit();
  }
}

async function deleteDocsInChunks(db, collection, ids) {
  for (let i = 0; i < ids.length; i += 400) {
    const batch = db.batch();
    const slice = ids.slice(i, i + 400);
    slice.forEach(id => batch.delete(db.collection(collection).doc(id)));
    await batch.commit();
  }
}

async function deleteByAbsolutePathsInChunks(db, paths) {
  for (let i = 0; i < paths.length; i += 400) {
    const batch = db.batch();
    const slice = paths.slice(i, i + 400);
    slice.forEach(p => batch.delete(db.doc(p)));
    await batch.commit();
  }
}
