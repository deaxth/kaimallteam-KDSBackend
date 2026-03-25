const express = require("express");
const router = express.Router();

const {
  syncNow,
  archiveNow,
  emitRefresh,
  getKitchenSnapshot,
  getPublicSnapshot,
  startPreparingItem,
  moveItemToAssembling,
  sendBackItem,
  finalizeItem,
  markItemDone,
  markOrderDone,
  doneAllItems,
} = require("../services/kdsService");

function getTerminalId(req) {
  return req.query.terminalId ? String(req.query.terminalId) : null;
}

function getActorName(req) {
  return String(req.body?.actorName || req.headers["x-kds-user"] || "KDS User");
}

router.get("/bootstrap", async (req, res) => {
  try {
    const screenType = String(req.query.screenType || "kitchen").toLowerCase();
    const terminalId = getTerminalId(req);

    const payload =
      screenType === "public"
        ? await getPublicSnapshot({ terminalId })
        : await getKitchenSnapshot({ terminalId });

    res.json(payload);
  } catch (error) {
    console.error("[KDS bootstrap]", error);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/kitchen", async (req, res) => {
  try {
    const terminalId = getTerminalId(req);
    const payload = await getKitchenSnapshot({ terminalId });
    res.json(payload);
  } catch (error) {
    console.error("[KDS kitchen]", error);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.get("/public", async (req, res) => {
  try {
    const terminalId = getTerminalId(req);
    const payload = await getPublicSnapshot({ terminalId });
    res.json(payload);
  } catch (error) {
    console.error("[KDS public]", error);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/sync/now", async (req, res) => {
  try {
    const io = req.app.get("io");
    const result = await syncNow(io);
    res.json({ success: true, ...result });
  } catch (error) {
    console.error("[KDS sync/now]", error);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/archive/now", async (req, res) => {
  try {
    const rawLimit = Number(req.body?.limit);
    const limit = Number.isFinite(rawLimit) && rawLimit > 0
      ? Math.min(rawLimit, 5000)
      : 300;

    const orderIds = Array.isArray(req.body?.orderIds)
      ? req.body.orderIds.map((v) => Number(v)).filter(Number.isFinite)
      : null;

    const reason = String(req.body?.reason || "MANUAL_ARCHIVE");

    const result = await archiveNow({
      orderIds,
      limit,
      reason,
    });

    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error("[KDS archive/now]", error);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/items/:itemId/start", async (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    const actorName = getActorName(req);

    const result = await startPreparingItem(itemId, actorName);
    emitRefresh(req.app.get("io"), result.TerminalID, "item-started", {
      orderId: result.KDSOrderID,
      itemId,
    });

    res.json({ success: true });
  } catch (error) {
    console.error("[KDS item start]", error);
    res.status(400).json({ success: false, message: error.message });
  }
});

router.post("/items/:itemId/to-assembling", async (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    const actorName = getActorName(req);

    const result = await moveItemToAssembling(itemId, actorName);
    emitRefresh(req.app.get("io"), result.TerminalID, "item-to-assembling", {
      orderId: result.KDSOrderID,
      itemId,
    });

    res.json({ success: true });
  } catch (error) {
    console.error("[KDS item -> assembling]", error);
    res.status(400).json({ success: false, message: error.message });
  }
});

router.post("/items/:itemId/send-back", async (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    const actorName = getActorName(req);
    const reason = String(req.body?.reason || "").trim();

    const result = await sendBackItem(itemId, reason, actorName);
    emitRefresh(req.app.get("io"), result.TerminalID, "item-send-back", {
      orderId: result.KDSOrderID,
      itemId,
    });

    res.json({ success: true });
  } catch (error) {
    console.error("[KDS item send-back]", error);
    res.status(400).json({ success: false, message: error.message });
  }
});

router.post("/items/:itemId/finalize", async (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    const actorName = getActorName(req);
    const mode = String(req.body?.mode || "SERVING").toUpperCase();

    const result = await finalizeItem(itemId, mode, actorName);
    emitRefresh(req.app.get("io"), result.TerminalID, "item-finalized", {
      orderId: result.KDSOrderID,
      itemId,
      mode,
    });

    res.json({ success: true });
  } catch (error) {
    console.error("[KDS item finalize]", error);
    res.status(400).json({ success: false, message: error.message });
  }
});

router.post("/items/:itemId/done", async (req, res) => {
  try {
    const itemId = Number(req.params.itemId);
    const actorName = getActorName(req);

    const result = await markItemDone(itemId, actorName);
    emitRefresh(req.app.get("io"), result.TerminalID, "item-done", {
      orderId: result.KDSOrderID,
      itemId,
    });

    res.json({ success: true });
  } catch (error) {
    console.error("[KDS item done]", error);
    res.status(400).json({ success: false, message: error.message });
  }
});

router.post("/orders/:orderId/done", async (req, res) => {
  try {
    const orderId = Number(req.params.orderId);
    const actorName = getActorName(req);

    const result = await markOrderDone(orderId, actorName);
    emitRefresh(req.app.get("io"), result.TerminalID, "order-done", {
      orderId,
    });

    res.json({ success: true });
  } catch (error) {
    console.error("[KDS order done]", error);
    res.status(400).json({ success: false, message: error.message });
  }
});

router.post("/all/done-all-items", async (req, res) => {
  try {
    const terminalIds = req.body?.terminalIds;
    const actorName = getActorName(req);

    const result = await doneAllItems(terminalIds, actorName);

    for (const terminalId of result.terminalIds || []) {
      emitRefresh(req.app.get("io"), terminalId, "done-all-items", {
        actorName,
      });
    }

    res.json({ success: true, ...result });
  } catch (err) {
    console.error("[KDS done-all-items]", err);
    res.status(400).json({ success: false, message: err.message });
  }
});

module.exports = router;