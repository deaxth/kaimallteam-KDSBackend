const express = require("express");
const sql = require("mssql");
const { getSQLPool } = require("../mssql-pool-management");
const localConfig = require("../config/localConfig");

const router = express.Router();

function httpError(message, statusCode = 400) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function sendError(error, res) {
  console.error("[KDS kitchen router test]", error);

  res.status(error.statusCode || 500).json({
    success: false,
    message: error.message || "Unable to process kitchen routing.",
  });
}

function validOutletId(value) {
  const outletId = Number(value);

  if (!Number.isInteger(outletId) || outletId <= 0) {
    throw httpError("A valid outlet ID is required.");
  }

  return outletId;
}

function validLimit(value) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed)) {
    return 80;
  }

  return Math.min(Math.max(Math.trunc(parsed), 1), 200);
}

router.get("/outlets/:outletId/items", async (req, res) => {
  try {
    const outletId = validOutletId(req.params.outletId);
    const limit = validLimit(req.query?.limit);
    const pool = await getSQLPool(localConfig);

    const result = await pool
      .request()
      .input("OutletID", sql.Int, outletId)
      .input("Limit", sql.Int, limit)
      .query(`
        SELECT TOP (@Limit)
          h.KDSOrderID,
          CAST(h.SourceTransID AS NVARCHAR(100)) AS SourceTransID,
          h.POS_No,
          h.TerminalID,
          h.DatePOS,
          h.TableNo,
          h.OrderType,
          h.StartedAt AS OrderStartedAt,

          i.KDSItemID,
          i.ItemCode,
          i.ItemName,
          i.Qty,
          i.KDSStatus,
          i.StartedAt AS ItemStartedAt,

          claim.ClaimedOutletID,
          claim.ClaimedStationNo,
          claim.ClaimedAt,
          claim.ClaimedBy,

          COALESCE(claim.ClaimedStationNo, route.StationNo) AS StationNo,
          route.TimeNeededMinutes

        FROM dbo.KDS_MenuKitchenRoute AS route WITH (NOLOCK)
        INNER JOIN dbo.KDS_OrderItem AS i WITH (NOLOCK)
          ON i.ItemCode = route.ItemCode
        INNER JOIN dbo.KDS_OrderHeader AS h WITH (NOLOCK)
          ON h.KDSOrderID = i.KDSOrderID
        LEFT JOIN dbo.KDS_SourceItemKitchenClaim AS claim
          ON claim.SourceTransID = LTRIM(RTRIM(CAST(h.SourceTransID AS NVARCHAR(100))))
          AND claim.ItemCode = LTRIM(RTRIM(CAST(i.ItemCode AS NVARCHAR(100))))
        WHERE route.OutletID = @OutletID
          AND route.IsActive = 1
          AND h.OverallStatus <> 'DONE'
          AND (h.IsCancelled = 0 OR DATEDIFF(SECOND, h.CancelledAt, GETDATE()) <= 7)
          AND i.IsVoided = 0
          AND (claim.SourceTransID IS NULL OR claim.ClaimedOutletID = @OutletID)

        ORDER BY h.DatePOS DESC, h.KDSOrderID DESC, i.KDSItemID DESC;
      `);

    res.json({
      success: true,
      outletId,
      limit,
      data: result.recordset,
      serverTime: new Date().toISOString(),
    });
  } catch (error) {
    sendError(error, res);
  }
});

router.post("/items/:itemId/claim", async (req, res) => {
  const pool = await getSQLPool(localConfig);
  const tx = new sql.Transaction(pool);
  let transactionStarted = false;

  try {
    const itemId = Number(req.params.itemId);

    if (!Number.isInteger(itemId) || itemId <= 0) {
      throw httpError("A valid KDS item ID is required.");
    }

    const outletId = validOutletId(req.body?.outletId);
    const actorName = String(req.body?.actorName || "KDS Kitchen").trim().slice(0, 150);

    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
    transactionStarted = true;

    const itemResult = await tx
      .request()
      .input("KDSItemID", sql.BigInt, itemId)
      .query(`
        SELECT TOP 1
          i.KDSItemID,
          i.KDSOrderID,
          LTRIM(RTRIM(CAST(h.SourceTransID AS NVARCHAR(100)))) AS SourceTransID,
          i.ItemCode,
          i.KDSStatus,
          i.IsVoided,
          h.IsCancelled,
          h.OverallStatus
        FROM dbo.KDS_OrderItem AS i WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN dbo.KDS_OrderHeader AS h
          ON h.KDSOrderID = i.KDSOrderID
        WHERE i.KDSItemID = @KDSItemID;
      `);

    const item = itemResult.recordset[0];

    if (!item) throw httpError("KDS item was not found.", 404);
    if (item.IsVoided) throw httpError("Voided item cannot be claimed.");
    if (item.IsCancelled) throw httpError("Cancelled order cannot be claimed.");
    if (item.OverallStatus === "DONE") throw httpError("Completed order cannot be claimed.");
    if (item.KDSStatus !== "PREPARING") {
      throw httpError("Only PREPARING items can be claimed.");
    }

    const sourceTransId = String(item.SourceTransID || "").trim();
    const itemCode = String(item.ItemCode || "").trim();

    if (!sourceTransId || !itemCode) {
      throw httpError("The POS source order or ItemCode is missing for this item.");
    }

    const existingClaimResult = await tx
      .request()
      .input("SourceTransID", sql.NVarChar(100), sourceTransId)
      .input("ItemCode", sql.NVarChar(100), itemCode)
      .query(`
        SELECT TOP 1
          SourceTransID,
          ItemCode,
          ClaimedOutletID,
          ClaimedStationNo,
          ClaimedAt,
          ClaimedBy
        FROM dbo.KDS_SourceItemKitchenClaim WITH (UPDLOCK, HOLDLOCK)
        WHERE SourceTransID = @SourceTransID
          AND ItemCode = @ItemCode;
      `);

    const existingClaim = existingClaimResult.recordset[0];

    if (existingClaim) {
      await tx.commit();
      transactionStarted = false;

      if (existingClaim.ClaimedOutletID === outletId) {
        return res.json({
          success: true,
          alreadyClaimed: true,
          claim: {
            ...existingClaim,
            KDSItemID: itemId,
            KDSOrderID: item.KDSOrderID,
          },
        });
      }

      throw httpError("This item was already accepted by another kitchen.", 409);
    }

    const routeResult = await tx
      .request()
      .input("ItemCode", sql.NVarChar(100), itemCode)
      .input("OutletID", sql.Int, outletId)
      .query(`
        SELECT TOP 1
          MenuKitchenRouteID,
          OutletID,
          StationNo
        FROM dbo.KDS_MenuKitchenRoute WITH (UPDLOCK, HOLDLOCK)
        WHERE ItemCode = @ItemCode
          AND OutletID = @OutletID
          AND IsActive = 1;
      `);

    const route = routeResult.recordset[0];

    if (!route) {
      throw httpError("This item is not assigned to the selected kitchen.");
    }

    await tx
      .request()
      .input("SourceTransID", sql.NVarChar(100), sourceTransId)
      .input("ItemCode", sql.NVarChar(100), itemCode)
      .input("ClaimedOutletID", sql.Int, outletId)
      .input("ClaimedStationNo", sql.Int, route.StationNo)
      .input("ClaimedBy", sql.NVarChar(150), actorName || "KDS Kitchen")
      .query(`
        INSERT INTO dbo.KDS_SourceItemKitchenClaim
          (SourceTransID, ItemCode, ClaimedOutletID, ClaimedStationNo, ClaimedBy)
        VALUES
          (@SourceTransID, @ItemCode, @ClaimedOutletID, @ClaimedStationNo, @ClaimedBy);
      `);

    await tx.commit();
    transactionStarted = false;

    const io = req.app.get("io");

    io.emit("kds:refresh-needed", {
      terminalId: null,
      outletId,
      reason: "item-claimed-by-kitchen",
      kdsItemId: itemId,
      serverTime: new Date().toISOString(),
    });

    res.status(201).json({
      success: true,
      alreadyClaimed: false,
      claim: {
        KDSItemID: itemId,
        KDSOrderID: item.KDSOrderID,
        ClaimedOutletID: outletId,
        SourceTransID: sourceTransId,
        ItemCode: itemCode,
        ClaimedStationNo: route.StationNo,
        ClaimedBy: actorName || "KDS Kitchen",
      },
    });
  } catch (error) {
    if (transactionStarted) {
      await tx.rollback().catch(() => {});
    }

    sendError(error, res);
  }
});

module.exports = router;