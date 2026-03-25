const sql = require("mssql");
const { getSQLPool } = require("../mssql-pool-management");

const sourceCfgModule = require("../config/config");
const localCfgModule = require("../config/localConfig");

const sourceDbConfig = sourceCfgModule.dbconfig || sourceCfgModule;
const localDbConfig = localCfgModule.dbconfig || localCfgModule;

const POLL_MS = 1500;
const NEW_ORDER_LOOKBACK_MINUTES = 720;
const CANCEL_VISIBLE_SECONDS = 5;

let syncTimer = null;
let syncBusy = false;

function normalizeText(value) {
  return String(value ?? "").trim();
}

function normalizeUpper(value) {
  return normalizeText(value).toUpperCase();
}

function isCancelledStatus(value) {
  const v = normalizeUpper(value);
  return v === "CANCELLED" || v === "CANCELED";
}

function isVoidedStatus(value) {
  return normalizeUpper(value) === "VOIDED";
}

async function getSourcePool() {
  return getSQLPool(sourceDbConfig);
}

async function getLocalPool() {
  return getSQLPool(localDbConfig);
}

function requestOf(executor) {
  return executor.request();
}

async function recordHistory(executor, data) {
  await requestOf(executor)
    .input("EntityType", sql.NVarChar(10), data.entityType)
    .input("EntityID", sql.BigInt, data.entityId)
    .input("KDSOrderID", sql.BigInt, data.kdsOrderId ?? null)
    .input("KDSItemID", sql.BigInt, data.kdsItemId ?? null)
    .input("FromStatus", sql.NVarChar(20), data.fromStatus ?? null)
    .input("ToStatus", sql.NVarChar(20), data.toStatus ?? null)
    .input("Action", sql.NVarChar(50), data.action)
    .input("Reason", sql.NVarChar(500), data.reason ?? null)
    .input("SourceStatus", sql.NVarChar(50), data.sourceStatus ?? null)
    .input("ActorName", sql.NVarChar(100), data.actorName ?? null)
    .query(`
      INSERT INTO dbo.KDS_StatusHistory
      (
        EntityType, EntityID, KDSOrderID, KDSItemID,
        FromStatus, ToStatus, Action, Reason, SourceStatus, ActorName, CreatedAt
      )
      VALUES
      (
        @EntityType, @EntityID, @KDSOrderID, @KDSItemID,
        @FromStatus, @ToStatus, @Action, @Reason, @SourceStatus, @ActorName, SYSUTCDATETIME()
      )
    `);
}

async function getOrderHeaderBySourceTransId(executor, sourceTransId) {
  const { recordset } = await requestOf(executor)
    .input("SourceTransID", sql.BigInt, sourceTransId)
    .query(`
      SELECT TOP 1 *
      FROM dbo.KDS_OrderHeader
      WHERE SourceTransID = @SourceTransID
    `);

  return recordset[0] || null;
}

async function getItemBySourceRecordId(executor, sourceRecordId) {
  const { recordset } = await requestOf(executor)
    .input("SourceRecordID", sql.BigInt, sourceRecordId)
    .query(`
      SELECT TOP 1 *
      FROM dbo.KDS_OrderItem
      WHERE SourceRecordID = @SourceRecordID
    `);

  return recordset[0] || null;
}

async function createOrderHeader(localPool, header) {
  const overallStatus = isCancelledStatus(header.HeaderStatus) ? "CANCELLED" : "PREPARING";

  const { recordset } = await localPool
    .request()
    .input("SourceTransID", sql.BigInt, header.TransID)
    .input("POS_No", sql.NVarChar(50), normalizeText(header.POS_No))
    .input("TerminalID", sql.NVarChar(50), normalizeText(header.TerminalID))
    .input("DatePOS", sql.DateTime2, header.DatePOS)
    .input("TableNo", sql.NVarChar(50), normalizeText(header.TableNo) || null)
    .input("CustomerRemarks", sql.NVarChar(500), normalizeText(header.CustomerRemarks) || null)
    .input("OverallStatus", sql.NVarChar(20), overallStatus)
    .input("HeaderStatus", sql.NVarChar(50), normalizeText(header.HeaderStatus) || null)
    .query(`
      INSERT INTO dbo.KDS_OrderHeader
      (
        SourceTransID, POS_No, TerminalID, DatePOS, TableNo, CustomerRemarks,
        OverallStatus, LastSourceHeaderStatus,
        PreparingAt, CancelledAt, IsCancelled, CreatedAt, UpdatedAt, LastSyncedAt
      )
      OUTPUT INSERTED.*
      VALUES
      (
        @SourceTransID, @POS_No, @TerminalID, @DatePOS, @TableNo, @CustomerRemarks,
        @OverallStatus, @HeaderStatus,
        CASE WHEN @OverallStatus = 'PREPARING' THEN SYSUTCDATETIME() ELSE NULL END,
        CASE WHEN @OverallStatus = 'CANCELLED' THEN SYSUTCDATETIME() ELSE NULL END,
        CASE WHEN @OverallStatus = 'CANCELLED' THEN 1 ELSE 0 END,
        SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME()
      )
    `);

  const created = recordset[0];

  await recordHistory(localPool, {
    entityType: "ORDER",
    entityId: created.KDSOrderID,
    kdsOrderId: created.KDSOrderID,
    fromStatus: null,
    toStatus: overallStatus,
    action: "SOURCE_IMPORT",
    sourceStatus: header.HeaderStatus,
  });

  return created;
}

async function updateOrderHeaderFromSource(localPool, kdsOrderId, header) {
  await localPool
    .request()
    .input("KDSOrderID", sql.BigInt, kdsOrderId)
    .input("POS_No", sql.NVarChar(50), normalizeText(header.POS_No))
    .input("TerminalID", sql.NVarChar(50), normalizeText(header.TerminalID))
    .input("DatePOS", sql.DateTime2, header.DatePOS)
    .input("TableNo", sql.NVarChar(50), normalizeText(header.TableNo) || null)
    .input("CustomerRemarks", sql.NVarChar(500), normalizeText(header.CustomerRemarks) || null)
    .input("HeaderStatus", sql.NVarChar(50), normalizeText(header.HeaderStatus) || null)
    .query(`
      UPDATE dbo.KDS_OrderHeader
      SET
        POS_No = @POS_No,
        TerminalID = @TerminalID,
        DatePOS = @DatePOS,
        TableNo = @TableNo,
        CustomerRemarks = @CustomerRemarks,
        LastSourceHeaderStatus = @HeaderStatus,
        LastSyncedAt = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
      WHERE KDSOrderID = @KDSOrderID
    `);
}

async function createOrderItem(localPool, kdsOrderId, detail) {
  const sourceStatus = normalizeText(detail.DetailStatus) || null;
  const isVoided = isVoidedStatus(sourceStatus);

  const { recordset } = await localPool
    .request()
    .input("KDSOrderID", sql.BigInt, kdsOrderId)
    .input("SourceRecordID", sql.BigInt, detail.RecordID)
    .input("SourceTransID", sql.BigInt, detail.TransID)
    .input("ItemCode", sql.NVarChar(50), normalizeText(detail.ItemCode))
    .input("ItemName", sql.NVarChar(255), normalizeText(detail.ItemName) || normalizeText(detail.ItemCode))
    .input("Qty", sql.Decimal(18, 4), Number(detail.QTY || 0))
    .input("UOM", sql.NVarChar(50), normalizeText(detail.UOM) || null)
    .input("SourceItemStatus", sql.NVarChar(50), sourceStatus)
    .input("IsVoided", sql.Bit, isVoided)
    .query(`
      INSERT INTO dbo.KDS_OrderItem
      (
        KDSOrderID, SourceRecordID, SourceTransID, ItemCode, ItemName, Qty, UOM,
        KDSStatus, FulfillmentMode, SourceItemStatus, IsVoided, VoidedAt,
        PreparingAt, CreatedAt, UpdatedAt
      )
      OUTPUT INSERTED.*
      VALUES
      (
        @KDSOrderID, @SourceRecordID, @SourceTransID, @ItemCode, @ItemName, @Qty, @UOM,
        'PREPARING', NULL, @SourceItemStatus, @IsVoided,
        CASE WHEN @IsVoided = 1 THEN SYSUTCDATETIME() ELSE NULL END,
        SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME()
      )
    `);

  const item = recordset[0];

  await recordHistory(localPool, {
    entityType: "ITEM",
    entityId: item.KDSItemID,
    kdsOrderId,
    kdsItemId: item.KDSItemID,
    fromStatus: null,
    toStatus: isVoided ? "VOIDED" : "PREPARING",
    action: isVoided ? "SOURCE_VOIDED_IMPORT" : "SOURCE_IMPORT",
    sourceStatus,
  });

  return item;
}

async function updateItemFromSource(localPool, item, detail) {
  const sourceStatus = normalizeText(detail.DetailStatus) || null;
  const nowVoided = isVoidedStatus(sourceStatus);

  await localPool
    .request()
    .input("KDSItemID", sql.BigInt, item.KDSItemID)
    .input("ItemCode", sql.NVarChar(50), normalizeText(detail.ItemCode))
    .input("ItemName", sql.NVarChar(255), normalizeText(detail.ItemName) || normalizeText(detail.ItemCode))
    .input("Qty", sql.Decimal(18, 4), Number(detail.QTY || 0))
    .input("UOM", sql.NVarChar(50), normalizeText(detail.UOM) || null)
    .input("SourceItemStatus", sql.NVarChar(50), sourceStatus)
    .input("NowVoided", sql.Bit, nowVoided)
    .query(`
      UPDATE dbo.KDS_OrderItem
      SET
        ItemCode = @ItemCode,
        ItemName = @ItemName,
        Qty = @Qty,
        UOM = @UOM,
        SourceItemStatus = @SourceItemStatus,
        IsVoided = CASE WHEN @NowVoided = 1 THEN 1 ELSE IsVoided END,
        VoidedAt = CASE WHEN @NowVoided = 1 AND VoidedAt IS NULL THEN SYSUTCDATETIME() ELSE VoidedAt END,
        UpdatedAt = SYSUTCDATETIME()
      WHERE KDSItemID = @KDSItemID
    `);

  if (nowVoided && !item.IsVoided) {
    await recordHistory(localPool, {
      entityType: "ITEM",
      entityId: item.KDSItemID,
      kdsOrderId: item.KDSOrderID,
      kdsItemId: item.KDSItemID,
      fromStatus: item.KDSStatus,
      toStatus: "VOIDED",
      action: "SOURCE_VOIDED",
      sourceStatus,
    });
  }
}

async function recomputeOrderStatus(executor, kdsOrderId) {
  const headerRs = await requestOf(executor)
    .input("KDSOrderID", sql.BigInt, kdsOrderId)
    .query(`
      SELECT TOP 1 KDSOrderID, OverallStatus, IsCancelled
      FROM dbo.KDS_OrderHeader
      WHERE KDSOrderID = @KDSOrderID
    `);

  const header = headerRs.recordset[0];
  if (!header || header.IsCancelled) return header;

  const statsRs = await requestOf(executor)
    .input("KDSOrderID", sql.BigInt, kdsOrderId)
    .query(`
      SELECT
        SUM(CASE WHEN IsVoided = 0 THEN 1 ELSE 0 END) AS ActiveCount,
        SUM(CASE WHEN IsVoided = 0 AND KDSStatus = 'PREPARING' THEN 1 ELSE 0 END) AS PreparingCount,
        SUM(CASE WHEN IsVoided = 0 AND KDSStatus = 'ASSEMBLING' THEN 1 ELSE 0 END) AS AssemblingCount,
        SUM(CASE WHEN IsVoided = 0 AND KDSStatus = 'SERVING' THEN 1 ELSE 0 END) AS ServingCount,
        SUM(CASE WHEN IsVoided = 0 AND KDSStatus = 'PICKUP' THEN 1 ELSE 0 END) AS PickupCount,
        SUM(CASE WHEN IsVoided = 0 AND KDSStatus = 'DONE' THEN 1 ELSE 0 END) AS DoneCount
      FROM dbo.KDS_OrderItem
      WHERE KDSOrderID = @KDSOrderID
    `);

  const stats = statsRs.recordset[0] || {};
  const activeCount = Number(stats.ActiveCount || 0);
  const preparingCount = Number(stats.PreparingCount || 0);
  const assemblingCount = Number(stats.AssemblingCount || 0);
  const servingCount = Number(stats.ServingCount || 0);
  const pickupCount = Number(stats.PickupCount || 0);
  const doneCount = Number(stats.DoneCount || 0);

  let nextStatus = "PREPARING";
  if (activeCount === 0 || doneCount === activeCount) nextStatus = "DONE";
  else if (preparingCount > 0) nextStatus = "PREPARING";
  else if (assemblingCount > 0) nextStatus = "ASSEMBLING";
  else if (servingCount > 0 && pickupCount === 0) nextStatus = "SERVING";
  else if (pickupCount > 0 && servingCount === 0) nextStatus = "PICKUP";
  else if (servingCount > 0 || pickupCount > 0) nextStatus = "SERVING";

  if (nextStatus !== header.OverallStatus) {
    await requestOf(executor)
      .input("KDSOrderID", sql.BigInt, kdsOrderId)
      .input("NextStatus", sql.NVarChar(20), nextStatus)
      .query(`
        UPDATE dbo.KDS_OrderHeader
        SET
          OverallStatus = @NextStatus,
          AssemblingAt = CASE WHEN @NextStatus = 'ASSEMBLING' AND AssemblingAt IS NULL THEN SYSUTCDATETIME() ELSE AssemblingAt END,
          ServingAt = CASE WHEN @NextStatus = 'SERVING' AND ServingAt IS NULL THEN SYSUTCDATETIME() ELSE ServingAt END,
          PickUpAt = CASE WHEN @NextStatus = 'PICKUP' AND PickUpAt IS NULL THEN SYSUTCDATETIME() ELSE PickUpAt END,
          DoneAt = CASE WHEN @NextStatus = 'DONE' AND DoneAt IS NULL THEN SYSUTCDATETIME() ELSE DoneAt END,
          UpdatedAt = SYSUTCDATETIME()
        WHERE KDSOrderID = @KDSOrderID
      `);

    await recordHistory(executor, {
      entityType: "ORDER",
      entityId: kdsOrderId,
      kdsOrderId,
      fromStatus: header.OverallStatus,
      toStatus: nextStatus,
      action: nextStatus === "DONE" ? "ORDER_DONE_AUTO" : "ORDER_STATUS_RECOMPUTE",
    });
  }

  return { ...header, OverallStatus: nextStatus };
}

async function cancelOrderFromSource(localPool, order, sourceStatus) {
  if (order.IsCancelled) return;

  await localPool
    .request()
    .input("KDSOrderID", sql.BigInt, order.KDSOrderID)
    .input("SourceStatus", sql.NVarChar(50), sourceStatus || "Cancelled")
    .query(`
      UPDATE dbo.KDS_OrderHeader
      SET
        IsCancelled = 1,
        OverallStatus = 'CANCELLED',
        LastSourceHeaderStatus = @SourceStatus,
        CancelledAt = COALESCE(CancelledAt, SYSUTCDATETIME()),
        UpdatedAt = SYSUTCDATETIME(),
        LastSyncedAt = SYSUTCDATETIME()
      WHERE KDSOrderID = @KDSOrderID
    `);

  await recordHistory(localPool, {
    entityType: "ORDER",
    entityId: order.KDSOrderID,
    kdsOrderId: order.KDSOrderID,
    fromStatus: order.OverallStatus,
    toStatus: "CANCELLED",
    action: "SOURCE_CANCELLED",
    sourceStatus,
  });
}

async function fetchSourceHeaders(sourcePool) {
  const { recordset } = await sourcePool
    .request()
    .input("LookbackMinutes", sql.Int, NEW_ORDER_LOOKBACK_MINUTES)
    .query(`
      SELECT
        p.TransID,
        p.POS_No,
        CAST(p.TerminalID AS NVARCHAR(50)) AS TerminalID,
        p.DatePOS,
        CAST(p.TableNo AS NVARCHAR(50)) AS TableNo,
        CAST(ISNULL(p.CustomerRemarks, '') AS NVARCHAR(500)) AS CustomerRemarks,
        CAST(ISNULL(p.Status, '') AS NVARCHAR(50)) AS HeaderStatus
      FROM dbo.tblPOS p
      WHERE p.DatePOS >= DATEADD(MINUTE, -@LookbackMinutes, GETDATE())
      ORDER BY p.DatePOS ASC
    `);

  return recordset;
}

async function fetchSourceDetails(sourcePool, transId) {
  const { recordset } = await sourcePool
    .request()
    .input("TransID", sql.BigInt, transId)
    .query(`
      SELECT
        d.RecordID,
        d.TransID,
        d.ItemCode,
        CAST(ISNULL(im.ItemName, d.ItemCode) AS NVARCHAR(255)) AS ItemName,
        d.QTY,
        CAST(d.UOM AS NVARCHAR(50)) AS UOM,
        CAST(ISNULL(d.Status, '') AS NVARCHAR(50)) AS DetailStatus
      FROM dbo.tblPOS_Details d
      LEFT JOIN JADE_01.dbo.tblItem_Master im
        ON im.ItemCode = d.ItemCode
      WHERE d.TransID = @TransID
      ORDER BY d.RecordID ASC
    `);

  return recordset;
}

async function upsertSourceOrder(sourcePool, localPool, header) {
  let localOrder = await getOrderHeaderBySourceTransId(localPool, header.TransID);

  if (!localOrder) {
    localOrder = await createOrderHeader(localPool, header);
  } else {
    await updateOrderHeaderFromSource(localPool, localOrder.KDSOrderID, header);
    localOrder = await getOrderHeaderBySourceTransId(localPool, header.TransID);
  }

  const details = await fetchSourceDetails(sourcePool, header.TransID);

  for (const detail of details) {
    const existingItem = await getItemBySourceRecordId(localPool, detail.RecordID);
    if (!existingItem) {
      await createOrderItem(localPool, localOrder.KDSOrderID, detail);
    } else {
      await updateItemFromSource(localPool, existingItem, detail);
    }
  }

  if (isCancelledStatus(header.HeaderStatus)) {
    await cancelOrderFromSource(localPool, localOrder, header.HeaderStatus);
  } else {
    await recomputeOrderStatus(localPool, localOrder.KDSOrderID);
  }

  return getOrderHeaderBySourceTransId(localPool, header.TransID);
}

function buildInClause(request, values, prefix) {
  return values
    .map((value, index) => {
      const key = `${prefix}${index}`;
      request.input(key, sql.BigInt, value);
      return `@${key}`;
    })
    .join(", ");
}

async function refreshActiveOrders(sourcePool, localPool) {
  const activeOrdersRs = await localPool.request().query(`
    SELECT KDSOrderID, SourceTransID
    FROM dbo.KDS_OrderHeader
    WHERE OverallStatus <> 'DONE'
      AND IsCancelled = 0
  `);

  const activeOrders = activeOrdersRs.recordset || [];
  if (!activeOrders.length) return [];

  const sourceReq = sourcePool.request();
  const clause = buildInClause(
    sourceReq,
    activeOrders.map((row) => row.SourceTransID),
    "TransID"
  );

  const headerRowsRs = await sourceReq.query(`
    SELECT
      p.TransID,
      p.POS_No,
      CAST(p.TerminalID AS NVARCHAR(50)) AS TerminalID,
      p.DatePOS,
      CAST(p.TableNo AS NVARCHAR(50)) AS TableNo,
      CAST(ISNULL(p.CustomerRemarks, '') AS NVARCHAR(500)) AS CustomerRemarks,
      CAST(ISNULL(p.Status, '') AS NVARCHAR(50)) AS HeaderStatus
    FROM dbo.tblPOS p
    WHERE p.TransID IN (${clause})
  `);

  const headerMap = new Map((headerRowsRs.recordset || []).map((row) => [String(row.TransID), row]));
  const changedTerminalIds = new Set();

  for (const order of activeOrders) {
    const sourceHeader = headerMap.get(String(order.SourceTransID));
    if (!sourceHeader) continue;

    const result = await upsertSourceOrder(sourcePool, localPool, sourceHeader);
    if (result?.TerminalID) changedTerminalIds.add(String(result.TerminalID));
  }

  return [...changedTerminalIds];
}

async function getKnownSourceTransIds(localPool) {
  const { recordset } = await localPool.request()
    .input("LookbackMinutes", sql.Int, NEW_ORDER_LOOKBACK_MINUTES)
    .query(`
      SELECT SourceTransID
      FROM dbo.KDS_OrderHeader
      WHERE DatePOS >= DATEADD(MINUTE, -@LookbackMinutes, SYSUTCDATETIME())
         OR OverallStatus <> 'DONE'
    `);

  return new Set((recordset || []).map((row) => String(row.SourceTransID)));
}

function emitRefresh(io, terminalId, reason, meta = {}) {
  if (!io) return;

  const payload = {
    terminalId: terminalId ? String(terminalId) : null,
    reason,
    at: new Date().toISOString(),
    ...meta,
  };

  if (terminalId) {
    io.to(`terminal:${terminalId}`).emit("kds:refresh-needed", payload);
    io.to("terminal:ALL").emit("kds:refresh-needed", payload);
    return;
  }

  io.emit("kds:refresh-needed", payload);
}

async function syncNow(io) {
  if (syncBusy) return { ok: true, skipped: true };
  syncBusy = true;

  try {
    const [sourcePool, localPool] = await Promise.all([getSourcePool(), getLocalPool()]);
    const headers = await fetchSourceHeaders(sourcePool);
    const knownSourceTransIds = await getKnownSourceTransIds(localPool);
    const touchedTerminals = new Set();

    for (const header of headers) {
      if (knownSourceTransIds.has(String(header.TransID))) continue;

      const order = await upsertSourceOrder(sourcePool, localPool, header);
      if (order?.TerminalID) touchedTerminals.add(String(order.TerminalID));
    }

    const activeTouched = await refreshActiveOrders(sourcePool, localPool);
    activeTouched.forEach((id) => touchedTerminals.add(String(id)));

    touchedTerminals.forEach((terminalId) => emitRefresh(io, terminalId, "sync-complete"));

    return { ok: true };
  } finally {
    syncBusy = false;
  }
}

function startSync(io) {
  if (syncTimer) return;

  syncNow(io).catch((error) => {
    console.error("[KDS sync] initial error:", error.message);
  });

  syncTimer = setInterval(() => {
    syncNow(io).catch((error) => {
      console.error("[KDS sync] interval error:", error.message);
    });
  }, POLL_MS);
}

function stopSync() {
  if (syncTimer) {
    clearInterval(syncTimer);
    syncTimer = null;
  }
}

function groupKitchenRows(rows) {
  const byOrder = new Map();

  for (const row of rows) {
    if (!byOrder.has(row.KDSOrderID)) {
      byOrder.set(row.KDSOrderID, {
        KDSOrderID: row.KDSOrderID,
        SourceTransID: row.SourceTransID,
        POS_No: row.POS_No,
        TerminalID: row.TerminalID,
        DatePOS: row.DatePOS,
        TableNo: row.TableNo,
        CustomerRemarks: row.CustomerRemarks,
        OverallStatus: row.OverallStatus,
        IsCancelled: !!row.IsCancelled,
        CancelledAt: row.CancelledAt,
        items: [],
      });
    }

    if (row.KDSItemID) {
      byOrder.get(row.KDSOrderID).items.push({
        KDSItemID: row.KDSItemID,
        SourceRecordID: row.SourceRecordID,
        ItemCode: row.ItemCode,
        ItemName: row.ItemName,
        Qty: row.Qty,
        UOM: row.UOM,
        KDSStatus: row.KDSStatus,
        FulfillmentMode: row.FulfillmentMode,
        IsVoided: !!row.IsVoided,
        SourceItemStatus: row.SourceItemStatus,
        LastSendBackReason: row.LastSendBackReason,
        SendBackCount: row.SendBackCount,
      });
    }
  }

  return [...byOrder.values()];
}

async function getKitchenSnapshot({ terminalId } = {}) {
  const localPool = await getLocalPool();
  const req = localPool.request();

  let where = `
    WHERE h.OverallStatus <> 'DONE'
      AND (h.IsCancelled = 0 OR DATEDIFF(SECOND, h.CancelledAt, SYSUTCDATETIME()) <= ${CANCEL_VISIBLE_SECONDS})
  `;

  if (terminalId) {
    req.input("TerminalID", sql.NVarChar(50), String(terminalId));
    where += ` AND h.TerminalID = @TerminalID`;
  }

  const { recordset } = await req.query(`
    SELECT
      h.KDSOrderID,
      h.SourceTransID,
      h.POS_No,
      h.TerminalID,
      h.DatePOS,
      h.TableNo,
      h.CustomerRemarks,
      h.OverallStatus,
      h.IsCancelled,
      h.CancelledAt,
      i.KDSItemID,
      i.SourceRecordID,
      i.ItemCode,
      i.ItemName,
      i.Qty,
      i.UOM,
      i.KDSStatus,
      i.FulfillmentMode,
      i.IsVoided,
      i.SourceItemStatus,
      i.LastSendBackReason,
      i.SendBackCount
    FROM dbo.KDS_OrderHeader h
    LEFT JOIN dbo.KDS_OrderItem i
      ON i.KDSOrderID = h.KDSOrderID
    ${where}
    ORDER BY h.DatePOS ASC, i.SourceRecordID ASC
  `);

  return {
    success: true,
    orders: groupKitchenRows(recordset || []),
    serverTime: new Date().toISOString(),
  };
}

async function getPublicSnapshot({ terminalId } = {}) {
  const localPool = await getLocalPool();
  const req = localPool.request();

  let where = `
    WHERE h.IsCancelled = 0
      AND h.OverallStatus <> 'DONE'
      AND i.IsVoided = 0
      AND i.KDSStatus IN ('PREPARING', 'SERVING', 'PICKUP')
  `;

  if (terminalId) {
    req.input("TerminalID", sql.NVarChar(50), String(terminalId));
    where += ` AND h.TerminalID = @TerminalID`;
  }

  const { recordset } = await req.query(`
    SELECT DISTINCT
      h.KDSOrderID,
      h.POS_No,
      h.TerminalID,
      h.DatePOS,
      i.KDSStatus
    FROM dbo.KDS_OrderHeader h
    INNER JOIN dbo.KDS_OrderItem i
      ON i.KDSOrderID = h.KDSOrderID
    ${where}
    ORDER BY h.DatePOS ASC
  `);

  const preparing = [];
  const serving = [];
  const pickup = [];

  for (const row of recordset || []) {
    const item = {
      KDSOrderID: row.KDSOrderID,
      POS_No: row.POS_No,
      TerminalID: row.TerminalID,
      DatePOS: row.DatePOS,
    };

    if (row.KDSStatus === "PREPARING") preparing.push(item);
    if (row.KDSStatus === "SERVING") serving.push(item);
    if (row.KDSStatus === "PICKUP") pickup.push(item);
  }

  return {
    success: true,
    preparing,
    serving,
    pickup,
    serverTime: new Date().toISOString(),
  };
}

async function getItemContext(executor, itemId) {
  const { recordset } = await requestOf(executor)
    .input("KDSItemID", sql.BigInt, itemId)
    .query(`
      SELECT TOP 1
        i.KDSItemID,
        i.KDSOrderID,
        i.KDSStatus,
        i.IsVoided,
        i.FulfillmentMode,
        o.TerminalID,
        o.IsCancelled,
        o.OverallStatus
      FROM dbo.KDS_OrderItem i
      INNER JOIN dbo.KDS_OrderHeader o
        ON o.KDSOrderID = i.KDSOrderID
      WHERE i.KDSItemID = @KDSItemID
    `);

  return recordset[0] || null;
}

async function moveItemToAssembling(itemId, actorName) {
  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const item = await getItemContext(tx, itemId);
    if (!item) throw new Error("Item not found.");
    if (item.IsVoided) throw new Error("Voided item cannot be updated.");
    if (item.IsCancelled) throw new Error("Cancelled order cannot be updated.");
    if (item.KDSStatus !== "PREPARING") throw new Error("Only PREPARING items can move to ASSEMBLING.");

    await requestOf(tx)
      .input("KDSItemID", sql.BigInt, itemId)
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        UPDATE dbo.KDS_OrderItem
        SET
          KDSStatus = 'ASSEMBLING',
          AssemblingAt = COALESCE(AssemblingAt, SYSUTCDATETIME()),
          LastActionBy = @ActorName,
          LastActionAt = SYSUTCDATETIME(),
          UpdatedAt = SYSUTCDATETIME()
        WHERE KDSItemID = @KDSItemID
      `);

    await recordHistory(tx, {
      entityType: "ITEM",
      entityId: itemId,
      kdsOrderId: item.KDSOrderID,
      kdsItemId: itemId,
      fromStatus: item.KDSStatus,
      toStatus: "ASSEMBLING",
      action: "CHEF_DONE",
      actorName,
    });

    await recomputeOrderStatus(tx, item.KDSOrderID);
    await tx.commit();

    return { KDSOrderID: item.KDSOrderID, TerminalID: item.TerminalID };
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

async function sendBackItem(itemId, reason, actorName) {
  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const item = await getItemContext(tx, itemId);
    if (!item) throw new Error("Item not found.");
    if (item.IsVoided) throw new Error("Voided item cannot be updated.");
    if (item.IsCancelled) throw new Error("Cancelled order cannot be updated.");
    if (item.KDSStatus !== "ASSEMBLING") throw new Error("Only ASSEMBLING items can be sent back.");

    await requestOf(tx)
      .input("KDSItemID", sql.BigInt, itemId)
      .input("Reason", sql.NVarChar(500), normalizeText(reason) || "No remarks")
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        UPDATE dbo.KDS_OrderItem
        SET
          KDSStatus = 'PREPARING',
          FulfillmentMode = NULL,
          SendBackCount = ISNULL(SendBackCount, 0) + 1,
          LastSendBackReason = @Reason,
          LastSentBackAt = SYSUTCDATETIME(),
          LastActionBy = @ActorName,
          LastActionAt = SYSUTCDATETIME(),
          UpdatedAt = SYSUTCDATETIME()
        WHERE KDSItemID = @KDSItemID
      `);

    await recordHistory(tx, {
      entityType: "ITEM",
      entityId: itemId,
      kdsOrderId: item.KDSOrderID,
      kdsItemId: itemId,
      fromStatus: item.KDSStatus,
      toStatus: "PREPARING",
      action: "SEND_BACK",
      reason,
      actorName,
    });

    await recomputeOrderStatus(tx, item.KDSOrderID);
    await tx.commit();

    return { KDSOrderID: item.KDSOrderID, TerminalID: item.TerminalID };
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

async function finalizeItem(itemId, mode, actorName) {
  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const nextStatus = normalizeUpper(mode) === "PICKUP" ? "PICKUP" : "SERVING";
    const item = await getItemContext(tx, itemId);
    if (!item) throw new Error("Item not found.");
    if (item.IsVoided) throw new Error("Voided item cannot be updated.");
    if (item.IsCancelled) throw new Error("Cancelled order cannot be updated.");
    if (item.KDSStatus !== "ASSEMBLING") throw new Error("Only ASSEMBLING items can be finalized.");

    await requestOf(tx)
      .input("KDSItemID", sql.BigInt, itemId)
      .input("NextStatus", sql.NVarChar(20), nextStatus)
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        UPDATE dbo.KDS_OrderItem
        SET
          KDSStatus = @NextStatus,
          FulfillmentMode = @NextStatus,
          ServingAt = CASE WHEN @NextStatus = 'SERVING' AND ServingAt IS NULL THEN SYSUTCDATETIME() ELSE ServingAt END,
          PickUpAt = CASE WHEN @NextStatus = 'PICKUP' AND PickUpAt IS NULL THEN SYSUTCDATETIME() ELSE PickUpAt END,
          LastActionBy = @ActorName,
          LastActionAt = SYSUTCDATETIME(),
          UpdatedAt = SYSUTCDATETIME()
        WHERE KDSItemID = @KDSItemID
      `);

    await recordHistory(tx, {
      entityType: "ITEM",
      entityId: itemId,
      kdsOrderId: item.KDSOrderID,
      kdsItemId: itemId,
      fromStatus: item.KDSStatus,
      toStatus: nextStatus,
      action: nextStatus === "SERVING" ? "FINALIZE_SERVE" : "FINALIZE_PICKUP",
      actorName,
    });

    await recomputeOrderStatus(tx, item.KDSOrderID);
    await tx.commit();

    return { KDSOrderID: item.KDSOrderID, TerminalID: item.TerminalID };
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

async function markItemDone(itemId, actorName) {
  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const item = await getItemContext(tx, itemId);
    if (!item) throw new Error("Item not found.");
    if (item.IsVoided) throw new Error("Voided item cannot be updated.");
    if (item.IsCancelled) throw new Error("Cancelled order cannot be updated.");
    if (!["SERVING", "PICKUP"].includes(item.KDSStatus)) {
      throw new Error("Only SERVING or PICKUP items can be completed.");
    }

    await requestOf(tx)
      .input("KDSItemID", sql.BigInt, itemId)
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        UPDATE dbo.KDS_OrderItem
        SET
          KDSStatus = 'DONE',
          DoneAt = COALESCE(DoneAt, SYSUTCDATETIME()),
          LastActionBy = @ActorName,
          LastActionAt = SYSUTCDATETIME(),
          UpdatedAt = SYSUTCDATETIME()
        WHERE KDSItemID = @KDSItemID
      `);

    await recordHistory(tx, {
      entityType: "ITEM",
      entityId: itemId,
      kdsOrderId: item.KDSOrderID,
      kdsItemId: itemId,
      fromStatus: item.KDSStatus,
      toStatus: "DONE",
      action: "ITEM_DONE",
      actorName,
    });

    await recomputeOrderStatus(tx, item.KDSOrderID);
    await tx.commit();

    return { KDSOrderID: item.KDSOrderID, TerminalID: item.TerminalID };
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

async function markOrderDone(orderId, actorName) {
  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const headerRs = await requestOf(tx)
      .input("KDSOrderID", sql.BigInt, orderId)
      .query(`
        SELECT TOP 1 KDSOrderID, TerminalID, OverallStatus, IsCancelled
        FROM dbo.KDS_OrderHeader
        WHERE KDSOrderID = @KDSOrderID
      `);

    const header = headerRs.recordset[0];
    if (!header) throw new Error("Order not found.");
    if (header.IsCancelled) throw new Error("Cancelled order cannot be completed.");

    const blockingRs = await requestOf(tx)
      .input("KDSOrderID", sql.BigInt, orderId)
      .query(`
        SELECT COUNT(1) AS BlockingCount
        FROM dbo.KDS_OrderItem
        WHERE KDSOrderID = @KDSOrderID
          AND IsVoided = 0
          AND KDSStatus IN ('PREPARING', 'ASSEMBLING')
      `);

    if (Number(blockingRs.recordset[0]?.BlockingCount || 0) > 0) {
      throw new Error("All active items must be finalized before completing the order.");
    }

    await requestOf(tx)
      .input("KDSOrderID", sql.BigInt, orderId)
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        INSERT INTO dbo.KDS_StatusHistory
        (
          EntityType, EntityID, KDSOrderID, KDSItemID,
          FromStatus, ToStatus, Action, Reason, SourceStatus, ActorName, CreatedAt
        )
        SELECT
          'ITEM', i.KDSItemID, i.KDSOrderID, i.KDSItemID,
          i.KDSStatus, 'DONE', 'ORDER_DONE_MANUAL', NULL, NULL, @ActorName, SYSUTCDATETIME()
        FROM dbo.KDS_OrderItem i
        WHERE i.KDSOrderID = @KDSOrderID
          AND i.IsVoided = 0
          AND i.KDSStatus <> 'DONE'
      `);

    await requestOf(tx)
      .input("KDSOrderID", sql.BigInt, orderId)
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        UPDATE dbo.KDS_OrderItem
        SET
          KDSStatus = 'DONE',
          DoneAt = COALESCE(DoneAt, SYSUTCDATETIME()),
          LastActionBy = @ActorName,
          LastActionAt = SYSUTCDATETIME(),
          UpdatedAt = SYSUTCDATETIME()
        WHERE KDSOrderID = @KDSOrderID
          AND IsVoided = 0
          AND KDSStatus <> 'DONE'
      `);

    await requestOf(tx)
      .input("KDSOrderID", sql.BigInt, orderId)
      .query(`
        UPDATE dbo.KDS_OrderHeader
        SET
          OverallStatus = 'DONE',
          DoneAt = COALESCE(DoneAt, SYSUTCDATETIME()),
          UpdatedAt = SYSUTCDATETIME()
        WHERE KDSOrderID = @KDSOrderID
      `);

    await recordHistory(tx, {
      entityType: "ORDER",
      entityId: orderId,
      kdsOrderId: orderId,
      fromStatus: header.OverallStatus,
      toStatus: "DONE",
      action: "ORDER_DONE_MANUAL",
      actorName,
    });

    await tx.commit();
    return { KDSOrderID: orderId, TerminalID: header.TerminalID };
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

module.exports = {
  startSync,
  stopSync,
  syncNow,
  emitRefresh,
  getKitchenSnapshot,
  getPublicSnapshot,
  moveItemToAssembling,
  sendBackItem,
  finalizeItem,
  markItemDone,
  markOrderDone,
};