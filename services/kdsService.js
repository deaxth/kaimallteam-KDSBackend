const sql = require("mssql");
const { getSQLPool } = require("../mssql-pool-management");

const sourceCfgModule = require("../config/config");
const localCfgModule = require("../config/localConfig");

const sourceDbConfig = sourceCfgModule.dbconfig || sourceCfgModule;
const localDbConfig = localCfgModule.dbconfig || localCfgModule;

const POLL_MS = 1500;
const NEW_ORDER_LOOKBACK_MINUTES = 720;
const CANCEL_VISIBLE_SECONDS = 5;

const ARCHIVE_BATCH_SIZE = 300;
const DONE_ARCHIVE_GRACE_SECONDS = 0;
const CANCEL_ARCHIVE_GRACE_SECONDS = CANCEL_VISIBLE_SECONDS + 2;

const DEFAULT_WARNING_MINUTES = 8;
const DEFAULT_OVERDUE_MINUTES = 15;
const DEFAULT_CRITICAL_MINUTES = 20;

const CONFIGURED_WARNING_RATIO = 0.6;
const CONFIGURED_CRITICAL_RATIO = 1.35;

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

function isExcludedUOM(value) {
  const uom = normalizeUpper(value);
  return uom === "BOTTLE" || uom === "CAN";
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

function normalizeOrderType(value) {
  const raw = normalizeText(value);
  if (!raw) return null;

  const compact = normalizeUpper(raw).replace(/[\s\-_]/g, "");

  if (compact === "DINEIN") return "DINE-IN";
  if (compact === "TAKEOUT") return "TAKE-OUT";

  return raw;
}

function normalizeTerminalIds(terminalIds) {
  const result = terminalIds.length === 1 ?
      `(${terminalIds[0]})` : `(${terminalIds.join(",")})`;
  return result;
}

function parseConfiguredMinutes(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

function buildTimingProfileFromMinutes(value) {
  const configuredMinutes = parseConfiguredMinutes(value);

  if (!configuredMinutes) {
    return {
      isConfigured: false,
      mode: "default",
      targetMinutes: DEFAULT_OVERDUE_MINUTES,
      warningMinutes: DEFAULT_WARNING_MINUTES,
      overdueMinutes: DEFAULT_OVERDUE_MINUTES,
      criticalMinutes: DEFAULT_CRITICAL_MINUTES,
    };
  }

  let warningMinutes = Math.max(
    1,
    Math.floor(configuredMinutes * CONFIGURED_WARNING_RATIO)
  );

  const overdueMinutes = Math.max(1, Math.ceil(configuredMinutes));

  const criticalMinutes = Math.max(
    overdueMinutes + 1,
    Math.ceil(configuredMinutes * CONFIGURED_CRITICAL_RATIO)
  );

  if (overdueMinutes > 1 && warningMinutes >= overdueMinutes) {
    warningMinutes = overdueMinutes - 1;
  }

  return {
    isConfigured: true,
    mode: "configured",
    targetMinutes: configuredMinutes,
    warningMinutes,
    overdueMinutes,
    criticalMinutes,
  };
}

function buildOrderTimingProfile(items = []) {
  const activeItems = items.filter((item) => !item.IsVoided);

  if (!activeItems.length) {
    return buildTimingProfileFromMinutes(null);
  }

  const profiles = activeItems.map((item) =>
    buildTimingProfileFromMinutes(item.ConfiguredTimeNeededMin)
  );

  const hasConfigured = profiles.some((profile) => profile.isConfigured);
  const hasDefault = profiles.some((profile) => !profile.isConfigured);

  return {
    isConfigured: hasConfigured,
    mode: hasConfigured ? (hasDefault ? "mixed" : "configured") : "default",
    targetMinutes: Math.max(...profiles.map((profile) => profile.targetMinutes)),
    warningMinutes: Math.max(...profiles.map((profile) => profile.warningMinutes)),
    overdueMinutes: Math.max(...profiles.map((profile) => profile.overdueMinutes)),
    criticalMinutes: Math.max(...profiles.map((profile) => profile.criticalMinutes)),
  };
}

function chunkArray(values, size = 1000) {
  const chunks = [];
  for (let i = 0; i < values.length; i += size) {
    chunks.push(values.slice(i, i + size));
  }
  return chunks;
}

function areDatesEqual(a, b) {
  const left = a ? new Date(a).getTime() : null;
  const right = b ? new Date(b).getTime() : null;
  return left === right;
}

function normalizeTerminalIdList(terminalIds) {
  const values = Array.isArray(terminalIds) ? terminalIds : [terminalIds];
  return [...new Set(values.map((v) => normalizeText(v)).filter(Boolean))];
}

function buildInClause(request, values, prefix, sqlType = sql.BigInt) {
  return values
    .map((value, index) => {
      const key = `${prefix}${index}`;
      request.input(key, sqlType, value);
      return `@${key}`;
    })
    .join(", ");
}

function hasOrderHeaderChanged(localOrder, header) {
  return (
    normalizeText(localOrder.POS_No) !== normalizeText(header.POS_No) ||
    normalizeText(localOrder.TerminalID) !== normalizeText(header.TerminalID) ||
    !areDatesEqual(localOrder.DatePOS, header.DatePOS) ||
    normalizeText(localOrder.TableNo) !== normalizeText(header.TableNo) ||
    normalizeText(localOrder.CustomerRemarks) !== normalizeText(header.CustomerRemarks) ||
    normalizeText(localOrder.OrderType) !== normalizeText(normalizeOrderType(header.OrderType)) ||
    normalizeText(localOrder.LastSourceHeaderStatus) !== normalizeText(header.HeaderStatus)
  );
}

function hasItemChanged(localItem, detail) {
  const sourceStatus = normalizeText(detail.DetailStatus) || null;
  const nowVoided = isVoidedStatus(sourceStatus);

  return (
    normalizeText(localItem.ItemCode) !== normalizeText(detail.ItemCode) ||
    normalizeText(localItem.ItemName) !==
      (normalizeText(detail.ItemName) || normalizeText(detail.ItemCode)) ||
    Number(localItem.Qty || 0) !== Number(detail.QTY || 0) ||
    normalizeText(localItem.UOM) !== normalizeText(detail.UOM) ||
    normalizeText(localItem.SourceItemStatus) !== normalizeText(sourceStatus) ||
    Boolean(localItem.IsVoided) !== Boolean(nowVoided || localItem.IsVoided)
  );
}

async function getSyncAnchor(localPool) {
  const { recordset } = await localPool
    .request()
    .input("LookbackMinutes", sql.Int, NEW_ORDER_LOOKBACK_MINUTES)
    .query(`
      SELECT
        MAX(CAST(SourceTransID AS BIGINT)) AS MaxSourceTransID,
        MAX(DatePOS) AS MaxDatePOS
      FROM dbo.KDS_OrderHeader
      WHERE DatePOS >= DATEADD(MINUTE, -@LookbackMinutes, GETDATE())
         OR OverallStatus <> 'DONE'
    `);

  return {
    maxSourceTransId: recordset[0]?.MaxSourceTransID ?? null,
    maxDatePos: recordset[0]?.MaxDatePOS ?? null,
  };
}

async function fetchNewSourceHeaders(sourcePool, anchor) {
  const { recordset } = await sourcePool
    .request()
    .input("LookbackMinutes", sql.Int, NEW_ORDER_LOOKBACK_MINUTES)
    .input("MaxSourceTransID", sql.BigInt, anchor.maxSourceTransId ?? null)
    .input("AnchorDatePOS", sql.DateTime2, anchor.maxDatePos ?? null)
    .input("AnchorBufferMinutes", sql.Int, 5)
    .query(`
      SELECT
        p.TransID,
        p.POS_No,
        CAST(p.TerminalID AS NVARCHAR(50)) AS TerminalID,
        p.DatePOS,
        CAST(p.TableNo AS NVARCHAR(50)) AS TableNo,
        CAST(ISNULL(p.CustomerRemarks, '') AS NVARCHAR(500)) AS CustomerRemarks,
        CAST(ISNULL(p.OrderType, '') AS NVARCHAR(30)) AS OrderType,
        CAST(ISNULL(p.Status, '') AS NVARCHAR(50)) AS HeaderStatus
      FROM dbo.tblPOS p
      WHERE p.DatePOS >= DATEADD(MINUTE, -@LookbackMinutes, GETDATE())
        AND (
          (@MaxSourceTransID IS NULL AND @AnchorDatePOS IS NULL)
          OR p.TransID > ISNULL(@MaxSourceTransID, 0)
          OR p.DatePOS >= DATEADD(MINUTE, -@AnchorBufferMinutes, @AnchorDatePOS)
        )
      ORDER BY p.DatePOS ASC, p.TransID ASC
    `);

  return recordset || [];
}

async function fetchSourceHeadersByTransIds(sourcePool, transIds) {
  const uniqueIds = [...new Set(transIds.map((v) => Number(v)).filter(Number.isFinite))];
  if (!uniqueIds.length) return [];

  const rows = [];
  const batches = chunkArray(uniqueIds, 1000);

  for (let i = 0; i < batches.length; i++) {
    const req = sourcePool.request();
    const clause = buildInClause(req, batches[i], `HdrTrans_${i}_`, sql.BigInt);

    const { recordset } = await req.query(`
      SELECT
        p.TransID,
        p.POS_No,
        CAST(p.TerminalID AS NVARCHAR(50)) AS TerminalID,
        p.DatePOS,
        CAST(p.TableNo AS NVARCHAR(50)) AS TableNo,
        CAST(ISNULL(p.CustomerRemarks, '') AS NVARCHAR(500)) AS CustomerRemarks,
        CAST(ISNULL(p.OrderType, '') AS NVARCHAR(30)) AS OrderType,
        CAST(ISNULL(p.Status, '') AS NVARCHAR(50)) AS HeaderStatus
      FROM dbo.tblPOS p
      WHERE p.TransID IN (${clause})
    `);

    rows.push(...(recordset || []));
  }

  return rows;
}

async function fetchSourceDetailsByTransIds(sourcePool, transIds) {
  const uniqueIds = [...new Set(transIds.map((v) => Number(v)).filter(Number.isFinite))];
  if (!uniqueIds.length) return [];

  const rows = [];
  const batches = chunkArray(uniqueIds, 1000);

  for (let i = 0; i < batches.length; i++) {
    const req = sourcePool.request();
    const clause = buildInClause(req, batches[i], `DtlTrans_${i}_`, sql.BigInt);

    const { recordset } = await req.query(`
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
      WHERE d.TransID IN (${clause})
        AND UPPER(LTRIM(RTRIM(ISNULL(CAST(d.UOM AS NVARCHAR(50)), '')))) NOT IN ('BOTTLE', 'CAN')
      ORDER BY d.TransID ASC, d.RecordID ASC
    `);

    rows.push(...(recordset || []));
  }

  return rows;
}

async function getLocalOrdersBySourceTransIds(localPool, transIds) {
  const uniqueIds = [...new Set(transIds.map((v) => Number(v)).filter(Number.isFinite))];
  if (!uniqueIds.length) return new Map();

  const rows = [];
  const batches = chunkArray(uniqueIds, 1000);

  for (let i = 0; i < batches.length; i++) {
    const req = localPool.request();
    const clause = buildInClause(req, batches[i], `LocHdr_${i}_`, sql.BigInt);

    const { recordset } = await req.query(`
      SELECT *
      FROM dbo.KDS_OrderHeader
      WHERE SourceTransID IN (${clause})
    `);

    rows.push(...(recordset || []));
  }

  return new Map(rows.map((row) => [String(row.SourceTransID), row]));
}

async function getLocalItemsBySourceRecordIds(localPool, recordIds) {
  const uniqueIds = [...new Set(recordIds.map((v) => Number(v)).filter(Number.isFinite))];
  if (!uniqueIds.length) return new Map();

  const rows = [];
  const batches = chunkArray(uniqueIds, 1000);

  for (let i = 0; i < batches.length; i++) {
    const req = localPool.request();
    const clause = buildInClause(req, batches[i], `LocItem_${i}_`, sql.BigInt);

    const { recordset } = await req.query(`
      SELECT *
      FROM dbo.KDS_OrderItem
      WHERE SourceRecordID IN (${clause})
    `);

    rows.push(...(recordset || []));
  }

  return new Map(rows.map((row) => [String(row.SourceRecordID), row]));
}

async function getArchivedOrdersBySourceTransIds(localPool, transIds) {
  const uniqueIds = [...new Set(transIds.map((v) => Number(v)).filter(Number.isFinite))];
  if (!uniqueIds.length) return new Map();

  const rows = [];
  const batches = chunkArray(uniqueIds, 1000);

  for (let i = 0; i < batches.length; i++) {
    const req = localPool.request();
    const clause = buildInClause(req, batches[i], `ArcHdr_${i}_`, sql.BigInt);

    const { recordset } = await req.query(`
      SELECT *
      FROM dbo.KDS_OrderHeaderArchive
      WHERE SourceTransID IN (${clause})
    `);

    rows.push(...(recordset || []));
  }

  return new Map(rows.map((row) => [String(row.SourceTransID), row]));
}

async function getArchiveCandidateOrderIds(executor, orderIds = null, limit = ARCHIVE_BATCH_SIZE) {
  const req = requestOf(executor)
    .input("Limit", sql.Int, Math.max(1, Number(limit || ARCHIVE_BATCH_SIZE)))
    .input("DoneArchiveGraceSeconds", sql.Int, DONE_ARCHIVE_GRACE_SECONDS)
    .input("CancelArchiveGraceSeconds", sql.Int, CANCEL_ARCHIVE_GRACE_SECONDS);

  let extraWhere = "";

  const normalizedIds = Array.isArray(orderIds)
    ? [...new Set(orderIds.map((v) => Number(v)).filter(Number.isFinite))]
    : [];

  if (normalizedIds.length) {
    const clause = buildInClause(req, normalizedIds, "ArchiveOrderID_", sql.BigInt);
    extraWhere = ` AND h.KDSOrderID IN (${clause}) `;
  }

  const { recordset } = await req.query(`
    SELECT TOP (@Limit) h.KDSOrderID
    FROM dbo.KDS_OrderHeader h
    WHERE
      (
        (
          h.OverallStatus = 'DONE'
          AND DATEDIFF(
            SECOND,
            COALESCE(h.DoneAt, h.UpdatedAt, h.CreatedAt),
            GETDATE()
          ) >= @DoneArchiveGraceSeconds
        )
        OR
        (
          h.IsCancelled = 1
          AND h.CancelledAt IS NOT NULL
          AND DATEDIFF(SECOND, h.CancelledAt, GETDATE()) >= @CancelArchiveGraceSeconds
        )
      )
      ${extraWhere}
    ORDER BY COALESCE(h.DoneAt, h.CancelledAt, h.UpdatedAt, h.CreatedAt) ASC, h.KDSOrderID ASC
  `);

  return (recordset || [])
    .map((row) => Number(row.KDSOrderID))
    .filter(Number.isFinite);
}

async function archiveOrderIds(localPool, orderIds, archiveReason = "AUTO_ARCHIVE") {
  const normalizedIds = [...new Set((orderIds || []).map((v) => Number(v)).filter(Number.isFinite))];
  if (!normalizedIds.length) {
    return { archivedOrders: 0, archivedItems: 0 };
  }

  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const req = requestOf(tx)
      .input("ArchiveReason", sql.NVarChar(50), normalizeText(archiveReason) || "AUTO_ARCHIVE");

    const clause = buildInClause(req, normalizedIds, "ArcMove_", sql.BigInt);

    const result = await req.query(`
      DECLARE @OrderMap TABLE
      (
        LiveKDSOrderID BIGINT PRIMARY KEY,
        KDSOrderArchiveID BIGINT NOT NULL
      );

      INSERT INTO dbo.KDS_OrderHeaderArchive
      (
        LiveKDSOrderID,
        SourceTransID,
        POS_No,
        TerminalID,
        DatePOS,
        TableNo,
        CustomerRemarks,
        OrderType,
        OverallStatus,
        LastSourceHeaderStatus,
        StartedAt,
        PreparingAt,
        AssemblingAt,
        ServingAt,
        PickUpAt,
        DoneAt,
        CancelledAt,
        IsCancelled,
        IsDoneManually,
        CreatedAt,
        UpdatedAt,
        LastSyncedAt,
        ArchivedAt,
        ArchiveReason
      )
      SELECT
        h.KDSOrderID,
        h.SourceTransID,
        h.POS_No,
        h.TerminalID,
        h.DatePOS,
        h.TableNo,
        h.CustomerRemarks,
        h.OrderType,
        h.OverallStatus,
        h.LastSourceHeaderStatus,
        h.StartedAt,
        h.PreparingAt,
        h.AssemblingAt,
        h.ServingAt,
        h.PickUpAt,
        h.DoneAt,
        h.CancelledAt,
        h.IsCancelled,
        ISNULL(h.IsDoneManually, 0),
        h.CreatedAt,
        h.UpdatedAt,
        h.LastSyncedAt,
        GETDATE(),
        @ArchiveReason
      FROM dbo.KDS_OrderHeader h
      WHERE h.KDSOrderID IN (${clause})
        AND NOT EXISTS (
          SELECT 1
          FROM dbo.KDS_OrderHeaderArchive a
          WHERE a.SourceTransID = h.SourceTransID
        );

      INSERT INTO @OrderMap (LiveKDSOrderID, KDSOrderArchiveID)
      SELECT
        h.KDSOrderID,
        a.KDSOrderArchiveID
      FROM dbo.KDS_OrderHeader h
      INNER JOIN dbo.KDS_OrderHeaderArchive a
        ON a.SourceTransID = h.SourceTransID
      WHERE h.KDSOrderID IN (${clause});

      INSERT INTO dbo.KDS_OrderItemArchive
      (
        KDSOrderArchiveID,
        LiveKDSOrderID,
        LiveKDSItemID,
        SourceRecordID,
        SourceTransID,
        ItemCode,
        ItemName,
        Qty,
        UOM,
        KDSStatus,
        FulfillmentMode,
        SourceItemStatus,
        IsVoided,
        VoidedAt,
        LastSendBackReason,
        SendBackCount,
        StartedAt,
        PreparingAt,
        AssemblingAt,
        ServingAt,
        PickUpAt,
        DoneAt,
        LastSentBackAt,
        LastActionBy,
        LastActionAt,
        IsDoneManually,
        CreatedAt,
        UpdatedAt,
        ArchivedAt,
        ArchiveReason
      )
      SELECT
        om.KDSOrderArchiveID,
        i.KDSOrderID,
        i.KDSItemID,
        i.SourceRecordID,
        i.SourceTransID,
        i.ItemCode,
        i.ItemName,
        i.Qty,
        i.UOM,
        i.KDSStatus,
        i.FulfillmentMode,
        i.SourceItemStatus,
        i.IsVoided,
        i.VoidedAt,
        i.LastSendBackReason,
        i.SendBackCount,
        i.StartedAt,
        i.PreparingAt,
        i.AssemblingAt,
        i.ServingAt,
        i.PickUpAt,
        i.DoneAt,
        i.LastSentBackAt,
        i.LastActionBy,
        i.LastActionAt,
        ISNULL(i.IsDoneManually, 0),
        i.CreatedAt,
        i.UpdatedAt,
        GETDATE(),
        @ArchiveReason
      FROM dbo.KDS_OrderItem i
      INNER JOIN @OrderMap om
        ON om.LiveKDSOrderID = i.KDSOrderID
      WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.KDS_OrderItemArchive ia
        WHERE ia.SourceRecordID = i.SourceRecordID
      );

      SELECT COUNT(1) AS ArchivedOrders
      FROM @OrderMap;

      SELECT COUNT(1) AS ArchivedItems
      FROM dbo.KDS_OrderItemArchive ia
      INNER JOIN @OrderMap om
        ON om.KDSOrderArchiveID = ia.KDSOrderArchiveID;

      DELETE i
      FROM dbo.KDS_OrderItem i
      INNER JOIN @OrderMap om
        ON om.LiveKDSOrderID = i.KDSOrderID;

      DELETE h
      FROM dbo.KDS_OrderHeader h
      INNER JOIN @OrderMap om
        ON om.LiveKDSOrderID = h.KDSOrderID;
    `);

    await tx.commit();

    return {
      archivedOrders: Number(result.recordsets?.[0]?.[0]?.ArchivedOrders || 0),
      archivedItems: Number(result.recordsets?.[1]?.[0]?.ArchivedItems || 0),
    };
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

async function archiveNow({ orderIds = null, limit = ARCHIVE_BATCH_SIZE, reason = "AUTO_ARCHIVE" } = {}) {
  const localPool = await getLocalPool();
  const candidateOrderIds = await getArchiveCandidateOrderIds(localPool, orderIds, limit);

  if (!candidateOrderIds.length) {
    return {
      archivedOrders: 0,
      archivedItems: 0,
      scannedOrders: 0,
    };
  }

  const archived = await archiveOrderIds(localPool, candidateOrderIds, reason);

  return {
    ...archived,
    scannedOrders: candidateOrderIds.length,
  };
}

async function safeArchiveOrderIds(orderIds, reason = "AUTO_ARCHIVE") {
  const normalizedIds = [...new Set((orderIds || []).map((v) => Number(v)).filter(Number.isFinite))];
  if (!normalizedIds.length) return;

  try {
    await archiveNow({
      orderIds: normalizedIds,
      limit: normalizedIds.length,
      reason,
    });
  } catch (error) {
    console.error("[KDS archive] non-fatal order archive error:", error.message);
  }
}

async function applySourceHeadersBatch(sourcePool, localPool, headers) {
  const dedupedHeaders = [];
  const seen = new Set();

  for (const header of headers || []) {
    const key = String(header.TransID);
    if (seen.has(key)) continue;
    seen.add(key);
    dedupedHeaders.push(header);
  }

  if (!dedupedHeaders.length) return [];

  const transIds = dedupedHeaders.map((row) => row.TransID);
  const sourceDetails = await fetchSourceDetailsByTransIds(sourcePool, transIds);

  const detailsByTransId = new Map();
  for (const detail of sourceDetails) {
    const key = String(detail.TransID);
    if (!detailsByTransId.has(key)) detailsByTransId.set(key, []);
    detailsByTransId.get(key).push(detail);
  }

  const relevantHeaders = dedupedHeaders.filter(
    (header) => (detailsByTransId.get(String(header.TransID)) || []).length > 0
  );

  if (!relevantHeaders.length) return [];

  const relevantTransIds = relevantHeaders.map((row) => row.TransID);
  const archivedOrdersByTransId = await getArchivedOrdersBySourceTransIds(localPool, relevantTransIds);

  const liveHeaders = relevantHeaders.filter(
    (header) => !archivedOrdersByTransId.has(String(header.TransID))
  );

  if (!liveHeaders.length) return [];

  const liveTransIds = liveHeaders.map((row) => row.TransID);
  const liveRecordIds = liveHeaders.flatMap(
    (header) => (detailsByTransId.get(String(header.TransID)) || []).map((d) => d.RecordID)
  );

  const [localOrdersByTransId, localItemsBySourceRecordId] = await Promise.all([
    getLocalOrdersBySourceTransIds(localPool, liveTransIds),
    getLocalItemsBySourceRecordIds(localPool, liveRecordIds),
  ]);

  const touchedTerminals = new Set();

  for (const header of liveHeaders) {
    const transKey = String(header.TransID);
    const details = detailsByTransId.get(transKey) || [];
    if (!details.length) continue;

    let touched = false;
    let localOrder = localOrdersByTransId.get(transKey);

    if (!localOrder) {
      localOrder = await createOrderHeader(localPool, header);
      localOrdersByTransId.set(transKey, localOrder);
      touched = true;
    } else if (hasOrderHeaderChanged(localOrder, header)) {
      await updateOrderHeaderFromSource(localPool, localOrder.KDSOrderID, header);
      localOrder = {
        ...localOrder,
        POS_No: normalizeText(header.POS_No),
        TerminalID: normalizeText(header.TerminalID),
        DatePOS: header.DatePOS,
        TableNo: normalizeText(header.TableNo) || null,
        CustomerRemarks: normalizeText(header.CustomerRemarks) || null,
        OrderType: normalizeOrderType(header.OrderType),
        LastSourceHeaderStatus: normalizeText(header.HeaderStatus) || null,
      };
      localOrdersByTransId.set(transKey, localOrder);
      touched = true;
    }

    for (const detail of details) {
      const recordKey = String(detail.RecordID);
      const existingItem = localItemsBySourceRecordId.get(recordKey);

      if (!existingItem) {
        const createdItem = await createOrderItem(localPool, localOrder.KDSOrderID, detail);
        if (createdItem) {
          localItemsBySourceRecordId.set(recordKey, createdItem);
          touched = true;
        }
        continue;
      }

      if (hasItemChanged(existingItem, detail)) {
        await updateItemFromSource(localPool, existingItem, detail);

        localItemsBySourceRecordId.set(recordKey, {
          ...existingItem,
          ItemCode: normalizeText(detail.ItemCode),
          ItemName: normalizeText(detail.ItemName) || normalizeText(detail.ItemCode),
          Qty: Number(detail.QTY || 0),
          UOM: normalizeText(detail.UOM) || null,
          SourceItemStatus: normalizeText(detail.DetailStatus) || null,
          IsVoided: existingItem.IsVoided || isVoidedStatus(detail.DetailStatus),
        });

        touched = true;
      }
    }

    if (isCancelledStatus(header.HeaderStatus)) {
      if (!localOrder.IsCancelled) {
        await cancelOrderFromSource(localPool, localOrder, header.HeaderStatus);
        touched = true;
      }
    } else if (touched) {
      await recomputeOrderStatus(localPool, localOrder.KDSOrderID);
    }

    if (touched) {
      touchedTerminals.add(String(header.TerminalID || localOrder.TerminalID));
    }
  }

  return [...touchedTerminals];
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
        @FromStatus, @ToStatus, @Action, @Reason, @SourceStatus, @ActorName, GETDATE()
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
    .input("OrderType", sql.NVarChar(30), normalizeOrderType(header.OrderType))
    .input("OverallStatus", sql.NVarChar(20), overallStatus)
    .input("HeaderStatus", sql.NVarChar(50), normalizeText(header.HeaderStatus) || null)
    .query(`
      INSERT INTO dbo.KDS_OrderHeader
      (
        SourceTransID, POS_No, TerminalID, DatePOS, TableNo, CustomerRemarks, OrderType,
        OverallStatus, LastSourceHeaderStatus,
        StartedAt, PreparingAt, CancelledAt, IsCancelled, CreatedAt, UpdatedAt, LastSyncedAt
      )
      OUTPUT INSERTED.*
      VALUES
      (
        @SourceTransID, @POS_No, @TerminalID, @DatePOS, @TableNo, @CustomerRemarks, @OrderType,
        @OverallStatus, @HeaderStatus,
        NULL,
        CASE WHEN @OverallStatus = 'PREPARING' THEN GETDATE() ELSE NULL END,
        CASE WHEN @OverallStatus = 'CANCELLED' THEN GETDATE() ELSE NULL END,
        CASE WHEN @OverallStatus = 'CANCELLED' THEN 1 ELSE 0 END,
        GETDATE(), GETDATE(), GETDATE()
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
    .input("OrderType", sql.NVarChar(30), normalizeOrderType(header.OrderType))
    .input("HeaderStatus", sql.NVarChar(50), normalizeText(header.HeaderStatus) || null)
    .query(`
      UPDATE dbo.KDS_OrderHeader
      SET
        POS_No = @POS_No,
        TerminalID = @TerminalID,
        DatePOS = @DatePOS,
        TableNo = @TableNo,
        CustomerRemarks = @CustomerRemarks,
        OrderType = @OrderType,
        LastSourceHeaderStatus = @HeaderStatus,
        LastSyncedAt = GETDATE(),
        UpdatedAt = GETDATE()
      WHERE KDSOrderID = @KDSOrderID
    `);
}

async function createOrderItem(localPool, kdsOrderId, detail) {
  if (isExcludedUOM(detail.UOM)) {
    return null;
  }
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
        StartedAt, PreparingAt, CreatedAt, UpdatedAt
      )
      OUTPUT INSERTED.*
      VALUES
      (
        @KDSOrderID, @SourceRecordID, @SourceTransID, @ItemCode, @ItemName, @Qty, @UOM,
        'PREPARING', NULL, @SourceItemStatus, @IsVoided,
        CASE WHEN @IsVoided = 1 THEN GETDATE() ELSE NULL END,
        NULL,
        GETDATE(), GETDATE(), GETDATE()
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
        VoidedAt = CASE WHEN @NowVoided = 1 AND VoidedAt IS NULL THEN GETDATE() ELSE VoidedAt END,
        UpdatedAt = GETDATE()
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
          AssemblingAt = CASE WHEN @NextStatus = 'ASSEMBLING' AND AssemblingAt IS NULL THEN GETDATE() ELSE AssemblingAt END,
          ServingAt = CASE WHEN @NextStatus = 'SERVING' AND ServingAt IS NULL THEN GETDATE() ELSE ServingAt END,
          PickUpAt = CASE WHEN @NextStatus = 'PICKUP' AND PickUpAt IS NULL THEN GETDATE() ELSE PickUpAt END,
          DoneAt = CASE WHEN @NextStatus = 'DONE' AND DoneAt IS NULL THEN GETDATE() ELSE DoneAt END,
          UpdatedAt = GETDATE()
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
        CancelledAt = COALESCE(CancelledAt, GETDATE()),
        UpdatedAt = GETDATE(),
        LastSyncedAt = GETDATE()
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
        CAST(ISNULL(p.OrderType, '') AS NVARCHAR(30)) AS OrderType,
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
        AND UPPER(LTRIM(RTRIM(ISNULL(CAST(d.UOM AS NVARCHAR(50)), '')))) NOT IN ('BOTTLE', 'CAN')
      ORDER BY d.RecordID ASC
    `);

  return recordset;
}

async function upsertSourceOrder(sourcePool, localPool, header) {
  const details = await fetchSourceDetails(sourcePool, header.TransID);

  // If after filtering there are no KDS-relevant items, do not record/display this transaction.
  if (!details.length) {
    return null;
  }

  let localOrder = await getOrderHeaderBySourceTransId(localPool, header.TransID);

  if (!localOrder) {
    localOrder = await createOrderHeader(localPool, header);
  } else {
    await updateOrderHeaderFromSource(localPool, localOrder.KDSOrderID, header);
    localOrder = await getOrderHeaderBySourceTransId(localPool, header.TransID);
  }

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

async function refreshActiveOrders(sourcePool, localPool) {
  const activeOrdersRs = await localPool.request().query(`
    SELECT SourceTransID
    FROM dbo.KDS_OrderHeader
    WHERE OverallStatus <> 'DONE'
      AND IsCancelled = 0
  `);

  const activeTransIds = [...new Set(
    (activeOrdersRs.recordset || [])
      .map((row) => Number(row.SourceTransID))
      .filter(Number.isFinite)
  )];

  if (!activeTransIds.length) return [];

  const headers = await fetchSourceHeadersByTransIds(sourcePool, activeTransIds);
  return applySourceHeadersBatch(sourcePool, localPool, headers);
}

async function getKnownSourceTransIds(localPool) {
  const { recordset } = await localPool.request()
    .input("LookbackMinutes", sql.Int, NEW_ORDER_LOOKBACK_MINUTES)
    .query(`
      SELECT SourceTransID
      FROM dbo.KDS_OrderHeader
      WHERE DatePOS >= DATEADD(MINUTE, -@LookbackMinutes, GETDATE())
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
    const touchedTerminals = new Set();

    const anchor = await getSyncAnchor(localPool);
    const newHeaders = await fetchNewSourceHeaders(sourcePool, anchor);
    const newTouched = await applySourceHeadersBatch(sourcePool, localPool, newHeaders);
    newTouched.forEach((id) => touchedTerminals.add(String(id)));

    const activeTouched = await refreshActiveOrders(sourcePool, localPool);
    activeTouched.forEach((id) => touchedTerminals.add(String(id)));

    const archiveResult = await archiveNow({
      limit: ARCHIVE_BATCH_SIZE,
      reason: "AUTO_ARCHIVE",
    });

    touchedTerminals.forEach((terminalId) => {
      emitRefresh(io, terminalId, "sync-complete");
    });

    return {
      ok: true,
      touchedTerminalCount: touchedTerminals.size,
      archivedOrders: archiveResult.archivedOrders,
      archivedItems: archiveResult.archivedItems,
    };
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
        OrderType: row.OrderType,
        OverallStatus: row.OverallStatus,
        IsCancelled: !!row.IsCancelled,
        CancelledAt: row.CancelledAt,
        StartedAt: row.OrderStartedAt,
        items: [],
      });
    }

    if (row.KDSItemID) {
      const configuredTimeNeededMin =
        row.ConfiguredTimeNeededMin !== null &&
        row.ConfiguredTimeNeededMin !== undefined &&
        String(row.ConfiguredTimeNeededMin).trim() !== ""
          ? Number(row.ConfiguredTimeNeededMin)
          : null;

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
        StartedAt: row.ItemStartedAt,
        ConfiguredTimeNeededMin: configuredTimeNeededMin,
        Timing: buildTimingProfileFromMinutes(configuredTimeNeededMin),
      });
    }
  }

  for (const order of byOrder.values()) {
    order.Timing = buildOrderTimingProfile(order.items);
  }

  return [...byOrder.values()];
}

async function getKitchenSnapshot({ terminalId } = {}) {
  const localPool = await getLocalPool();
  const req = localPool.request();

  let where = `
    WHERE h.OverallStatus <> 'DONE'
      AND (h.IsCancelled = 0 OR DATEDIFF(SECOND, h.CancelledAt, GETDATE()) <= ${CANCEL_VISIBLE_SECONDS})
  `;

  if (terminalId) {
    req.input("TerminalID", sql.NVarChar(50), String(terminalId));
    where += ` AND h.TerminalID = @TerminalID`;
  }

  const { recordset } = await req.query(`
    ;WITH CookingTime AS (
      SELECT
        ItemCodeNorm = LTRIM(RTRIM(CAST(ct.ItemCode AS NVARCHAR(50)))),
        TimeNeededMinutes = MAX(
          CASE
            WHEN TRY_CONVERT(DECIMAL(10, 2), NULLIF(LTRIM(RTRIM(CAST(ct.TimeNeeded AS NVARCHAR(50)))), '')) > 0
              THEN TRY_CONVERT(DECIMAL(10, 2), NULLIF(LTRIM(RTRIM(CAST(ct.TimeNeeded AS NVARCHAR(50)))), ''))
            ELSE NULL
          END
        )
      FROM dbo.CookingTimeItemMainte ct
      GROUP BY LTRIM(RTRIM(CAST(ct.ItemCode AS NVARCHAR(50))))
    )
    SELECT
      h.KDSOrderID,
      h.SourceTransID,
      h.POS_No,
      h.TerminalID,
      h.DatePOS,
      h.TableNo,
      h.CustomerRemarks,
      h.OrderType,
      h.OverallStatus,
      h.IsCancelled,
      h.CancelledAt,
      h.StartedAt AS OrderStartedAt,
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
      i.SendBackCount,
      i.StartedAt AS ItemStartedAt,
      CASE
        WHEN ctm.TimeNeededMinutes IS NOT NULL AND ctm.TimeNeededMinutes > 0
          THEN ctm.TimeNeededMinutes
        ELSE NULL
      END AS ConfiguredTimeNeededMin
    FROM dbo.KDS_OrderHeader h
    LEFT JOIN dbo.KDS_OrderItem i
      ON i.KDSOrderID = h.KDSOrderID
    LEFT JOIN CookingTime ctm
      ON ctm.ItemCodeNorm = LTRIM(RTRIM(CAST(i.ItemCode AS NVARCHAR(50))))
    ${where}
    ORDER BY h.DatePOS ASC, h.KDSOrderID ASC, i.SourceRecordID ASC
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
      h.OrderType,
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
      OrderType: row.OrderType,
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
        i.StartedAt AS ItemStartedAt,
        o.TerminalID,
        o.IsCancelled,
        o.OverallStatus,
        o.StartedAt AS OrderStartedAt
      FROM dbo.KDS_OrderItem i
      INNER JOIN dbo.KDS_OrderHeader o
        ON o.KDSOrderID = i.KDSOrderID
      WHERE i.KDSItemID = @KDSItemID
    `);

  return recordset[0] || null;
}

async function startPreparingItem(itemId, actorName) {
  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const item = await getItemContext(tx, itemId);
    if (!item) throw new Error("Item not found.");
    if (item.IsVoided) throw new Error("Voided item cannot be updated.");
    if (item.IsCancelled) throw new Error("Cancelled order cannot be updated.");
    if (item.KDSStatus !== "PREPARING") throw new Error("Only PREPARING items can be started.");

    const itemWasNotStarted = !item.ItemStartedAt;
    const orderWasNotStarted = !item.OrderStartedAt;

    await requestOf(tx)
      .input("KDSItemID", sql.BigInt, itemId)
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        UPDATE dbo.KDS_OrderItem
        SET
          StartedAt = COALESCE(StartedAt, GETDATE()),
          LastActionBy = @ActorName,
          LastActionAt = GETDATE(),
          UpdatedAt = GETDATE()
        WHERE KDSItemID = @KDSItemID
      `);

    await requestOf(tx)
      .input("KDSOrderID", sql.BigInt, item.KDSOrderID)
      .query(`
        UPDATE dbo.KDS_OrderHeader
        SET
          StartedAt = COALESCE(StartedAt, GETDATE()),
          UpdatedAt = GETDATE()
        WHERE KDSOrderID = @KDSOrderID
      `);

    if (itemWasNotStarted) {
      await recordHistory(tx, {
        entityType: "ITEM",
        entityId: itemId,
        kdsOrderId: item.KDSOrderID,
        kdsItemId: itemId,
        fromStatus: item.KDSStatus,
        toStatus: item.KDSStatus,
        action: "ITEM_START",
        actorName,
      });
    }

    if (orderWasNotStarted) {
      await recordHistory(tx, {
        entityType: "ORDER",
        entityId: item.KDSOrderID,
        kdsOrderId: item.KDSOrderID,
        fromStatus: item.OverallStatus,
        toStatus: item.OverallStatus,
        action: "ORDER_TIMER_START",
        actorName,
      });
    }

    await tx.commit();
    return { KDSOrderID: item.KDSOrderID, TerminalID: item.TerminalID };
  } catch (error) {
    await tx.rollback();
    throw error;
  }
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
    if (!item.ItemStartedAt) throw new Error("Item must be started first.");

    await requestOf(tx)
      .input("KDSItemID", sql.BigInt, itemId)
      .input("ActorName", sql.NVarChar(100), actorName || null)
      .query(`
        UPDATE dbo.KDS_OrderItem
        SET
          KDSStatus = 'ASSEMBLING',
          AssemblingAt = COALESCE(AssemblingAt, GETDATE()),
          LastActionBy = @ActorName,
          LastActionAt = GETDATE(),
          UpdatedAt = GETDATE()
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
          LastSentBackAt = GETDATE(),
          LastActionBy = @ActorName,
          LastActionAt = GETDATE(),
          UpdatedAt = GETDATE()
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
          ServingAt = CASE WHEN @NextStatus = 'SERVING' AND ServingAt IS NULL THEN GETDATE() ELSE ServingAt END,
          PickUpAt = CASE WHEN @NextStatus = 'PICKUP' AND PickUpAt IS NULL THEN GETDATE() ELSE PickUpAt END,
          LastActionBy = @ActorName,
          LastActionAt = GETDATE(),
          UpdatedAt = GETDATE()
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
          DoneAt = COALESCE(DoneAt, GETDATE()),
          LastActionBy = @ActorName,
          LastActionAt = GETDATE(),
          UpdatedAt = GETDATE()
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

    const result = { KDSOrderID: item.KDSOrderID, TerminalID: item.TerminalID };
    await safeArchiveOrderIds([item.KDSOrderID], "DONE_AUTO");

    return result;

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
          i.KDSStatus, 'DONE', 'ORDER_DONE_MANUAL', NULL, NULL, @ActorName, GETDATE()
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
          DoneAt = COALESCE(DoneAt, GETDATE()),
          LastActionBy = @ActorName,
          LastActionAt = GETDATE(),
          UpdatedAt = GETDATE()
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
          DoneAt = COALESCE(DoneAt, GETDATE()),
          UpdatedAt = GETDATE()
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

    const result = { KDSOrderID: orderId, TerminalID: header.TerminalID };
    await safeArchiveOrderIds([orderId], "DONE_MANUAL");

    return result;
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

async function doneAllItems(terminalIds, actor) {
  const normalizedTerminalIds = normalizeTerminalIdList(terminalIds);
  if (!normalizedTerminalIds.length) {
    throw new Error("At least one terminal ID is required.");
  }

  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();

  try {
    const terminalReq = requestOf(tx);
    const terminalClause = buildInClause(
      terminalReq,
      normalizedTerminalIds,
      "TerminalID_",
      sql.NVarChar(50)
    );

    const orderDataRs = await terminalReq
      .input("Actor", sql.NVarChar(100), actor || null)
      .query(`
        SELECT
          h.KDSOrderID,
          h.TerminalID,
          h.OverallStatus
        FROM dbo.KDS_OrderHeader h
        WHERE h.TerminalID IN (${terminalClause})
          AND h.OverallStatus <> 'DONE';

        SELECT
          i.KDSItemID,
          i.KDSOrderID,
          i.KDSStatus
        FROM dbo.KDS_OrderItem i
        INNER JOIN dbo.KDS_OrderHeader h
          ON h.KDSOrderID = i.KDSOrderID
        WHERE h.TerminalID IN (${terminalClause})
          AND i.IsVoided = 0
          AND i.KDSStatus <> 'DONE';
      `);

    const orderHeaders = orderDataRs.recordsets?.[0] || [];
    const orderItems = orderDataRs.recordsets?.[1] || [];

    if (!orderHeaders.length && !orderItems.length) {
      await tx.commit();
      return {
        message: "Nothing to update.",
        terminalIds: normalizedTerminalIds,
      };
    }

    const historyReq1 = requestOf(tx);
    const historyClause1 = buildInClause(
      historyReq1,
      normalizedTerminalIds,
      "HistTerminalA_",
      sql.NVarChar(50)
    );

    await historyReq1
      .input("Actor", sql.NVarChar(100), actor || null)
      .query(`
        INSERT INTO dbo.KDS_StatusHistory
        (
          EntityType, EntityID, KDSOrderID, KDSItemID,
          FromStatus, ToStatus, Action, Reason, SourceStatus, ActorName, CreatedAt
        )
        SELECT
          'ORDER',
          h.KDSOrderID,
          h.KDSOrderID,
          NULL,
          h.OverallStatus,
          'DONE',
          'ORDER_DONE_MANUAL',
          NULL,
          NULL,
          @Actor,
          GETDATE()
        FROM dbo.KDS_OrderHeader h
        WHERE h.TerminalID IN (${historyClause1})
          AND h.OverallStatus <> 'DONE';

        INSERT INTO dbo.KDS_StatusHistory
        (
          EntityType, EntityID, KDSOrderID, KDSItemID,
          FromStatus, ToStatus, Action, Reason, SourceStatus, ActorName, CreatedAt
        )
        SELECT
          'ITEM',
          i.KDSItemID,
          i.KDSOrderID,
          i.KDSItemID,
          i.KDSStatus,
          'DONE',
          'ORDER_DONE_MANUAL',
          NULL,
          NULL,
          @Actor,
          GETDATE()
        FROM dbo.KDS_OrderItem i
        INNER JOIN dbo.KDS_OrderHeader h
          ON h.KDSOrderID = i.KDSOrderID
        WHERE h.TerminalID IN (${historyClause1})
          AND i.IsVoided = 0
          AND i.KDSStatus <> 'DONE';
      `);

    const updateReq = requestOf(tx);
    const updateClause = buildInClause(
      updateReq,
      normalizedTerminalIds,
      "UpdTerminal_",
      sql.NVarChar(50)
    );

    await updateReq
      .input("Actor", sql.NVarChar(100), actor || null)
      .query(`
        UPDATE i
        SET
          i.KDSStatus = 'DONE',
          i.DoneAt = COALESCE(i.DoneAt, GETDATE()),
          i.UpdatedAt = GETDATE(),
          i.LastActionAt = GETDATE(),
          i.IsDoneManually = 1,
          i.LastActionBy = @Actor
        FROM dbo.KDS_OrderItem i
        INNER JOIN dbo.KDS_OrderHeader h
          ON h.KDSOrderID = i.KDSOrderID
        WHERE h.TerminalID IN (${updateClause})
          AND i.IsVoided = 0
          AND i.KDSStatus <> 'DONE';

        UPDATE h
        SET
          h.OverallStatus = 'DONE',
          h.IsDoneManually = 1,
          h.UpdatedAt = GETDATE(),
          h.DoneAt = COALESCE(h.DoneAt, GETDATE())
        FROM dbo.KDS_OrderHeader h
        WHERE h.TerminalID IN (${updateClause})
          AND h.OverallStatus <> 'DONE';
      `);

    await tx.commit();

    const result = {
      message: "Success.",
      terminalIds: normalizedTerminalIds,
      affectedOrders: orderHeaders.length,
      affectedItems: orderItems.length,
    };

    await safeArchiveOrderIds(
      orderHeaders.map((row) => row.KDSOrderID),
      "DONE_MANUAL_BATCH"
    );

    return result;
  } catch (err) {
    await tx.rollback();
    throw err;
  }
}

module.exports = {
  startSync,
  stopSync,
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
  doneAllItems
};