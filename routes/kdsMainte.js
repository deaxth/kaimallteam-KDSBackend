const express = require('express');
const router = express.Router();
const sql = require('mssql');
const localConfig = require('../config/localConfig');
const { getSQLPool } = require('../mssql-pool-management');

const successMesage = 'Success.'
const internalError = 'Something went wrong. Please contact support.';

router.get('/get-item-mainte', async(req, res) => {
  const pool = await getSQLPool(localConfig);
  try {
    const getResult = await pool.request()
      .query(`
        SELECT ct.ItemCode, ct.TimeNeeded, ct.Station, ct.WhoCreated, ct.DateCreated FROM CookingTimeItemMainte ct
          LEFT JOIN [JADE_LINK].JADE_01.dbo.tblItem_Master im
        ON ct.ItemCode = im.ItemCode
          LEFT JOIN HelpdeskDB.dbo.Users u
        ON ct.WhoCreated = u.UserId
      `);
    const items = getResult.recordset;
    res.json({ items: items });
  } catch(err) {
    res.status(500).json({ message: internalError });
    console.log(err);
  } 
});

router.get('/get-items', async(req, res) => {
  const pool = await getSQLPool(localConfig);
  try {
    const getResult = await pool.request()
      .query(`
        SELECT ItemName, ItemCode FROM [JADE_LINK].JADE_01.dbo.tblItem_Master WHERE isSales = 1 AND Status = 'Active'
      `);
    const items = getResult.recordset;
    res.json({ items: items });
  } catch(err) {
    res.status(500).json({ message: internalError });
    console.log(err);
  } 
});

router.post('/add-cooking-time', async(req, res) => {
  const { itemsToUpload } = req.body;
  const userId = 1;
  const pool = await getSQLPool(localConfig);
  const tx = new sql.Transaction(pool);
  await tx.begin();
  try { 
    for (const item of itemsToUpload) {
      const results = await new sql.Request(tx)
        .input('ItemCode', sql.VarChar, item.ItemCode)
        .query('SELECT ItemCode, TimeNeeded FROM CookingTimeItemMainte WHERE ItemCode = @ItemCode');
      const itemCode = results.recordset[0]?.ItemCode;
      if (!itemCode) {
        await new sql.Request(tx)
        .input('ItemCode', sql.VarChar, item.ItemCode)
        .input('TimeNeeded', sql.Int, item.TimeNeeded)
        .input('Station', sql.Int, Number(item.Station))
        .input('WhoCreated', sql.Int, userId)
        .query(`
          INSERT INTO CookingTimeItemMainte
            (ItemCode, TimeNeeded, Station, WhoCreated)
          VALUES
            (@ItemCode, @TimeNeeded, @Station, @WhoCreated);
        `);
      }
    };

    await tx.commit();
    res.json({ message: successMesage });
  } catch(err) {
    console.log(err);
    await tx.rollback();
    res.status(500).json({ message: internalError })
  }
});

router.post('/edit-cooking-time', async(req, res) => {
  const { itemsToUpload } = req.body;
  const userId = 1;
  const pool = await getSQLPool(localConfig);
  const tx = new sql.Transaction(pool);
  await tx.begin();
  try {
    let itemsToChange = [];

    for (const item of itemsToUpload) {
      const results = await new sql.Request(tx)
        .input('ItemCode', sql.VarChar, item.ItemCode)
        .query('SELECT ItemCode, TimeNeeded, Station FROM CookingTimeItemMainte WHERE ItemCode = @ItemCode');
        const currentRow = results.recordset[0];
        const timeNeeded = currentRow?.TimeNeeded;
        const station = currentRow?.Station;
        
        const newValues = {
          itemCode: item.ItemCode,
          newTime: item.TimeNeeded,
          station: Number(item.Station),
        };
        
        const timeChanged =
          parseInt(item.TimeNeeded, 10) !== parseInt(timeNeeded, 10);
        
        const stationChanged =
          parseInt(item.Station, 10) !== parseInt(station, 10);
        
        if (timeChanged || stationChanged) {
          itemsToChange.push(newValues);
        }
    };

    for (const item of itemsToUpload) {
      await new sql.Request(tx)
        .input('ItemCode', sql.VarChar, item.ItemCode)
        .input('TimeNeeded', sql.Int, item.TimeNeeded)
        .input('Station', sql.Int, Number(item.Station))
        .input('WhoModified', sql.Int, userId)
        .query(`
          UPDATE CookingTimeItemMainte
          SET TimeNeeded = @TimeNeeded,
              ModifiedBy = @WhoModified,
              Station = @Station,
              DateModified = GETDATE()
          WHERE ItemCode = @ItemCode;
        `);
    };
    const responseMessage = itemsToChange.length === 0 ? 'No changes made.' : successMesage;

    await tx.commit();
    res.json({ message: responseMessage });
  } catch(err) {
    console.log(err);
    await tx.rollback();
    res.status(500).json({ message: internalError })
  }
});

router.post("/upload-cooking-time", async (req, res) => {
  const rawItems = Array.isArray(req.body?.itemsToUpload)
    ? req.body.itemsToUpload
    : [];

  const userId = 1;
  const pool = await getSQLPool(localConfig);
  const tx = new sql.Transaction(pool);

  function cleanText(value) {
    return String(value ?? "").trim();
  }

  function normalizeKey(value) {
    return cleanText(value).toUpperCase();
  }

  function parseTimeNeeded(value) {
    if (value === undefined || value === null) return null;
    const text = String(value).trim().replace(/,/g, "");
    if (!text) return null;

    const num = Number(text);
    if (!Number.isFinite(num)) return null;

    return Math.max(0, Math.round(num));
  }

  function parseStation(value) {
    if (value === undefined || value === null) return null;

    const text = String(value).trim();
    if (!/^[1-3]$/.test(text)) return null;

    return Number(text);
  }

  if (!rawItems.length) {
    return res.status(400).json({ message: "No items found in upload." });
  }

  const invalidRows = [];
  const dedupedMap = new Map();

  rawItems.forEach((row, index) => {
    const itemCode = cleanText(row?.ItemCode);
    const timeNeeded = parseTimeNeeded(row?.TimeNeeded);
    const station = parseStation(row?.Station);

    if (!itemCode) {
      invalidRows.push({
        row: index + 1,
        ItemCode: "",
        reason: "ItemCode is required.",
      });
      return;
    }

    if (timeNeeded === null) {
      invalidRows.push({
        row: index + 1,
        ItemCode: itemCode,
        reason: "TimeNeeded must be a valid whole number.",
      });
      return;
    }

    if (station === null) {
      invalidRows.push({
        row: index + 1,
        ItemCode: itemCode,
        reason: "Station must be a single digit from 1 to 3.",
      });
      return;
    }

    dedupedMap.set(normalizeKey(itemCode), {
      ItemCode: itemCode,
      TimeNeeded: timeNeeded,
      Station: station,
    });
  });

  const itemsToProcess = Array.from(dedupedMap.values());

  if (!itemsToProcess.length) {
    return res.status(400).json({
      message: "No valid rows to upload.",
      invalidRows,
    });
  }

  await tx.begin();

  try {
    const itemMasterResult = await new sql.Request(tx).query(`
      SELECT ItemCode, ItemName
      FROM [JADE_LINK].JADE_01.dbo.tblItem_Master
      WHERE isSales = 1
        AND Status = 'Active'
    `);

    const activeItemMap = new Map(
      itemMasterResult.recordset.map((row) => [
        normalizeKey(row.ItemCode),
        {
          ItemCode: cleanText(row.ItemCode),
          ItemName: cleanText(row.ItemName),
        },
      ])
    );

    const existingResult = await new sql.Request(tx).query(`
      SELECT ItemCode, TimeNeeded, Station
      FROM CookingTimeItemMainte
    `);

    const existingMap = new Map(
      existingResult.recordset.map((row) => [
        normalizeKey(row.ItemCode),
        {
          TimeNeeded: parseInt(row.TimeNeeded, 10),
          Station: row.Station === null || row.Station === undefined
            ? null
            : parseInt(row.Station, 10),
        },
      ])
    );

    let created = 0;
    let updated = 0;
    let unchanged = 0;
    const skippedItems = [...invalidRows];

    for (const item of itemsToProcess) {
      const key = normalizeKey(item.ItemCode);

      if (!activeItemMap.has(key)) {
        skippedItems.push({
          ItemCode: item.ItemCode,
          reason: "ItemCode not found in active sales items.",
        });
        continue;
      }

      const existing = existingMap.get(key);

      if (existing === undefined) {
        await new sql.Request(tx)
          .input("ItemCode", sql.VarChar, item.ItemCode)
          .input("TimeNeeded", sql.Int, item.TimeNeeded)
          .input("Station", sql.Int, item.Station)
          .input("WhoCreated", sql.Int, userId)
          .query(`
            INSERT INTO CookingTimeItemMainte
              (ItemCode, TimeNeeded, Station, WhoCreated)
            VALUES
              (@ItemCode, @TimeNeeded, @Station, @WhoCreated);
          `);

        created += 1;
        existingMap.set(key, {
          TimeNeeded: item.TimeNeeded,
          Station: item.Station,
        });
        continue;
      }

      const sameTime =
        parseInt(existing.TimeNeeded, 10) === parseInt(item.TimeNeeded, 10);
      const sameStation =
        parseInt(existing.Station, 10) === parseInt(item.Station, 10);

      if (sameTime && sameStation) {
        unchanged += 1;
        continue;
      }

      await new sql.Request(tx)
        .input("ItemCode", sql.VarChar, item.ItemCode)
        .input("TimeNeeded", sql.Int, item.TimeNeeded)
        .input("Station", sql.Int, item.Station)
        .input("WhoModified", sql.Int, userId)
        .query(`
          UPDATE CookingTimeItemMainte
          SET TimeNeeded = @TimeNeeded,
              Station = @Station,
              ModifiedBy = @WhoModified,
              DateModified = GETDATE()
          WHERE ItemCode = @ItemCode;
        `);

      updated += 1;
      existingMap.set(key, {
        TimeNeeded: item.TimeNeeded,
        Station: item.Station,
      });
    }

    await tx.commit();

    const hasChanges = created > 0 || updated > 0;
    const message = hasChanges
      ? "Cooking time upload completed."
      : "No changes made.";

    return res.json({
      message,
      created,
      updated,
      unchanged,
      skippedItems,
    });
  } catch (err) {
    console.log(err);
    await tx.rollback();
    return res.status(500).json({ message: internalError });
  }
});

router.get("/summary-per-transaction", async (req, res) => {
  const pool = await getSQLPool(localConfig);

  try {
    const type = String(req.query.type || "details").trim().toLowerCase();
    const terminalIDRaw = String(req.query.terminalID ?? "").trim();
    const orderType = String(req.query.orderType ?? "").trim();
    const dateFrom = String(req.query.dateFrom ?? "").trim();
    const dateTo = String(req.query.dateTo ?? "").trim();

    const hasTerminalID =
      terminalIDRaw !== "" && Number.isFinite(Number(terminalIDRaw));
    const terminalID = hasTerminalID ? Number(terminalIDRaw) : null;

    const request = pool.request();
    if (hasTerminalID) request.input("terminalID", sql.Int, terminalID);
    if (orderType) request.input("orderType", sql.VarChar(50), orderType);
    if (dateFrom) request.input("dateFrom", sql.Date, dateFrom);
    if (dateTo) request.input("dateTo", sql.Date, dateTo);

    const filters = [`H.isDoneManually = 0`];
    if (hasTerminalID) filters.push(`H.TerminalID = @terminalID`);
    if (orderType) filters.push(`H.OrderType = @orderType`);
    if (dateFrom) filters.push(`CAST(H.PreparingAt AS date) >= @dateFrom`);
    if (dateTo) filters.push(`CAST(H.PreparingAt AS date) <= @dateTo`);

    const whereSql = `WHERE ${filters.join(" AND ")}`;

    const baseCte = `
      ;WITH base AS (
        SELECT
          H.POS_No,
          H.TerminalID,
          H.OrderType,
          H.PreparingAt,
          H.StartedAt,
          H.AssemblingAt,
          H.PickUpAt,
          H.DoneAt,

          CAST(
            CASE
              WHEN H.PreparingAt IS NOT NULL AND H.StartedAt IS NOT NULL
              THEN DATEDIFF(SECOND, H.PreparingAt, H.StartedAt) / 60.0
            END
          AS DECIMAL(10,2)) AS TimeBeforeStarting,

          CAST(
            CASE
              WHEN H.StartedAt IS NOT NULL AND H.AssemblingAt IS NOT NULL
              THEN DATEDIFF(SECOND, H.StartedAt, H.AssemblingAt) / 60.0
            END
          AS DECIMAL(10,2)) AS KitchenAverageTime,

          CAST(
            CASE
              WHEN H.AssemblingAt IS NOT NULL AND H.PickUpAt IS NOT NULL
              THEN DATEDIFF(SECOND, H.AssemblingAt, H.PickUpAt) / 60.0
            END
          AS DECIMAL(10,2)) AS AssemblerAverageTime,

          CAST(
            CASE
              WHEN H.PickUpAt IS NOT NULL AND H.DoneAt IS NOT NULL
              THEN DATEDIFF(SECOND, H.PickUpAt, H.DoneAt) / 60.0
            END
          AS DECIMAL(10,2)) AS LastUpdateTime,

          CAST(
            CASE
              WHEN H.PreparingAt IS NOT NULL AND H.DoneAt IS NOT NULL
              THEN DATEDIFF(SECOND, H.PreparingAt, H.DoneAt) / 60.0
            END
          AS DECIMAL(10,2)) AS TotalTimeBeforeDone
        FROM KDS_OrderHeaderArchive H
        ${whereSql}
      )
    `;

    let q = "";

    if (type === "summary") {
      q = `
        ${baseCte}
        SELECT
          TerminalID,
          COUNT(*) AS TransactionCount,
          SUM(CASE WHEN UPPER(LTRIM(RTRIM(ISNULL(OrderType, '')))) = 'DINE-IN' THEN 1 ELSE 0 END) AS DineInCount,
          SUM(CASE WHEN UPPER(LTRIM(RTRIM(ISNULL(OrderType, '')))) IN ('TAKE-OUT', 'TAKE OUT') THEN 1 ELSE 0 END) AS TakeOutCount,

          CAST(AVG(TimeBeforeStarting) AS DECIMAL(10,2)) AS TimeBeforeStarting,
          CAST(AVG(KitchenAverageTime) AS DECIMAL(10,2)) AS KitchenAverageTime,
          CAST(AVG(AssemblerAverageTime) AS DECIMAL(10,2)) AS AssemblerAverageTime,
          CAST(AVG(LastUpdateTime) AS DECIMAL(10,2)) AS LastUpdateTime,
          CAST(AVG(TotalTimeBeforeDone) AS DECIMAL(10,2)) AS TotalTimeBeforeDone,

          CAST(MIN(TotalTimeBeforeDone) AS DECIMAL(10,2)) AS FastestTotalTime,
          CAST(MAX(TotalTimeBeforeDone) AS DECIMAL(10,2)) AS SlowestTotalTime
        FROM base
        GROUP BY TerminalID
        ORDER BY TotalTimeBeforeDone DESC, TerminalID ASC
      `;
    } else {
      q = `
        ${baseCte}
        SELECT
          POS_No,
          TerminalID,
          OrderType,
          PreparingAt,
          StartedAt,
          AssemblingAt,
          PickUpAt,
          DoneAt,
          TimeBeforeStarting,
          KitchenAverageTime,
          AssemblerAverageTime,
          LastUpdateTime,
          TotalTimeBeforeDone
        FROM base
        ORDER BY PreparingAt DESC, POS_No DESC
      `;
    }

    const result = await request.query(q);
    res.json({ success: true, data: result.recordset });
  } catch (err) {
    console.log(err);
    res.status(500).json({ success: false, message: internalError });
  }
});

router.get("/summary-per-items", async (req, res) => {
  const pool = await getSQLPool(localConfig);

  try {
    const dateFrom = String(req.query.dateFrom ?? "").trim();
    const dateTo = String(req.query.dateTo ?? "").trim();

    const request = pool.request();
    if (dateFrom) request.input("dateFrom", sql.Date, dateFrom);
    if (dateTo) request.input("dateTo", sql.Date, dateTo);

    const filters = [`I.isDoneManually = 0`];
    if (dateFrom) filters.push(`CAST(I.PreparingAt AS date) >= @dateFrom`);
    if (dateTo) filters.push(`CAST(I.PreparingAt AS date) <= @dateTo`);

    const whereSql = `WHERE ${filters.join(" AND ")}`;

    const q = `
      ;WITH base AS (
        SELECT
          I.ItemCode,
          I.ItemName,

          CAST(
            CASE
              WHEN I.PreparingAt IS NOT NULL AND I.StartedAt IS NOT NULL
              THEN DATEDIFF(SECOND, I.PreparingAt, I.StartedAt) / 60.0
            END
          AS DECIMAL(10,2)) AS TimeBeforeStarting,

          CAST(
            CASE
              WHEN I.StartedAt IS NOT NULL AND I.AssemblingAt IS NOT NULL
              THEN DATEDIFF(SECOND, I.StartedAt, I.AssemblingAt) / 60.0
            END
          AS DECIMAL(10,2)) AS KitchenAverageTime,

          CAST(
            CASE
              WHEN I.AssemblingAt IS NOT NULL AND I.PickUpAt IS NOT NULL
              THEN DATEDIFF(SECOND, I.AssemblingAt, I.PickUpAt) / 60.0
            END
          AS DECIMAL(10,2)) AS AssemblerAverageTime,

          CAST(
            CASE
              WHEN I.PickUpAt IS NOT NULL AND I.DoneAt IS NOT NULL
              THEN DATEDIFF(SECOND, I.PickUpAt, I.DoneAt) / 60.0
            END
          AS DECIMAL(10,2)) AS LastUpdateTime,

          CAST(
            CASE
              WHEN I.PreparingAt IS NOT NULL AND I.DoneAt IS NOT NULL
              THEN DATEDIFF(SECOND, I.PreparingAt, I.DoneAt) / 60.0
            END
          AS DECIMAL(10,2)) AS TotalTimeBeforeDone
        FROM KDS_OrderItemArchive I
        ${whereSql}
      )
      SELECT
        ItemCode,
        ItemName,
        COUNT(*) AS ItemCount,
        CAST(AVG(TimeBeforeStarting) AS DECIMAL(10,2)) AS TimeBeforeStarting,
        CAST(AVG(KitchenAverageTime) AS DECIMAL(10,2)) AS KitchenAverageTime,
        CAST(AVG(AssemblerAverageTime) AS DECIMAL(10,2)) AS AssemblerAverageTime,
        CAST(AVG(LastUpdateTime) AS DECIMAL(10,2)) AS LastUpdateTime,
        CAST(AVG(TotalTimeBeforeDone) AS DECIMAL(10,2)) AS TotalTimeBeforeDone,
        CAST(MIN(TotalTimeBeforeDone) AS DECIMAL(10,2)) AS FastestTotalTime,
        CAST(MAX(TotalTimeBeforeDone) AS DECIMAL(10,2)) AS SlowestTotalTime
      FROM base
      GROUP BY ItemCode, ItemName
      ORDER BY TotalTimeBeforeDone DESC, ItemName ASC
    `;

    const result = await request.query(q);
    res.json({ success: true, data: result.recordset });
  } catch (err) {
    console.log(err);
    res.status(500).json({ success: false, message: internalError });
  }
});

module.exports = router;