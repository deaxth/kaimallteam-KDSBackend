const express = require("express");
const sql = require("mssql");
const { getSQLPool } = require("../mssql-pool-management");
const localConfig = require("../config/localConfig");

const router = express.Router();

function badRequest(message) {
  const error = new Error(message);
  error.statusCode = 400;
  return error;
}

function normalizeRoute(body = {}) {
  const itemCode = String(body.itemCode || "").trim();

  if (!itemCode) {
    throw badRequest("Item code is required.");
  }

  const outletId = Number(body.outletId);
  if (!Number.isInteger(outletId) || outletId <= 0) {
    throw badRequest("A valid kitchen outlet is required.");
  }

  const stationNo = Number(body.stationNo);
  if (!Number.isInteger(stationNo) || stationNo < 1 || stationNo > 3) {
    throw badRequest("Station must be 1, 2, or 3.");
  }

  const rawTime = body.timeNeededMinutes;
  const timeNeededMinutes =
    rawTime === null || rawTime === undefined || rawTime === ""
      ? null
      : Number(rawTime);

  if (
    timeNeededMinutes !== null &&
    (!Number.isFinite(timeNeededMinutes) || timeNeededMinutes < 0)
  ) {
    throw badRequest("Time needed must be zero or a positive number.");
  }

  const inactiveValues = [false, 0, "0", "false", "inactive"];
  const isActive = !inactiveValues.includes(
    typeof body.isActive === "string" ? body.isActive.toLowerCase() : body.isActive
  );

  return {
    itemCode,
    outletId,
    stationNo,
    timeNeededMinutes,
    isActive,
  };
}

async function ensureOutletExists(pool, outletId) {
  const result = await pool
    .request()
    .input("OutletID", sql.Int, outletId)
    .query("SELECT OutletID FROM dbo.KaiTerminalOutlets WHERE OutletID = @OutletID;");

  if (!result.recordset[0]) {
    throw badRequest("Selected outlet does not exist.");
  }
}

function sendError(error, res) {
  console.error("[KDS menu routing]", error);

  if (error.number === 2601 || error.number === 2627) {
    return res.status(409).json({
      success: false,
      message: "This item is already assigned to that kitchen outlet.",
    });
  }

  return res.status(error.statusCode || 500).json({
    success: false,
    message: error.message || "Unable to process menu routing.",
  });
}

router.get("/get-routes", async (_req, res) => {
  try {
    const pool = await getSQLPool(localConfig);

    const result = await pool.request().query(`
      SELECT
        r.MenuKitchenRouteID,
        r.ItemCode,
        r.OutletID,
        o.TerminalName AS OutletName,
        r.StationNo,
        r.TimeNeededMinutes,
        r.IsActive,
        r.CreatedAt,
        r.UpdatedAt
      FROM dbo.KDS_MenuKitchenRoute AS r
      INNER JOIN dbo.KaiTerminalOutlets AS o
        ON o.OutletID = r.OutletID
      ORDER BY r.ItemCode, o.TerminalName, r.StationNo, r.MenuKitchenRouteID;
    `);

    res.json({ success: true, data: result.recordset });
  } catch (error) {
    sendError(error, res);
  }
});

router.post("/add-route", async (req, res) => {
  try {
    const values = normalizeRoute(req.body);
    const pool = await getSQLPool(localConfig);

    await ensureOutletExists(pool, values.outletId);

    const result = await pool
      .request()
      .input("ItemCode", sql.NVarChar(50), values.itemCode)
      .input("OutletID", sql.Int, values.outletId)
      .input("StationNo", sql.Int, values.stationNo)
      .input("TimeNeededMinutes", sql.Decimal(10, 2), values.timeNeededMinutes)
      .input("IsActive", sql.Bit, values.isActive)
      .query(`
        INSERT INTO dbo.KDS_MenuKitchenRoute
          (ItemCode, OutletID, StationNo, TimeNeededMinutes, IsActive)
        OUTPUT INSERTED.MenuKitchenRouteID
        VALUES
          (@ItemCode, @OutletID, @StationNo, @TimeNeededMinutes, @IsActive);
      `);

    res.status(201).json({
      success: true,
      menuKitchenRouteId: result.recordset[0].MenuKitchenRouteID,
    });
  } catch (error) {
    sendError(error, res);
  }
});

router.post("/edit-route", async (req, res) => {
  try {
    const menuKitchenRouteId = Number(req.body?.menuKitchenRouteId);

    if (!Number.isInteger(menuKitchenRouteId) || menuKitchenRouteId <= 0) {
      throw badRequest("Valid route ID is required.");
    }

    const values = normalizeRoute(req.body);
    const pool = await getSQLPool(localConfig);

    await ensureOutletExists(pool, values.outletId);

    const result = await pool
      .request()
      .input("MenuKitchenRouteID", sql.Int, menuKitchenRouteId)
      .input("ItemCode", sql.NVarChar(50), values.itemCode)
      .input("OutletID", sql.Int, values.outletId)
      .input("StationNo", sql.Int, values.stationNo)
      .input("TimeNeededMinutes", sql.Decimal(10, 2), values.timeNeededMinutes)
      .input("IsActive", sql.Bit, values.isActive)
      .query(`
        UPDATE dbo.KDS_MenuKitchenRoute
        SET
          ItemCode = @ItemCode,
          OutletID = @OutletID,
          StationNo = @StationNo,
          TimeNeededMinutes = @TimeNeededMinutes,
          IsActive = @IsActive,
          UpdatedAt = SYSDATETIME()
        OUTPUT INSERTED.MenuKitchenRouteID
        WHERE MenuKitchenRouteID = @MenuKitchenRouteID;
      `);

    if (!result.recordset[0]) {
      throw badRequest("Menu route was not found.");
    }

    res.json({ success: true });
  } catch (error) {
    sendError(error, res);
  }
});

module.exports = router;