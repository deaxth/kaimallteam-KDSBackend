const express = require("express");
const sql = require("mssql");
const { getSQLPool } = require("../mssql-pool-management");
const localConfig = require("../config/localConfig");

const router = express.Router();
const SCREEN_TYPES = new Set(["COOKING", "PREPARATION", "PUBLIC", "CENTRAL"]);

function badRequest(message) {
  const error = new Error(message);
  error.statusCode = 400;
  return error;
}

function normalizeScreen(body = {}) {
  const screenName = String(body.screenName || "").trim();
  const screenType = String(body.screenType || "").trim().toUpperCase();

  if (!screenName) throw badRequest("Screen name is required.");
  if (screenName.length > 150) throw badRequest("Screen name is too long.");
  if (!SCREEN_TYPES.has(screenType)) throw badRequest("Invalid screen type.");

  let outletId =
    body.outletId === null || body.outletId === undefined || body.outletId === ""
      ? null
      : Number(body.outletId);

  let stationNo =
    body.stationNo === null || body.stationNo === undefined || body.stationNo === ""
      ? null
      : Number(body.stationNo);

  if (screenType === "CENTRAL") {
    outletId = null;
    stationNo = null;
  } else if (!Number.isInteger(outletId) || outletId <= 0) {
    throw badRequest("An outlet is required for this screen.");
  }

  if (stationNo !== null && (!Number.isInteger(stationNo) || stationNo < 1 || stationNo > 3)) {
    throw badRequest("Station must be 1, 2, or 3.");
  }

  const inactiveValues = [false, 0, "0", "false", "inactive"];
  const isActive = !inactiveValues.includes(
    typeof body.isActive === "string" ? body.isActive.toLowerCase() : body.isActive
  );

  return { screenName, screenType, outletId, stationNo, isActive };
}

async function ensureOutletExists(pool, outletId) {
  if (outletId === null) return;

  const result = await pool
    .request()
    .input("OutletID", sql.Int, outletId)
    .query("SELECT OutletID FROM dbo.KaiTerminalOutlets WHERE OutletID = @OutletID;");

  if (!result.recordset[0]) {
    throw badRequest("Selected outlet does not exist.");
  }
}

function sendError(error, res) {
  console.error("[KDS screens]", error);
  res.status(error.statusCode || 500).json({
    success: false,
    message: error.message || "Unable to process screen maintenance.",
  });
}

router.get("/get-screens", async (_req, res) => {
  try {
    const pool = await getSQLPool(localConfig);

    const result = await pool.request().query(`
      SELECT
        s.ScreenID,
        s.OutletID,
        COALESCE(o.TerminalName, N'All Outlets') AS OutletName,
        s.ScreenType,
        s.StationNo,
        s.ScreenName,
        s.IsActive,
        s.CreatedAt,
        s.UpdatedAt
      FROM dbo.KDS_OutletScreen AS s
      LEFT JOIN dbo.KaiTerminalOutlets AS o
        ON o.OutletID = s.OutletID
      ORDER BY
        CASE WHEN s.ScreenType = N'CENTRAL' THEN 0 ELSE 1 END,
        o.TerminalName,
        s.ScreenType,
        s.StationNo,
        s.ScreenID;
    `);

    res.json({ success: true, data: result.recordset });
  } catch (error) {
    sendError(error, res);
  }
});

router.post("/add-screen", async (req, res) => {
  try {
    const values = normalizeScreen(req.body);
    const pool = await getSQLPool(localConfig);

    await ensureOutletExists(pool, values.outletId);

    const result = await pool
      .request()
      .input("OutletID", sql.Int, values.outletId)
      .input("ScreenType", sql.NVarChar(40), values.screenType)
      .input("StationNo", sql.Int, values.stationNo)
      .input("ScreenName", sql.NVarChar(150), values.screenName)
      .input("IsActive", sql.Bit, values.isActive)
      .query(`
        INSERT INTO dbo.KDS_OutletScreen
          (OutletID, ScreenType, StationNo, ScreenName, IsActive)
        OUTPUT INSERTED.ScreenID
        VALUES
          (@OutletID, @ScreenType, @StationNo, @ScreenName, @IsActive);
      `);

    res.status(201).json({
      success: true,
      screenId: result.recordset[0].ScreenID,
    });
  } catch (error) {
    sendError(error, res);
  }
});

router.post("/edit-screen", async (req, res) => {
  try {
    const screenId = Number(req.body?.screenId);

    if (!Number.isInteger(screenId) || screenId <= 0) {
      throw badRequest("Valid screen ID is required.");
    }

    const values = normalizeScreen(req.body);
    const pool = await getSQLPool(localConfig);

    await ensureOutletExists(pool, values.outletId);

    const result = await pool
      .request()
      .input("ScreenID", sql.Int, screenId)
      .input("OutletID", sql.Int, values.outletId)
      .input("ScreenType", sql.NVarChar(40), values.screenType)
      .input("StationNo", sql.Int, values.stationNo)
      .input("ScreenName", sql.NVarChar(150), values.screenName)
      .input("IsActive", sql.Bit, values.isActive)
      .query(`
        UPDATE dbo.KDS_OutletScreen
        SET
          OutletID = @OutletID,
          ScreenType = @ScreenType,
          StationNo = @StationNo,
          ScreenName = @ScreenName,
          IsActive = @IsActive,
          UpdatedAt = SYSDATETIME()
        OUTPUT INSERTED.ScreenID
        WHERE ScreenID = @ScreenID;
      `);

    if (!result.recordset[0]) {
      throw badRequest("Screen record was not found.");
    }

    res.json({ success: true });
  } catch (error) {
    sendError(error, res);
  }
});

module.exports = router;