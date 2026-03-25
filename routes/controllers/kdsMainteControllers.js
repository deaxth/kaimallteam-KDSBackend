const sql = require('mssql');
const { getSQLPool } = require('../../mssql-pool-management');
const localConfig = require('../../config/localConfig');

async function getLocalPool() {
  return getSQLPool(localConfig);
}

function requestOf(executor) {
  return executor.request()
}

function normalSuccess(res) {
  res.json({ message: 'Success.' });
}

function catchError(err, res) {
  console.log(err);
  res.status(500).json({ message: 'Something went wrong. Please contact support.' })
};

async function getValuesToChange(oldValues, newValues) {
  let valuesToChange = [];

  for (let i = 0; i < oldValues.length; i++) {

    if (oldValues[i].label !== 'terminalIds' && oldValues[i].value !== newValues[i].value ) {
      valuesToChange.push({ label: newValues[i].label, value:  newValues[i].value});
    }

    if (oldValues[i].label === 'terminalIds') {
      const set = new Set(oldValues[i].value);
      const newSet = new Set(newValues[i].value);
      const newIds = [];
      const ids = [];
      const forDeletion = [];
      for (const newValue of newValues[i].value) {
        if (!set.has(newValue)){
          newIds.push(newValue);
        };
        ids.push(newValue);
      };

      for (const oldValue of oldValues[i].value) {
        if (!newSet.has(oldValue)){
          forDeletion.push(oldValue);
        };
      };

      if (newIds.length !== 0 || forDeletion.length !== 0) {
        valuesToChange.push({ label: newValues[i].label, value: ids, oldValue: oldValues[i].value, deletion: forDeletion })
      }
    }
  }

  return valuesToChange;
}

function updatingHelper(label, tag) {
  const labelTag = label === 'terminalName' ? 'TerminalName' : 
                label === 'hasMultiple' ? 'HasMultipleStations' :
                'Status';
  const dataType = label === 'terminalName' ? sql.VarChar : 
                   label === 'hasMultiple' ? sql.Bit:
                  sql.VarChar;

  return tag === 'name' ? labelTag : dataType;
}

async function getAllTerminals(req, res) {
  try {
    const pool = await getSQLPool(localConfig);
    const getTerminalHeaders = await pool.request()
      .query(`SELECT OutletID, TerminalName, HasMultipleStations, Status FROM KaiTerminalOutlets`);
    const getTerminalIds = await pool.request()
      .query(`SELECT OutletID, TerminalID FROM KaiTerminalIDs`);
    const terminalHeaders = getTerminalHeaders.recordset;
    const terminalIds = getTerminalIds.recordset;

    let finalToReturn = [];

    for (const terminalHeader of terminalHeaders) {
      const schema = {
        OutletID: null,
        TerminalName: null, 
        HasMultipleStations: null,
        Status: null,
        TerminalIDs: []
      };
      schema.OutletID = terminalHeader.OutletID;
      schema.TerminalName = terminalHeader.TerminalName;
      schema.HasMultipleStations = terminalHeader.HasMultipleStations;
      schema.Status = terminalHeader.Status;
      const myMap = new Map(terminalIds.map(terminalId => [terminalId.TerminalID, terminalId]));

      for (const map of myMap.values()) {
        if (map.OutletID === terminalHeader.OutletID) {
          schema.TerminalIDs.push(map.TerminalID);
        }
      }
      finalToReturn.push(schema);
    }
    
    res.json({ message: "request success.", data: finalToReturn})
    
  } catch(err) {
    catchError(err, res);
  };
};

async function addTerminal (req, res) {
  const { terminalName, hasMultiple, status, terminalIds } = req.body;
  const userId = 1;
  const localPool = await getLocalPool();
  const tx = await new sql.Transaction(localPool);
  await tx.begin();
  try {
    const checkTerminal = await requestOf(tx)
      .input('TerminalName', sql.VarChar, terminalName)
      .query('SELECT TerminalName FROM KaiTerminalOutlets WHERE TerminalName = @TerminalName');
    
    if (checkTerminal.recordset[0]?.TerminalName) {
      await tx.rollback();
      res.status(409).json({ message: "Terminal already exists." });
    }

    const inserted = await requestOf(tx)
      .input('TerminalName', sql.VarChar, terminalName)
      .input('HasMultipleStations', sql.Bit, hasMultiple || 0)
      .input('Status', sql.VarChar, status || 'Active')
      .input('WhoCreated', sql.Int, userId)
      .query(`
        INSERT INTO KaiTerminalOutlets
          (TerminalName, HasMultipleStations, Status, WhoCreated)
        OUTPUT INSERTED.OutletID
        VALUES 
          (@TerminalName, @HasMultipleStations, @Status, @WhoCreated);
      `);
    const outletId = inserted.recordset[0].OutletID;

    for (const terminalId of terminalIds) {
      await requestOf(tx)
        .input('OutletID', sql.Int, parseInt(outletId))
        .input('TerminalID', sql.Int, parseInt(terminalId))
        .query(`
          INSERT INTO KaiTerminalIDs
            (OutletID, TerminalID)
          VALUES 
            (@OutletID, @TerminalID);
        `);
    };
    await tx.commit();
    normalSuccess(res);
  } catch(err) {
    catchError(err, res);
  }
};

async function editTerminal(req, res) {
  const { outletId, terminalName, hasMultiple, status, terminalIds } = req.body;
  const userId = 1;
  const localPool = await getLocalPool();
  const tx = new sql.Transaction(localPool);
  await tx.begin();
  try {
    const newValues = [ { label: 'terminalName', value: terminalName},
                        { label: 'hasMultiple', value: hasMultiple},
                        { label: 'status', value: status } ,
                        { label: 'terminalIds', value: terminalIds } ,
                      ];
    const getExistingDetails = await requestOf(tx)  
      .input('OutletID', sql.Int, parseInt(outletId))
      .query('SELECT TerminalName, HasMultipleStations, Status FROM KaiTerminalOutlets WHERE OutletID = @OutletID');
   
    const getExistingOutletIds = await requestOf(tx)
      .input('OutletID', sql.Int, parseInt(outletId))         
      .query('SELECT OutletID, TerminalID FROM KaiTerminalIDs WHERE OutletID = @OutletID ');
    let retrieveOutlets = [];

    const getExistingOutlets = getExistingOutletIds.recordset;

    for (const outlet of getExistingOutlets) {
      retrieveOutlets.push(outlet.TerminalID);
    } 
    
    const oldValues = [
                        { label: 'terminalName', value: getExistingDetails.recordset[0].TerminalName},
                        { label: 'hasMultiple', value: getExistingDetails.recordset[0].HasMultipleStations },
                        { label: 'status', value: getExistingDetails.recordset[0].Status } ,
                        { label: 'terminalIds', value: retrieveOutlets } ,
                      ];

    const resultsArray = await getValuesToChange(oldValues, newValues);
    console.log(resultsArray)                  
    if (resultsArray.length === 0) {
      await tx.rollback();
      return res.status(400).json({ message: "No changes made." });
    }

    for (const result of resultsArray) {
      if (result.label !== 'terminalIds') {
        await requestOf(tx)
          .input(updatingHelper(result.label, 'name'),  updatingHelper(result.label, 'data-type'), result.value)
          .input('OutletID', sql.Int, outletId)
          .input('UserID', sql.Int, userId)
          .query(`
            UPDATE KaiTerminalOutlets
            SET ${updatingHelper(result.label, 'name')} = @${updatingHelper(result.label, 'name')},
                WhoModified = @UserID,
                DateModified = GETDATE()
            WHERE OutletID = @OutletID;
        `);
      };

      if (result.label === 'terminalIds') {
        for (let i = 0; i < result.value.length; i++) {
          const getTerminalIds = await requestOf(tx)  
            .input('OutletID', sql.Int, outletId)
            .input('TerminalID', sql.Int, result.oldValue[i] || 0)
            .query('SELECT TerminalID FROM KaiTerminalIDs WHERE OutletID = @OutletID AND TerminalID = @TerminalID');
          
          const isExisting = getTerminalIds.recordset[0]?.TerminalID;
          if (isExisting) {
            await requestOf(tx)
            .input('OutletID', sql.Int, outletId)
            .input('UserID', sql.Int, userId)
            .input('TerminalID', sql.Int, parseInt(result.value[i]))
            .input('OldTerminalID', sql.Int, parseInt(result.oldValue[i]))
            .query(`
              UPDATE KaiTerminalIDs
                SET TerminalID = @TerminalID
              WHERE OutletID = @OutletID AND TerminalID = @OldTerminalID;

              UPDATE KaiTerminalOutlets
              SET WhoModified = @UserID,
                  DateModified = GETDATE()
              WHERE OutletID = @OutletID;  
            `)
          }
          console.log(isExisting, result.value[i], result.oldValue[i]);
          if (!isExisting) {
            await requestOf(tx)
            .input('OutletID', sql.Int, outletId)
            .input('UserID', sql.Int, userId)
            .input('TerminalID', sql.Int, parseInt(result.value[i]))
            .query(`
              INSERT INTO KaiTerminalIDs
                (OutletID, TerminalID)
              VALUES 
                (@OutletID, @TerminalID);

              UPDATE KaiTerminalOutlets
              SET WhoModified = @UserID,
                  DateModified = GETDATE()
              WHERE OutletID = @OutletID;  
            `);
          }
        }

        if (result.deletion?.length !== 0) {
          const forDeletion = result.deletion;
          const deletionInput = "(" + forDeletion.join(",") + ")";

          await requestOf(tx)
          .input('OutletID', sql.Int, outletId)
          .input('UserID', sql.Int, userId)
          .query(`
            DELETE FROM KaiTerminalIDs WHERE TerminalID IN ${deletionInput} AND OutletID = @OutletID;
            
            UPDATE KaiTerminalOutlets
            SET WhoModified = @UserID,
                DateModified = GETDATE()
            WHERE OutletID = @OutletID;  
          `);
        }
      }
    };
    console.log(resultsArray);
    await tx.commit();
    res.json({ result: 'success.' });
  } catch(err) {
    catchError(err, res);
  };
}

module.exports = {
  addTerminal,
  editTerminal,
  getAllTerminals
};