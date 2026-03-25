const express = require('express');
const sql = require('mssql');
const { getSQLPool } = require('../mssql-pool-management');

const localConfig = require('../config/localConfig');

const { addTerminal, editTerminal } = require('./controllers/kdsMainteControllers');

const router = express.Router();

router.post('/add-terminal', addTerminal);
router.post('/edit-terminal', editTerminal);


module.exports = router;


