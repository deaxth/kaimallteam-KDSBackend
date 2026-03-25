// mssql-pool-management.js
const sql   = require('mssql');
const pools = new Map();

function _configKey(cfg) {
  return `${cfg.server}|${cfg.database}|${cfg.user}|${cfg.port||1433}`;
}

async function getSQLPool(dbConfig) {
  const key = _configKey(dbConfig);
  if (pools.has(key)) {
    const pool = pools.get(key);
    if (pool.connected) return pool;
    // else fall through and reconnect
  }
  const pool = new sql.ConnectionPool(dbConfig);
  await pool.connect();
  pools.set(key, pool);
  console.log(`✅ Connected to database: ${dbConfig.database}`);
  return pool;
}

async function closeAllPools() {
  for (const [key, pool] of pools) {
    await pool.close();
    console.log(`🛑 Closed pool: ${key}`);
  }
  pools.clear();
}

module.exports = { getSQLPool, closeAllPools };
