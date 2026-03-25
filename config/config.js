const dbconfig = {
  user: process.env.SRC_DB_USER,
  password: process.env.SRC_DB_PW,
  server: process.env.SRC_DB_SERVER,
  database: process.env.SRC_DB_DB,  // Primary database for transactions
  options: {
    trustServerCertificate: true,
    enableArithAbort: true,
    instancename: "MSSQLSERVER",
    requestTimeout: 30000 // Set timeout to 30 seconds (30000 ms)
  },
  port: parseInt(process.env.SRC_DB_PORT),
};

module.exports = dbconfig;