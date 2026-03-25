require('dotenv').config('../');

const dbconfig = {
    user: process.env.LOC_DB_USER,
    password: process.env.LOC_DB_PW,
    server: process.env.LOC_DB_SERVER,
    database: process.env.LOC_DB_DB,  // Primary database for transactions
    options: {
      trustServerCertificate: true,
      enableArithAbort: true,
      instancename: "MSSQLSERVER",
      requestTimeout: 30000 // Set timeout to 30 seconds (30000 ms)
    },
    port: parseInt(process.env.LOC_DB_PORT),
  };


module.exports = dbconfig;