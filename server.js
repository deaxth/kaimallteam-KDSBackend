require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const { closeAllPools } = require("./mssql-pool-management");
const kdsRoutes = require("./routes/kdsRoutes");
const kdsMainte = require("./routes/kdsMainte");
const kdsTerminals = require("./routes/kdsTerminalMainte");
const kdsScreens = require("./routes/kdsScreenMainte");
const { startSync, stopSync } = require("./services/kdsService");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST", "PATCH"],
  },
  transports: ["websocket", "polling"],
  connectionStateRecovery: {
    maxDisconnectionDuration: 2 * 60 * 1000,
    skipMiddlewares: true,
  },
});

app.set("io", io);

app.use(cors());
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: true }));

app.get("/health", (_req, res) => {
  res.json({
    success: true,
    serverTime: new Date().toISOString(),
  });
});

app.use("/api/kds", kdsRoutes);
app.use("/api/kds-mainte", kdsMainte);
app.use("/api/kds-terminals", kdsTerminals);
app.use("/api/kds-screens", kdsScreens);

io.on("connection", (socket) => {
  const screenType = String(
    socket.handshake.query?.screenType || "unknown"
  ).toLowerCase();
  const terminalId = String(socket.handshake.query?.terminalId || "ALL");

  socket.join(`screen:${screenType}`);
  socket.join(`terminal:${terminalId}`);
  socket.join("terminal:ALL");

  socket.emit("kds:connected", {
    socketId: socket.id,
    screenType,
    terminalId,
    serverTime: new Date().toISOString(),
    recovered: !!socket.recovered,
  });

  socket.on("kds:join", (payload = {}) => {
    const nextScreenType = String(
      payload.screenType || screenType
    ).toLowerCase();
    const nextTerminalId = String(payload.terminalId || terminalId);

    socket.join(`screen:${nextScreenType}`);
    socket.join(`terminal:${nextTerminalId}`);
    socket.join("terminal:ALL");
  });

  socket.on("disconnect", (reason) => {
    console.log(`[socket] disconnected: ${socket.id} - ${reason}`);
  });
});

const port = Number(process.env.PORT);

server.listen(port, () => {
  console.log(`Server started at port ${port}.`);
  startSync(io);
});

async function gracefulShutdown(signal) {
  console.log(`\n${signal} received. Closing KDS services...`);
  stopSync();

  server.close(async () => {
    try {
      await closeAllPools();
    } catch (error) {
      console.error("Error while closing SQL pools:", error.message);
    } finally {
      process.exit(0);
    }
  });
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));