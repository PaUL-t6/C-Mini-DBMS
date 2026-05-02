"use strict";

const express      = require("express");
const cors         = require("cors");
const queryRoutes  = require("./routes/queryRoutes");

const app  = express();
const PORT = process.env.PORT || 5000;

// ---------------------------------------------------------------------------
// CORS Configuration  (fixes browser "Failed to fetch")
// ---------------------------------------------------------------------------

app.use(cors({
  origin: "http://localhost:3000",   // allow React frontend
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type"]
}));

// Handle preflight requests
app.options("*", cors());

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

// Parse JSON request bodies
app.use(express.json());

// Simple request logger
app.use((req, _res, next) => {
  console.log(`[server] ${req.method} ${req.url}`);
  next();
});

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Mount query routes at /api
// exposes:
//   POST /api/query
//   GET  /api/health
app.use("/api", queryRoutes);

// ---------------------------------------------------------------------------
// 404 handler
// ---------------------------------------------------------------------------

app.use((_req, res) => {
  res.status(404).json({
    success: false,
    error: "Route not found. Available: POST /api/query, GET /api/health",
  });
});

// ---------------------------------------------------------------------------
// Global error handler
// ---------------------------------------------------------------------------

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error(`[server] Unhandled error: ${err.message}`);
  res.status(500).json({ success: false, error: "Internal server error" });
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`[server] Listening on http://localhost:${PORT}`);
  console.log(`[server] Send queries with:`);
  console.log(`         curl -X POST http://localhost:${PORT}/api/query \\`);
  console.log(`              -H "Content-Type: application/json" \\`);
  console.log(`              -d '{"query":"SELECT * FROM students"}'`);
});

module.exports = app;