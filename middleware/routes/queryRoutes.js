/**
 * 
 *
 * Routes
 * ------
 *   POST /query      Send a SQL string to the DBMS, get output back.
 *   GET  /health     Quick liveness check (no DB interaction needed).
 */

"use strict";

const express   = require("express");
const router    = express.Router();
const { runQuery } = require("../dbProcess");



router.post("/query", async (req, res) => {
  const { query } = req.body;

  // --- Validate input ---
  if (!query || typeof query !== "string" || query.trim() === "") {
    return res.status(400).json({
      success: false,
      error: 'Request body must contain a non-empty "query" string.',
    });
  }

  const sql = query.trim();
  console.log(`[route] POST /query  →  "${sql}"`);

  try {
    const result = await runQuery(sql);

    return res.status(200).json({
      success: true,
      query:   sql,
      result:  result,
    });
  } catch (err) {
    console.error(`[route] Query failed: ${err.message}`);
    return res.status(500).json({
      success: false,
      error:   err.message,
    });
  }
});



router.get("/health", (req, res) => {
  res.status(200).json({ status: "ok", timestamp: new Date().toISOString() });
});

module.exports = router;