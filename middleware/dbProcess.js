

"use strict";

const { spawn } = require("child_process");
const path = require("path");


const DBMS_BINARY = path.resolve(__dirname, "../backend/dbms" + (process.platform === "win32" ? ".exe" : ""));

const QUERY_TIMEOUT_MS = 5000;



let dbProcess = null;   // the ChildProcess object
let outputBuf = "";     // accumulates stdout chunks between queries
let pending = null;   // { resolve, reject, timer } for the in-flight query
const queue = [];     // waiting queries: [{ query, resolve, reject }]

//  start the C process

function spawnDBMS() {
  console.log(`[dbProcess] Spawning DBMS: ${DBMS_BINARY}`);

  dbProcess = spawn(DBMS_BINARY, [], {
    stdio: ["pipe", "pipe", "pipe"],
  });


  dbProcess.stdout.on("data", (chunk) => {
    outputBuf += chunk.toString();


    if (pending && outputBuf.includes("> ")) {
      const result = extractResult(outputBuf);
      outputBuf = "";                        // clear buffer for next query

      clearTimeout(pending.timer);
      const { resolve } = pending;
      pending = null;

      resolve(result);
      processQueue();                        // kick off next waiting query
    }
  });

  // ---- stderr: log C-side errors but do not crash the server ----
  dbProcess.stderr.on("data", (data) => {
    console.error(`[dbms stderr] ${data.toString().trim()}`);
  });

  // ---- exit: log and clear the handle so callers get a clean error ----
  dbProcess.on("exit", (code, signal) => {
    console.warn(`[dbProcess] DBMS exited — code=${code} signal=${signal}`);
    dbProcess = null;

    // Reject any in-flight or queued query
    if (pending) {
      clearTimeout(pending.timer);
      pending.reject(new Error("DBMS process exited unexpectedly"));
      pending = null;
    }
    while (queue.length) {
      queue.shift().reject(new Error("DBMS process is not running"));
    }
  });

  // Discard the startup banner so the first real prompt lands cleanly
  outputBuf = "";
}




function extractResult(raw) {

  const normalised = "\n" + raw;
  const segments = normalised.split(/\n> /);

  // Collect non-empty segments that are not part of the startup banner
  const results = segments
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .filter((s) => !s.includes("Mini In-Memory DBMS"))
    .filter((s) => !s.includes("Type HELP"));

  return results.join("\n").trim();
}


// processQueue  –  send the next waiting query if the process is free


function processQueue() {
  if (pending || queue.length === 0) return;

  const { query, resolve, reject } = queue.shift();
  sendToProcess(query, resolve, reject);
}


// sendToProcess  –  write one SQL line and set up the pending promise


function sendToProcess(query, resolve, reject) {
  if (!dbProcess) {
    return reject(new Error("DBMS process is not running"));
  }

  // Timeout guard — if the C process hangs, reject after QUERY_TIMEOUT_MS
  const timer = setTimeout(() => {
    pending = null;
    outputBuf = "";
    reject(new Error(`Query timed out after ${QUERY_TIMEOUT_MS}ms`));
    processQueue();
  }, QUERY_TIMEOUT_MS);

  pending = { resolve, reject, timer };

  // Send the SQL followed by a newline (what the C REPL expects)
  dbProcess.stdin.write(query + "\n");
}


// runQuery  –  public API used by the route handler


/**
 * Send a SQL string to the DBMS and return a Promise that resolves
 * with the trimmed output string.
 *
 * @param  {string} query  SQL statement (e.g. "SELECT * FROM students")
 * @returns {Promise<string>}
 */
function runQuery(query) {
  return new Promise((resolve, reject) => {
    if (!dbProcess) {
      return reject(new Error("DBMS process is not running. Check that the binary exists at: " + DBMS_BINARY));
    }

    if (pending) {
      // Another query is in-flight — add this one to the wait queue
      queue.push({ query, resolve, reject });
    } else {
      sendToProcess(query, resolve, reject);
    }
  });
}


// shutdown  –  gracefully close the child process


function shutdown() {
  if (dbProcess) {
    console.log("[dbProcess] Sending EXIT to DBMS...");
    dbProcess.stdin.write("EXIT\n");
    dbProcess.stdin.end();
  }
}


// Initialise on module load

spawnDBMS();

// Ensure the child is killed if the Node process exits
process.on("exit", shutdown);
process.on("SIGINT", () => { shutdown(); process.exit(0); });
process.on("SIGTERM", () => { shutdown(); process.exit(0); });


module.exports = { runQuery, shutdown };