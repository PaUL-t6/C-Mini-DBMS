import { useState, useRef, useEffect } from "react";

/* ─────────────────────────────────────────────────────────────────
   Global styles — same design tokens, adds new animation keyframes
───────────────────────────────────────────────────────────────── */
const GLOBAL_STYLE = `
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Syne:wght@700;800&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg:       #0c0e11;  --surface: #13161b;  --border:  #1f2530;
    --border2:  #2a3040;  --accent:  #00e5a0;  --accent2: #00b87a;
    --amber:    #f5a623;  --red:     #ff4d6d;  --blue:    #4d9fff;
    --purple:   #a78bfa;  --text:    #cdd5e0;  --muted:   #5a6680;
    --code-bg:  #0a0c0f;
  }
  html, body, #root { height: 100%; background: var(--bg); color: var(--text);
    font-family: 'JetBrains Mono', monospace; font-size: 13px; line-height: 1.6;
    -webkit-font-smoothing: antialiased; }
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--muted); }
  @keyframes fadeUp  { from { opacity:0; transform:translateY(10px) } to { opacity:1; transform:translateY(0) } }
  @keyframes fadeIn  { from { opacity:0 } to { opacity:1 } }
  @keyframes pulse   { 0%,100% { opacity:1 } 50% { opacity:0 } }
  @keyframes glow    { 0%,100% { box-shadow:0 0 8px #00e5a030 } 50% { box-shadow:0 0 20px #00e5a060 } }
  @keyframes rowIn   { from { opacity:0; transform:translateX(-6px) } to { opacity:1; transform:translateX(0) } }
  @keyframes spin    { to { transform:rotate(360deg) } }
  @keyframes nodeIn  { from { opacity:0; transform:scale(.88) } to { opacity:1; transform:scale(1) } }
  @keyframes countUp { from { opacity:0; transform:translateY(8px) } to { opacity:1; transform:translateY(0) } }
  @keyframes lineGrow { from { width:0 } to { width:100% } }
`;

/* ─────────────────────────────────────────────────────────────────
   Utility parsers
───────────────────────────────────────────────────────────────── */
// C backend log prefixes — never part of table data
const LOG_LINE = /^\s*\[(optimizer|executor|storage|transaction|dbms|parser)\]/i;

function parseTableOutput(raw) {
  if (!raw) return null;
  // Strip backend log lines so they are never mistaken for table rows
  const lines  = raw.split("\n").map(l => l.trimEnd()).filter(l => !LOG_LINE.test(l));
  const sepIdx = lines.findIndex(l => /^[\s-]+$/.test(l) && l.includes("--"));
  if (sepIdx < 1) return null;
  const headerLine = lines[sepIdx - 1];
  const dataLines  = lines.slice(sepIdx + 1).filter(l => l.trim() !== "");
  const sepLine    = lines[sepIdx];
  const colBounds  = [];
  let inBlock = false, start = 0;
  for (let i = 0; i <= sepLine.length; i++) {
    const ch = sepLine[i];
    if (!inBlock && ch === "-")  { inBlock = true; start = i; }
    if (inBlock  && ch !== "-")  { colBounds.push([start, i]); inBlock = false; }
  }
  if (inBlock) colBounds.push([start, sepLine.length]);
  const extract = (line, [s, e]) => line.slice(s, e)?.trim() ?? "";
  return {
    headers: colBounds.map(b => extract(headerLine, b)),
    rows:    dataLines.map(l => colBounds.map(b => extract(l, b))),
  };
}

function parseExplainOutput(raw) {
  if (!raw || !raw.includes("Query Plan:")) return null;
  const lines = raw.split("\n").map(l => l.trim()).filter(Boolean);

  // The pipeline line looks like:
  //   "Parser -> Optimizer -> Hash Index Search"
  // Find it by looking for a line that contains "->" and starts with "Parser"
  // OR reconstruct it if the middleware split it on "> " (old bug, kept for safety)
  let pipeline = lines.find(l => l.startsWith("Parser") && l.includes("->"));
  if (!pipeline) {
    // Fallback: find "Parser -" fragment and glue surrounding lines
    const idx = lines.findIndex(l => l.startsWith("Parser"));
    if (idx >= 0) pipeline = lines.slice(idx, idx + 3).join(" -> ").replace(/ -> -> /g, " -> ");
  }

  // Robust field extraction — handles varying whitespace around ":"
  const findField = (prefix) => {
    const l = lines.find(ln => ln.replace(/\s+/g, " ").startsWith(prefix));
    if (!l) return null;
    const colonIdx = l.indexOf(":");
    return colonIdx >= 0 ? l.slice(colonIdx + 1).trim() : null;
  };

  const planType = findField("Plan Type");
  const estCost  = findField("Est. Cost");
  const reason   = findField("Reason");
  const innerQ   = findField("Inner Query");
  const records  = findField("Records");
  const matches  = findField("Matches");
  const height   = findField("Tree Height");

  // Extract only the final stage from the pipeline
  const strategy = pipeline
    ? pipeline.split(/\s*->\s*|\s*→\s*/).map(s => s.trim()).filter(Boolean).pop()
    : null;

  return { pipeline, strategy, planType, estCost, reason, innerQ, records, matches, height };
}

function parseIndexStats(rawList) {
  const combined = rawList.join("\n");
  const rowsMatch = combined.match(/\((\d+)\s+rows?\)/i);
  const n = rowsMatch ? parseInt(rowsMatch[1]) : 0;
  return {
    rowCount:  n,
    bpHeight:  n > 0 ? Math.max(1, Math.ceil(Math.log(n + 1) / Math.log(4))) : 0,
    hashLoad:  n > 0 ? Math.min(100, Math.round((n / 101) * 100)) : 0,
  };
}

/* ─────────────────────────────────────────────────────────────────
   Presets
───────────────────────────────────────────────────────────────── */
const PRESETS = [
  { label:"CREATE",      sql:"CREATE TABLE students" },
  { label:"INSERT · 1", sql:"INSERT INTO students VALUES (1, Alice, 20)" },
  { label:"INSERT · 2", sql:"INSERT INTO students VALUES (2, Bob, 22)" },
  { label:"INSERT · 3", sql:"INSERT INTO students VALUES (3, Charlie, 19)" },
  { label:"SELECT ALL", sql:"SELECT * FROM students" },
  { label:"WHERE id=1", sql:"SELECT * FROM students WHERE id = 1" },
  { label:"EXPLAIN",    sql:"EXPLAIN SELECT * FROM students WHERE id = 1" },
  { label:"COUNT",      sql:"SELECT COUNT(*) FROM students" },
];

/* ─────────────────────────────────────────────────────────────────
   QueryEditor  — unchanged from original
───────────────────────────────────────────────────────────────── */
function QueryEditor({ sql, setSql, onRun, loading }) {
  const taRef = useRef(null);
  useEffect(() => {
    const el = taRef.current; if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.max(80, el.scrollHeight) + "px";
  }, [sql]);
  const handleKey = e => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); onRun(); }
    if (e.key === "Tab") {
      e.preventDefault();
      const { selectionStart: s, selectionEnd: end } = e.target;
      const next = sql.slice(0, s) + "  " + sql.slice(end);
      setSql(next);
      requestAnimationFrame(() => { e.target.selectionStart = e.target.selectionEnd = s + 2; });
    }
  };
  return (
    <div style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderRadius:8, overflow:"hidden", animation:"fadeUp .4s ease both" }}>
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", padding:"8px 14px", borderBottom:"1px solid var(--border)", background:"#0f1218" }}>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          {["#ff5f57","#ffbd2e","#28c840"].map((c,i) => <span key={i} style={{ width:10, height:10, borderRadius:"50%", background:c, display:"inline-block", opacity:.7 }} />)}
          <span style={{ marginLeft:8, color:"var(--muted)", fontSize:11, letterSpacing:".08em" }}>SQL EDITOR</span>
        </div>
        <span style={{ color:"var(--muted)", fontSize:10, letterSpacing:".06em" }}>CTRL+ENTER TO RUN</span>
      </div>
      <div style={{ position:"relative" }}>
        <div style={{ position:"absolute", top:0, left:0, bottom:0, width:36, background:"#0a0c0f", borderRight:"1px solid var(--border)", display:"flex", flexDirection:"column", alignItems:"flex-end", paddingTop:14, paddingRight:8, pointerEvents:"none", overflow:"hidden" }}>
          {sql.split("\n").map((_,i) => <div key={i} style={{ color:"var(--muted)", fontSize:11, lineHeight:"1.6", minHeight:"20.8px" }}>{i+1}</div>)}
        </div>
        <textarea ref={taRef} value={sql} onChange={e => setSql(e.target.value)} onKeyDown={handleKey} spellCheck={false} placeholder="-- Write SQL here..."
          style={{ display:"block", width:"100%", minHeight:80, padding:"14px 14px 14px 50px", background:"var(--code-bg)", color:"#e2e8f0", border:"none", outline:"none", resize:"none", fontFamily:"'JetBrains Mono',monospace", fontSize:13, lineHeight:1.6, caretColor:"var(--accent)" }} />
      </div>
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", padding:"10px 14px", borderTop:"1px solid var(--border)", background:"#0f1218" }}>
        <div style={{ display:"flex", gap:6, flexWrap:"wrap" }}>
          {PRESETS.map(p => (
            <button key={p.label} onClick={() => setSql(p.sql)} style={{ padding:"3px 9px", background:"transparent", border:"1px solid var(--border2)", borderRadius:4, color:"var(--muted)", fontFamily:"'JetBrains Mono',monospace", fontSize:10, letterSpacing:".04em", cursor:"pointer", transition:"all .15s" }}
              onMouseEnter={e => { e.target.style.borderColor="var(--accent)"; e.target.style.color="var(--accent)"; }}
              onMouseLeave={e => { e.target.style.borderColor="var(--border2)"; e.target.style.color="var(--muted)"; }}>
              {p.label}
            </button>
          ))}
        </div>
        <button onClick={onRun} disabled={loading || !sql.trim()} style={{ display:"flex", alignItems:"center", gap:8, padding:"8px 22px", background:loading?"var(--border)":"var(--accent)", color:loading?"var(--muted)":"#0c0e11", border:"none", borderRadius:6, fontFamily:"'JetBrains Mono',monospace", fontSize:12, fontWeight:700, letterSpacing:".08em", cursor:loading?"not-allowed":"pointer", transition:"all .15s", animation:!loading&&sql.trim()?"glow 2s ease infinite":"none" }}
          onMouseEnter={e => { if (!loading) e.currentTarget.style.background="var(--accent2)"; }}
          onMouseLeave={e => { if (!loading) e.currentTarget.style.background="var(--accent)"; }}>
          {loading
            ? <><span style={{ width:12, height:12, border:"2px solid var(--muted)", borderTopColor:"var(--accent)", borderRadius:"50%", display:"inline-block", animation:"spin .7s linear infinite" }}/>RUNNING</>
            : <><span style={{ fontSize:10 }}>▶</span>RUN QUERY</>}
        </button>
      </div>
    </div>
  );
}

/* ─────────────────────────────────────────────────────────────────
   ResultsTable  — unchanged from original
───────────────────────────────────────────────────────────────── */
function ResultsTable({ result, loading, elapsed }) {
  if (loading) return (
    <div style={{ display:"flex", alignItems:"center", justifyContent:"center", height:120, color:"var(--muted)", gap:10, animation:"fadeUp .3s ease both" }}>
      <span style={{ width:14, height:14, border:"2px solid var(--border2)", borderTopColor:"var(--accent)", borderRadius:"50%", display:"inline-block", animation:"spin .7s linear infinite" }} />
      Executing query…
    </div>
  );
  if (!result) return null;
  const table = parseTableOutput(result.raw);
  const isError = !result.success;
  return (
    <div style={{ animation:"fadeUp .35s ease both" }}>
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", padding:"8px 14px", background:"var(--surface)", border:"1px solid var(--border2)", borderRadius:"8px 8px 0 0", borderBottom:"1px solid var(--border)" }}>
        <div style={{ display:"flex", alignItems:"center", gap:10 }}>
          <span style={{ width:7, height:7, borderRadius:"50%", background:isError?"var(--red)":"var(--accent)", display:"inline-block", boxShadow:isError?"0 0 8px var(--red)":"0 0 8px var(--accent)" }} />
          <span style={{ color:isError?"var(--red)":"var(--accent)", fontSize:11, fontWeight:700, letterSpacing:".1em" }}>{isError?"ERROR":"OK"}</span>
          {table && (
            <>
              <span style={{ color:"var(--muted)", fontSize:11 }}>· {table.rows.length} row{table.rows.length!==1?"s":""}</span>
              {(result.sql.toLowerCase().includes("where id") || result.sql.toLowerCase().includes("where item_id")) && table.rows.length > 1 && (
                <span style={{ background:"var(--amber)20", color:"var(--amber)", fontSize:9, padding:"2px 8px", borderRadius:4, border:"1px solid var(--amber)40", marginLeft:8, letterSpacing:".04em" }}>
                  🔁 Duplicate keys detected — retrieved via B+ Tree
                </span>
              )}
            </>
          )}
        </div>
        {elapsed!=null && <span style={{ color:"var(--muted)", fontSize:10, letterSpacing:".04em" }}>{elapsed}ms</span>}
      </div>
      <div style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderTop:"none", borderRadius:"0 0 8px 8px", overflow:"hidden" }}>
        {table ? (
          <div style={{ overflowX:"auto" }}>
            <table style={{ width:"100%", borderCollapse:"collapse" }}>
              <thead>
                <tr style={{ background:"#0f1218" }}>
                  {table.headers.map(h => <th key={h} style={{ padding:"9px 16px", textAlign:"left", color:"var(--accent)", fontSize:10, fontWeight:700, letterSpacing:".12em", borderBottom:"1px solid var(--border2)", whiteSpace:"nowrap" }}>{h}</th>)}
                </tr>
              </thead>
              <tbody>
                {table.rows.map((row,ri) => (
                  <tr key={ri} style={{ borderBottom:ri<table.rows.length-1?"1px solid var(--border)":"none", animation:"rowIn .25s ease both", animationDelay:`${ri*40}ms` }}>
                    {row.map((cell,ci) => <td key={ci} style={{ padding:"9px 16px", color:ci===0?"var(--amber)":"var(--text)", fontSize:12, fontVariantNumeric:"tabular-nums" }}>{cell}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <pre style={{ padding:"14px 16px", color:isError?"var(--red)":"var(--text)", fontSize:12, lineHeight:1.7, whiteSpace:"pre-wrap", wordBreak:"break-word" }}>
            {isError ? result.error : result.raw}
          </pre>
        )}
      </div>
    </div>
  );
}

/* ─────────────────────────────────────────────────────────────────
   HistoryItem  — unchanged from original
───────────────────────────────────────────────────────────────── */
function HistoryItem({ entry, onClick }) {
  return (
    <button onClick={() => onClick(entry.sql)} title={entry.sql} style={{ display:"flex", alignItems:"center", gap:8, width:"100%", padding:"7px 12px", background:"transparent", border:"none", borderRadius:5, textAlign:"left", cursor:"pointer", transition:"background .12s" }}
      onMouseEnter={e => e.currentTarget.style.background="var(--border)"}
      onMouseLeave={e => e.currentTarget.style.background="transparent"}>
      <span style={{ width:5, height:5, borderRadius:"50%", flexShrink:0, background:entry.success?"var(--accent)":"var(--red)" }} />
      <span style={{ color:"var(--muted)", fontSize:11, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", maxWidth:180 }}>{entry.sql}</span>
    </button>
  );
}

/* ─────────────────────────────────────────────────────────────────
   Shared primitives
───────────────────────────────────────────────────────────────── */
function SectionHeader({ icon, label, color }) {
  return (
    <div style={{ display:"flex", alignItems:"center", gap:10, padding:"8px 14px", background:"var(--surface)", border:"1px solid var(--border2)", borderRadius:"8px 8px 0 0", borderBottom:"1px solid var(--border)" }}>
      <span style={{ color, fontSize:13 }}>{icon}</span>
      <span style={{ color, fontSize:10, fontWeight:700, letterSpacing:".14em" }}>{label}</span>
    </div>
  );
}
function EmptyPanel({ icon, label, sub }) {
  return (
    <div style={{ display:"flex", flexDirection:"column", alignItems:"center", justifyContent:"center", gap:8, padding:"36px 0", color:"var(--muted)", opacity:.5, animation:"fadeUp .4s ease both", background:"var(--surface)", border:"1px solid var(--border2)", borderTop:"none", borderRadius:"0 0 8px 8px" }}>
      <div style={{ fontSize:28 }}>{icon}</div>
      <div style={{ fontSize:11, letterSpacing:".08em" }}>{label}</div>
      {sub && <div style={{ fontSize:10, opacity:.7 }}>{sub}</div>}
    </div>
  );
}

/* ─────────────────────────────────────────────────────────────────
   NEW ── ExplainPlan
───────────────────────────────────────────────────────────────── */
function ExplainPlan({ result }) {
  if (!result) return <EmptyPanel icon="⬡" label="RUN AN EXPLAIN QUERY" sub="e.g. EXPLAIN SELECT * FROM students WHERE id = 1" />;
  const plan = parseExplainOutput(result.raw);
  if (!plan) return <EmptyPanel icon="⬡" label="NO EXPLAIN DATA IN THIS RESULT" sub="Run: EXPLAIN SELECT * FROM students WHERE id = 1" />;

  const planColor = { 
    INDEX_HASH: "var(--accent)", 
    INDEX_BPTREE: "var(--amber)", 
    TABLE_SCAN: "var(--blue)",
    NESTED_LOOP: "var(--purple)" 
  }[plan.planType] ?? "var(--text)";

  const stages = [
    { id:"parser",    label:"PARSER",    icon:"✦", color:"var(--blue)",   desc:"Tokenise & validate SQL" },
    { id:"optimizer", label:"OPTIMIZER", icon:"⬡", color:"var(--purple)", desc:"Choose execution strategy" },
    { id:"strategy",  label:(plan.strategy ?? "EXECUTE").toUpperCase(), icon:"◈", color:planColor, 
      desc: plan.planType === "INDEX_BPTREE" ? "Leaf chain traversal" : 
            plan.planType === "INDEX_HASH"   ? "Direct bucket lookup" : 
            plan.planType === "NESTED_LOOP"  ? "Join cross-product"    : "Linear array scan" },
  ];

  return (
    <div style={{ animation:"fadeUp .35s ease both" }}>
      <SectionHeader icon="⬡" label="EXPLAIN PLAN" color="var(--purple)" />

      {/* Pipeline diagram */}
      <div style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderTop:"none", padding:"28px 24px", display:"flex", alignItems:"center", justifyContent:"center", gap:0, flexWrap:"wrap" }}>
        {stages.map((s, i) => (
          <div key={s.id} style={{ display:"flex", alignItems:"center" }}>
            <div style={{ display:"flex", flexDirection:"column", alignItems:"center", gap:8, animation:`nodeIn .4s ease both`, animationDelay:`${i*100}ms` }}>
              <div style={{ width:64, height:64, borderRadius:12, background:"#0f1218", border:`2px solid ${s.color}`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:22, boxShadow:`0 0 16px ${s.color}30`, position:"relative" }}>
                <span style={{ color:s.color }}>{s.icon}</span>
                {i === stages.length - 1 && (
                  <div style={{ position:"absolute", top:-3, right:-3, width:10, height:10, borderRadius:"50%", background:s.color, boxShadow:`0 0 8px ${s.color}`, animation:"pulse 1.5s ease infinite" }} />
                )}
              </div>
              <div style={{ textAlign:"center" }}>
                <div style={{ color:s.color, fontSize:9, fontWeight:700, letterSpacing:".12em" }}>{s.label}</div>
                <div style={{ color:"var(--muted)", fontSize:9, marginTop:2, maxWidth:90, textAlign:"center" }}>{s.desc}</div>
              </div>
            </div>
            {i < stages.length - 1 && (
              <div style={{ display:"flex", alignItems:"center", width:48, height:2, margin:"0 4px", marginBottom:32, position:"relative", overflow:"hidden" }}>
                <div style={{ position:"absolute", inset:0, background:`linear-gradient(90deg,${stages[i].color}80,${stages[i+1].color}80)`, animation:`lineGrow .5s ease both`, animationDelay:`${i*100+200}ms` }} />
                <div style={{ position:"absolute", right:0, width:0, height:0, borderTop:"5px solid transparent", borderBottom:"5px solid transparent", borderLeft:`6px solid ${stages[i+1].color}80`, marginTop:"-4px" }} />
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Metadata grid */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:1, background:"var(--border)", border:"1px solid var(--border2)", borderTop:"none" }}>
        {[
          { label:"PLAN TYPE",   value:plan.planType ?? "—", color:planColor },
          { label:"EST. COST",   value:plan.estCost  ?? "—", color:"var(--amber)" },
          { label:"RECORDS",     value:plan.records  ?? "—", color:"var(--accent)" },
          { label:"MATCHES",     value:plan.matches  ?? "—", color:"var(--red)" },
          { label:"TREE HEIGHT", value:plan.height   ?? "—", color:"var(--blue)" },
          { label:"INNER QUERY", value:plan.innerQ   ?? "—", color:"var(--text)" },
        ].map(({ label, value, color }) => (
          <div key={label} style={{ background:"var(--surface)", padding:"14px 18px", display:"flex", flexDirection:"column", gap:6 }}>
            <div style={{ color:"var(--muted)", fontSize:9, letterSpacing:".12em", fontWeight:700 }}>{label}</div>
            <div style={{ color, fontSize:12, fontWeight:500 }}>{value}</div>
          </div>
        ))}
      </div>

      {/* Reason */}
      {plan.reason && (
        <div style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderTop:"none", borderRadius:"0 0 8px 8px", padding:"12px 18px", display:"flex", gap:12, alignItems:"flex-start" }}>
          <span style={{ color:planColor, fontSize:14, flexShrink:0, marginTop:1 }}>ℹ</span>
          <div>
            <div style={{ color:"var(--muted)", fontSize:9, letterSpacing:".1em", fontWeight:700, marginBottom:4 }}>OPTIMIZER REASONING</div>
            <div style={{ color:"var(--text)", fontSize:11, lineHeight:1.7 }}>{plan.reason}</div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ─────────────────────────────────────────────────────────────────
   NEW ── IndexStats
───────────────────────────────────────────────────────────────── */
function IndexStats({ allResults }) {
  const raws  = allResults.map(r => r.raw);
  const stats = parseIndexStats(raws);
  const { rowCount, bpHeight, hashLoad } = stats;
  const hashUsed = Math.min(rowCount, 101);

  const tiles = [
    { label:"RECORDS",        value:rowCount,    unit:"rows",       icon:"▦", color:"var(--accent)", desc:"Total rows in table",                 bar:null },
    { label:"B+ TREE HEIGHT", value:bpHeight,    unit:bpHeight===1?"level":"levels", icon:"⬡", color:"var(--amber)", desc:`Order-4 · ~${rowCount} node${rowCount!==1?"s":""}`, bar:{ v:bpHeight, max:6, color:"var(--amber)" } },
    { label:"HASH TABLE",     value:`${hashLoad}%`, unit:`${hashUsed}/101 slots`, icon:"#", color:"var(--blue)", desc:"Separate chaining · 101 buckets", bar:{ v:hashLoad, max:100, color:hashLoad>70?"var(--red)":"var(--blue)" } },
  ];

  return (
    <div style={{ animation:"fadeUp .35s ease both" }}>
      <SectionHeader icon="▦" label="INDEX STATISTICS" color="var(--accent)" />

      {/* Stat tiles */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))", gap:1, background:"var(--border)", border:"1px solid var(--border2)", borderTop:"none" }}>
        {tiles.map((t, i) => (
          <div key={t.label} style={{ background:"var(--surface)", padding:"20px 22px", display:"flex", flexDirection:"column", gap:10, animation:`countUp .4s ease both`, animationDelay:`${i*80}ms` }}>
            <div style={{ display:"flex", alignItems:"center", gap:8 }}>
              <span style={{ width:28, height:28, borderRadius:6, background:`${t.color}18`, border:`1px solid ${t.color}40`, display:"flex", alignItems:"center", justifyContent:"center", color:t.color, fontSize:13, flexShrink:0 }}>{t.icon}</span>
              <span style={{ color:"var(--muted)", fontSize:9, letterSpacing:".12em", fontWeight:700 }}>{t.label}</span>
            </div>
            <div style={{ display:"flex", alignItems:"baseline", gap:6 }}>
              <span style={{ color:t.color, fontSize:28, fontWeight:700, letterSpacing:"-.02em", lineHeight:1, fontFamily:"'Syne',sans-serif" }}>{t.value}</span>
              <span style={{ color:"var(--muted)", fontSize:10 }}>{t.unit}</span>
            </div>
            {t.bar && (
              <div style={{ height:3, borderRadius:2, background:"var(--border2)", overflow:"hidden" }}>
                <div style={{ height:"100%", width:`${(t.bar.v/t.bar.max)*100}%`, background:t.bar.color, borderRadius:2, boxShadow:`0 0 6px ${t.bar.color}60`, transition:"width .6s ease" }} />
              </div>
            )}
            <div style={{ color:"var(--muted)", fontSize:10, lineHeight:1.5 }}>{t.desc}</div>
          </div>
        ))}
      </div>

      {/* B+ tree visualisation */}
      <div style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderTop:"1px solid var(--border)", borderRadius:"0 0 8px 8px", padding:"16px 22px", display:"flex", flexDirection:"column", gap:12 }}>
        <div style={{ color:"var(--muted)", fontSize:9, letterSpacing:".1em", fontWeight:700 }}>B+ TREE STRUCTURE  (ORDER 4 · ESTIMATED)</div>

        {rowCount === 0 ? (
          <div style={{ color:"var(--muted)", fontSize:10, opacity:.5, textAlign:"center", padding:"12px 0" }}>No records — run some INSERTs first</div>
        ) : (
          <>
            <div style={{ display:"flex", flexDirection:"column", alignItems:"center", gap:10 }}>
              {Array.from({ length: bpHeight }, (_, level) => {
                const isLeaf    = level === bpHeight - 1;
                const nodeCount = Math.min(Math.pow(4, level), 8);
                const extra     = Math.max(0, Math.pow(4, level) - 8);
                return (
                  <div key={level} style={{ display:"flex", alignItems:"center", gap:8 }}>
                    <span style={{ color:"var(--muted)", fontSize:9, width:14, textAlign:"right", flexShrink:0 }}>L{level}</span>
                    <div style={{ display:"flex", gap:4 }}>
                      {Array.from({ length: nodeCount }, (_, ni) => (
                        <div key={ni} style={{ width:isLeaf?18:22, height:isLeaf?10:14, borderRadius:3, background:isLeaf?"var(--amber)18":"var(--purple)18", border:`1px solid ${isLeaf?"var(--amber)":"var(--purple)"}50`, animation:`nodeIn .3s ease both`, animationDelay:`${(level*4+ni)*25}ms` }} />
                      ))}
                    </div>
                    {extra > 0 && <span style={{ color:"var(--muted)", fontSize:9 }}>+{extra}</span>}
                    {isLeaf && nodeCount > 1 && <span style={{ color:"var(--amber)", fontSize:8, opacity:.4 }}>← linked →</span>}
                  </div>
                );
              })}
            </div>
            <div style={{ display:"flex", gap:16 }}>
              {[{ c:"var(--purple)", l:"Internal" }, { c:"var(--amber)", l:"Leaf (data)" }].map(({ c, l }) => (
                <div key={l} style={{ display:"flex", alignItems:"center", gap:5 }}>
                  <div style={{ width:10, height:10, borderRadius:2, background:`${c}28`, border:`1px solid ${c}50` }} />
                  <span style={{ color:"var(--muted)", fontSize:9 }}>{l}</span>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

/* ─────────────────────────────────────────────────────────────────
   NEW ── TableViewer
───────────────────────────────────────────────────────────────── */
function TableViewer({ result }) {
  const [sortCol, setSortCol] = useState(null);
  const [sortAsc, setSortAsc] = useState(true);
  const [filter,  setFilter]  = useState("");
  const [hovRow,  setHovRow]  = useState(null);

  if (!result) return <EmptyPanel icon="⊞" label="RUN A SELECT QUERY TO VIEW TABLE DATA" />;
  const table = parseTableOutput(result.raw);
  if (!table || table.rows.length === 0) return <EmptyPanel icon="⊞" label="NO TABLE DATA IN THIS RESULT" sub={result.raw?.includes("0 rows") ? "Table is empty" : "Result is not a tabular query"} />;

  const filtered = filter.trim()
    ? table.rows.filter(row => row.some(c => c.toLowerCase().includes(filter.toLowerCase())))
    : table.rows;

  const sorted = sortCol === null ? filtered : [...filtered].sort((a, b) => {
    const av = a[sortCol], bv = b[sortCol];
    const an = parseFloat(av), bn = parseFloat(bv);
    const cmp = !isNaN(an) && !isNaN(bn) ? an - bn : av.localeCompare(bv);
    return sortAsc ? cmp : -cmp;
  });

  const colStats = table.headers.map((_, ci) => {
    const vals = table.rows.map(r => parseFloat(r[ci])).filter(v => !isNaN(v));
    return vals.length ? { min: Math.min(...vals), max: Math.max(...vals) } : null;
  });

  const handleSort = ci => {
    if (sortCol === ci) setSortAsc(a => !a);
    else { setSortCol(ci); setSortAsc(true); }
  };

  return (
    <div style={{ animation:"fadeUp .35s ease both" }}>
      <SectionHeader icon="⊞" label="TABLE VIEWER" color="var(--amber)" />

      {/* Toolbar */}
      <div style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderTop:"none", padding:"10px 14px", display:"flex", alignItems:"center", justifyContent:"space-between", gap:12 }}>
        <div style={{ display:"flex", alignItems:"center", gap:8, flex:1, maxWidth:280 }}>
          <span style={{ color:"var(--muted)", fontSize:12 }}>⌕</span>
          <input value={filter} onChange={e => setFilter(e.target.value)} placeholder="Filter rows…"
            style={{ flex:1, background:"var(--code-bg)", border:"1px solid var(--border2)", borderRadius:4, padding:"5px 10px", color:"var(--text)", fontFamily:"'JetBrains Mono',monospace", fontSize:11, outline:"none" }}
            onFocus={e => e.target.style.borderColor="var(--amber)"}
            onBlur={e  => e.target.style.borderColor="var(--border2)"} />
          {filter && <button onClick={() => setFilter("")} style={{ background:"none", border:"none", color:"var(--muted)", cursor:"pointer", fontSize:12 }}>✕</button>}
        </div>
        <div style={{ display:"flex", gap:12, alignItems:"center" }}>
          <span style={{ color:"var(--muted)", fontSize:10 }}>
            {sorted.length < table.rows.length
              ? <><span style={{ color:"var(--amber)" }}>{sorted.length}</span> / {table.rows.length} rows</>
              : <><span style={{ color:"var(--accent)" }}>{table.rows.length}</span> row{table.rows.length!==1?"s":""}</>}
          </span>
          {sortCol !== null && (
            <button onClick={() => { setSortCol(null); setSortAsc(true); }} style={{ background:"none", border:"1px solid var(--border2)", borderRadius:4, padding:"2px 8px", color:"var(--muted)", fontSize:9, cursor:"pointer", letterSpacing:".06em" }}>CLEAR SORT</button>
          )}
        </div>
      </div>

      {/* Table */}
      <div style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderTop:"none", borderRadius:"0 0 8px 8px", overflow:"hidden" }}>
        <div style={{ overflowX:"auto" }}>
          <table style={{ width:"100%", borderCollapse:"collapse" }}>
            <thead>
              <tr style={{ background:"#0f1218" }}>
                <th style={{ padding:"9px 10px", width:36, color:"var(--muted)", fontSize:9, fontWeight:400, letterSpacing:".06em", borderBottom:"1px solid var(--border2)", textAlign:"center" }}>#</th>
                {table.headers.map((h, ci) => (
                  <th key={h} onClick={() => handleSort(ci)} style={{ padding:"9px 16px", textAlign:"left", color:sortCol===ci?"var(--amber)":"var(--accent)", fontSize:10, fontWeight:700, letterSpacing:".12em", borderBottom:"1px solid var(--border2)", whiteSpace:"nowrap", cursor:"pointer", userSelect:"none", transition:"color .15s" }}
                    onMouseEnter={e => { if (sortCol!==ci) e.currentTarget.style.color="var(--text)"; }}
                    onMouseLeave={e => { if (sortCol!==ci) e.currentTarget.style.color="var(--accent)"; }}>
                    <div style={{ display:"flex", alignItems:"center", gap:5 }}>
                      {h}
                      <span style={{ fontSize:8, opacity:sortCol===ci?1:.3 }}>{sortCol===ci?(sortAsc?"▲":"▼"):"⇅"}</span>
                    </div>
                    {colStats[ci] && (
                      <div style={{ marginTop:3, height:2, borderRadius:1, background:"var(--border2)", overflow:"hidden" }}>
                        <div style={{ height:"100%", width:"60%", background:sortCol===ci?"var(--amber)":"var(--accent)", opacity:.35 }} />
                      </div>
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sorted.length === 0 ? (
                <tr><td colSpan={table.headers.length+1} style={{ padding:"24px 16px", textAlign:"center", color:"var(--muted)", fontSize:11 }}>No rows match "{filter}"</td></tr>
              ) : sorted.map((row, ri) => (
                <tr key={ri} onMouseEnter={() => setHovRow(ri)} onMouseLeave={() => setHovRow(null)}
                  style={{ borderBottom:ri<sorted.length-1?"1px solid var(--border)":"none", background:hovRow===ri?"var(--border)":"transparent", transition:"background .1s", animation:"rowIn .25s ease both", animationDelay:`${ri*30}ms` }}>
                  <td style={{ padding:"9px 10px", color:"var(--muted)", fontSize:10, textAlign:"center", fontVariantNumeric:"tabular-nums" }}>{ri+1}</td>
                  {row.map((cell, ci) => (
                    <td key={ci} style={{ padding:"9px 16px", color:ci===0?"var(--amber)":"var(--text)", fontSize:12, fontVariantNumeric:"tabular-nums" }}>{cell}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {colStats.some(Boolean) && (
          <div style={{ padding:"10px 14px", borderTop:"1px solid var(--border)", display:"flex", gap:20, flexWrap:"wrap" }}>
            {table.headers.map((h, ci) => colStats[ci] && (
              <div key={h} style={{ display:"flex", gap:5, alignItems:"center" }}>
                <span style={{ color:"var(--muted)", fontSize:9, letterSpacing:".06em" }}>{h}</span>
                <span style={{ color:"var(--accent)", fontSize:9 }}>{colStats[ci].min}</span>
                <span style={{ color:"var(--muted)", fontSize:8 }}>→</span>
                <span style={{ color:"var(--amber)", fontSize:9 }}>{colStats[ci].max}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

/* ─────────────────────────────────────────────────────────────────
   TabBar
───────────────────────────────────────────────────────────────── */
const TABS = [
  { id:"results", label:"RESULTS",     icon:"▤" },
  { id:"explain", label:"EXPLAIN",     icon:"⬡" },
  { id:"stats",   label:"INDEX STATS", icon:"▦" },
  { id:"viewer",  label:"TABLE VIEW",  icon:"⊞" },
];

function TabBar({ active, onSelect, result }) {
  const hasExplain = result?.raw?.includes("Query Plan:");
  const hasTable   = result ? !!parseTableOutput(result.raw) : false;
  return (
    <div style={{ display:"flex", gap:2, borderBottom:"1px solid var(--border)", background:"#0a0c0f" }}>
      {TABS.map(tab => {
        const isActive = active === tab.id;
        const badge    = (tab.id==="explain"&&hasExplain) || (tab.id==="viewer"&&hasTable);
        return (
          <button key={tab.id} onClick={() => onSelect(tab.id)}
            style={{ display:"flex", alignItems:"center", gap:6, padding:"9px 16px", background:isActive?"var(--surface)":"transparent", border:"none", borderBottom:isActive?"2px solid var(--accent)":"2px solid transparent", color:isActive?"var(--accent)":"var(--muted)", fontSize:10, fontWeight:700, letterSpacing:".1em", cursor:"pointer", transition:"all .15s", fontFamily:"'JetBrains Mono',monospace" }}
            onMouseEnter={e => { if (!isActive) e.currentTarget.style.color="var(--text)"; }}
            onMouseLeave={e => { if (!isActive) e.currentTarget.style.color="var(--muted)"; }}>
            <span style={{ fontSize:11 }}>{tab.icon}</span>
            {tab.label}
            {badge && !isActive && <span style={{ width:5, height:5, borderRadius:"50%", background:"var(--accent)", boxShadow:"0 0 4px var(--accent)", flexShrink:0 }} />}
          </button>
        );
      })}
    </div>
  );
}

/* ─────────────────────────────────────────────────────────────────
   ROOT APP
───────────────────────────────────────────────────────────────── */
export default function App() {
  const [sql,        setSql]        = useState("SELECT * FROM students");
  const [loading,    setLoading]    = useState(false);
  const [result,     setResult]     = useState(null);
  const [elapsed,    setElapsed]    = useState(null);
  const [history,    setHistory]    = useState([]);
  const [apiUrl,     setApiUrl]     = useState("http://localhost:5000");
  const [activeTab,  setActiveTab]  = useState("results");
  const [allResults, setAllResults] = useState([]);

  // Send a single SQL string to the backend, return { success, raw, error }
  const sendOne = async (query) => {
    let res;
    try {
      res = await fetch(`${apiUrl}/api/query`, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ query }),
      });
    } catch (networkErr) {
      throw new Error(
        `Cannot reach server at ${apiUrl}. ` +
        `Make sure the middleware is running (cd middleware && node server.js).`
      );
    }
    const ct = res.headers.get("content-type") ?? "";
    if (!ct.includes("application/json")) {
      const body    = await res.text();
      const preview = body.slice(0, 120).replace(/\s+/g, " ").trim();
      throw new Error(
        `Expected JSON from ${apiUrl}/api/query but got HTTP ${res.status}. ` +
        `Starts with: "${preview}". Check middleware port.`
      );
    }
    return res.json();
  };

  const runQuery = async () => {
    if (!sql.trim() || loading) return;

    // Split editor content into individual statements (one per non-empty line)
    const statements = sql
      .split("\n")
      .map(l => l.trim())
      .filter(l => l.length > 0 && !l.startsWith("--"));

    setLoading(true);
    setResult(null);
    const t0 = performance.now();

    try {
      let lastEntry = null;

      for (const stmt of statements) {
        let data;
        try {
          data = await sendOne(stmt);
        } catch (err) {
          const entry = { sql: stmt, success: false, raw: "", error: String(err) };
          setResult(entry);
          setHistory(h => [entry, ...h].slice(0, 30));
          setAllResults(h => [entry, ...h].slice(0, 50));
          setLoading(false);
          return;                    // stop on network/server error
        }

        const entry = {
          sql:     stmt,
          success: data.success,
          raw:     data.result ?? data.error ?? "",
          error:   data.success ? null : (data.error ?? "Unknown error"),
        };
        setHistory(h => [entry, ...h].slice(0, 30));
        setAllResults(h => [entry, ...h].slice(0, 50));
        lastEntry = entry;
      }

      // Show result of the LAST statement in the panel
      const ms = Math.round(performance.now() - t0);
      setElapsed(ms);
      if (lastEntry) {
        setResult(lastEntry);
        if (lastEntry.raw?.includes("Query Plan:")) setActiveTab("explain");
        else if (parseTableOutput(lastEntry.raw))    setActiveTab("results");
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      <style>{GLOBAL_STYLE}</style>
      <div style={{ pointerEvents:"none", position:"fixed", inset:0, zIndex:9999, backgroundImage:"repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,.03) 2px,rgba(0,0,0,.03) 4px)" }} />

      <div style={{ minHeight:"100vh", display:"grid", gridTemplateRows:"auto 1fr", gridTemplateColumns:"220px 1fr", gridTemplateAreas:'"header header" "sidebar main"' }}>

        {/* HEADER */}
        <header style={{ gridArea:"header", display:"flex", alignItems:"center", justifyContent:"space-between", padding:"12px 24px", background:"#0a0c0f", borderBottom:"1px solid var(--border)" }}>
          <div style={{ display:"flex", alignItems:"center", gap:14 }}>
            <div style={{ width:32, height:32, background:"var(--accent)", borderRadius:6, display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0 }}>
              <span style={{ color:"#0c0e11", fontSize:16, fontWeight:900, lineHeight:1 }}>Σ</span>
            </div>
            <div>
              <div style={{ fontFamily:"'Syne',sans-serif", fontSize:15, fontWeight:800, color:"#fff", letterSpacing:".02em" }}>MiniDB</div>
              <div style={{ color:"var(--muted)", fontSize:10, letterSpacing:".08em" }}>IN-MEMORY · B+TREE · HASH INDEX</div>
            </div>
          </div>
          <div style={{ display:"flex", alignItems:"center", gap:8 }}>
            <span style={{ color:"var(--muted)", fontSize:10, letterSpacing:".06em" }}>API</span>
            <input value={apiUrl} onChange={e => setApiUrl(e.target.value)}
              style={{ background:"var(--surface)", border:"1px solid var(--border2)", borderRadius:4, padding:"4px 10px", color:"var(--text)", fontFamily:"'JetBrains Mono',monospace", fontSize:11, outline:"none", width:200 }}
              onFocus={e => e.target.style.borderColor="var(--accent)"}
              onBlur={e  => e.target.style.borderColor="var(--border2)"} />
          </div>
        </header>

        {/* SIDEBAR */}
        <aside style={{ gridArea:"sidebar", background:"#0a0c0f", borderRight:"1px solid var(--border)", display:"flex", flexDirection:"column", overflow:"hidden" }}>
          <div style={{ padding:"12px 12px 8px", color:"var(--muted)", fontSize:10, letterSpacing:".1em", fontWeight:700, borderBottom:"1px solid var(--border)" }}>HISTORY</div>
          <div style={{ flex:1, overflowY:"auto", padding:"6px 4px" }}>
            {history.length === 0
              ? <div style={{ padding:"16px 12px", color:"var(--muted)", fontSize:11, lineHeight:1.6, opacity:.6 }}>Queries you run will appear here.</div>
              : history.map((e,i) => <HistoryItem key={i} entry={e} onClick={setSql} />)}
          </div>
          <div style={{ padding:"10px 12px", borderTop:"1px solid var(--border)", color:"var(--muted)", fontSize:10, lineHeight:1.8 }}>
            <div><kbd style={{ color:"var(--accent)" }}>Ctrl+Enter</kbd> Run</div>
            <div><kbd style={{ color:"var(--accent)" }}>Tab</kbd> Indent</div>
          </div>
        </aside>

        {/* MAIN */}
        <main style={{ gridArea:"main", padding:24, display:"flex", flexDirection:"column", gap:20, overflowY:"auto" }}>
          <QueryEditor sql={sql} setSql={setSql} onRun={runQuery} loading={loading} />

          {/* Tabbed info panel */}
          <div>
            <TabBar active={activeTab} onSelect={setActiveTab} result={result} />
            <div style={{ marginTop:0 }}>
              {activeTab === "results" && (
                result || loading
                  ? <ResultsTable result={result} loading={loading} elapsed={elapsed} />
                  : <EmptyPanel icon="⌗" label="RUN A QUERY TO SEE RESULTS" />
              )}
              {activeTab === "explain" && <ExplainPlan result={result} />}
              {activeTab === "stats"   && <IndexStats  allResults={allResults} />}
              {activeTab === "viewer"  && <TableViewer result={result} />}
            </div>
          </div>
        </main>
      </div>
    </>
  );
}