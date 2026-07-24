package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"runtime"
	"time"

	"github.com/Anmol202005/VScale/internal/gate/gateway"
	"github.com/Anmol202005/VScale/internal/gate/router"
	"github.com/Anmol202005/VScale/internal/gate/session"
	"github.com/Anmol202005/VScale/internal/topology"
	"github.com/Anmol202005/VScale/internal/vschema"
)

type Server struct {
	httpServer *http.Server
	mux        *http.ServeMux
	router     *router.Router
	sessMgr    *session.Manager
	gw         *gateway.Gateway
	topo       *topology.EtcdTopology
	vs         *vschema.VSchema
}

func New(addr string, r *router.Router, sm *session.Manager, gw *gateway.Gateway, topo *topology.EtcdTopology, vs *vschema.VSchema) *Server {
	mux := http.NewServeMux()
	s := &Server{
		mux:     mux,
		router:  r,
		sessMgr: sm,
		gw:      gw,
		topo:    topo,
		vs:      vs,
	}

	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/topology", s.handleTopology)
	mux.HandleFunc("/api/shards", s.handleShards)
	mux.HandleFunc("/api/sessions", s.handleSessions)
	mux.HandleFunc("/api/schema", s.handleSchema)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/gateway", s.handleGateway)

	s.httpServer = &http.Server{
		Addr:     addr,
		Handler:  mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return s
}

func (s *Server) Serve() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	tmpl := template.Must(template.New("admin").Parse(indexHTML))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, nil)
}

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	tablets := s.topo.Tablets()
	out := make([]topology.Tablet, len(tablets))
	copy(out, tablets)
	if out == nil {
		out = []topology.Tablet{}
	}
	resp := struct {
		Tablets []topology.Tablet `json:"tablets"`
		Count   int               `json:"count"`
	}{
		Tablets: out,
		Count:   len(out),
	}
	s.writeJSON(w, resp)
}

func (s *Server) handleShards(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, s.router.GetShardSummary())
}

func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, s.sessMgr.Summary())
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	resp := struct {
		Keyspace      string            `json:"keyspace"`
		ShardedTables map[string]string `json:"sharded_tables"`
	}{
		Keyspace:      s.vs.Keyspace,
		ShardedTables: s.vs.ShardedTables,
	}
	s.writeJSON(w, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	resp := struct {
		Status      string `json:"status"`
		Shards      int    `json:"shards"`
		Tablets     int    `json:"tablets"`
		Sessions    int    `json:"sessions"`
		GoRoutines  int    `json:"goroutines"`
		MemoryMB    uint64 `json:"memory_mb"`
		UptimeGuess string `json:"uptime_guess"`
	}{
		Status:      "ok",
		Shards:      s.router.ShardCount(),
		Tablets:     len(s.topo.Tablets()),
		Sessions:    s.sessMgr.Count(),
		GoRoutines:  runtime.NumGoroutine(),
		MemoryMB:    m.Alloc / 1024 / 1024,
		UptimeGuess: time.Now().UTC().Format(time.RFC3339),
	}
	s.writeJSON(w, resp)
}

func (s *Server) handleGateway(w http.ResponseWriter, r *http.Request) {
	resp := struct {
		HasCoordinator bool `json:"has_coordinator"`
		HasRouter      bool `json:"has_router"`
		HasSessionMgr  bool `json:"has_session_manager"`
	}{
		HasCoordinator: s.gw != nil,
		HasRouter:      s.router != nil,
		HasSessionMgr:  s.sessMgr != nil,
	}
	s.writeJSON(w, resp)
}

func (s *Server) writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
	}
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>VScale Admin</title>
	<style>
		:root { --bg: #0d1117; --panel: #161b22; --border: #30363d; --text: #c9d1d9; --muted: #8b949e; --ok: #238636; --warn: #d29922; --bad: #da3633; }
		* { box-sizing: border-box; margin: 0; padding: 0; }
		body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, sans-serif; background: var(--bg); color: var(--text); line-height: 1.45; }
		header { border-bottom: 1px solid var(--border); padding: 1rem 1.5rem; display: flex; align-items: center; justify-content: space-between; }
		header h1 { font-size: 1.1rem; letter-spacing: .2px; }
		.badge { font-size: .7rem; text-transform: uppercase; letter-spacing: .6px; padding: .2rem .5rem; border-radius: 999px; border: 1px solid var(--border); color: var(--muted); }
		main { max-width: 1100px; margin: 0 auto; padding: 1.25rem 1.5rem; }
		section { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; margin-bottom: 1rem; }
		section h2 { font-size: .85rem; text-transform: uppercase; letter-spacing: .6px; color: var(--muted); margin-bottom: .75rem; }
		.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: .75rem; }
		.card { background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: .75rem 1rem; }
		.card .value { font-size: 1.4rem; font-weight: 600; }
		.card .label { font-size: .75rem; color: var(--muted); margin-top: .15rem; }
		table { width: 100%; border-collapse: collapse; font-size: .85rem; }
		th, td { text-align: left; padding: .45rem .5rem; border-bottom: 1px solid var(--border); }
		th { color: var(--muted); font-weight: 500; white-space: nowrap; }
		tbody tr:last-child td { border-bottom: none; }
		.status-ok { color: var(--ok); }
		.status-warn { color: var(--warn); }
		.status-bad { color: var(--bad); }
		.tag { display: inline-block; font-size: .7rem; padding: .1rem .35rem; border-radius: 4px; background: var(--bg); border: 1px solid var(--border); margin-right: .25rem; }
		.empty { color: var(--muted); font-size: .85rem; padding: .5rem 0; }
		.refresh { cursor: pointer; background: transparent; border: 1px solid var(--border); color: var(--muted); padding: .25rem .6rem; border-radius: 4px; font-size: .75rem; }
		.refresh:hover { color: var(--text); border-color: var(--text); }
		pre { font-size: .78rem; color: var(--muted); overflow-x: auto; }
		@media (max-width: 640px) { main { padding: 1rem; } .grid { grid-template-columns: repeat(2, 1fr); } }
	</style>
</head>
<body>
<header>
	<h1>VScale <span style="color:var(--muted);font-weight:400;">Admin</span></h1>
	<div style="display:flex;gap:.5rem;align-items:center;">
		<span class="badge" id="statusBadge">checking</span>
		<button class="refresh" onclick="loadAll()">Refresh</button>
	</div>
</header>
<main>
	<section>
		<h2>Overview</h2>
		<div class="grid">
			<div class="card"><div class="value" id="shardCount">-</div><div class="label">Shards</div></div>
			<div class="card"><div class="value" id="tabletCount">-</div><div class="label">Tablets</div></div>
			<div class="card"><div class="value" id="sessionCount">-</div><div class="label">Sessions</div></div>
			<div class="card"><div class="value" id="goroutines">-</div><div class="label">Goroutines</div></div>
			<div class="card"><div class="value" id="mem">-</div><div class="label">Memory (MB)</div></div>
			<div class="card"><div class="value" id="uptime">-</div><div class="label">Timestamp</div></div>
		</div>
	</section>

	<section>
		<h2>Shards</h2>
		<div id="shardsBox"><div class="empty">Loading...</div></div>
	</section>

	<section>
		<h2>Topology</h2>
		<div id="topologyBox"><div class="empty">Loading...</div></div>
	</section>

	<section>
		<h2>Active Sessions</h2>
		<div id="sessionsBox"><div class="empty">Loading...</div></div>
	</section>

	<section>
		<h2>VSchema</h2>
		<pre id="schemaBox">Loading...</pre>
	</section>

	<section>
		<h2>Gateway</h2>
		<pre id="gatewayBox">Loading...</pre>
	</section>
</main>
<script>
async function get(path) {
	const r = await fetch(path);
	if (!r.ok) throw new Error(path + ' ' + r.status);
	return r.json();
}
function el(id) { return document.getElementById(id); }
function escapeHtml(t) {
	return String(t).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function arr(x) { return Array.isArray(x) ? x : []; }
async function getSafe(path) {
	try { return await get(path); } catch(e) { console.warn('admin', e); return null; }
}
async function loadHealth() {
	const d = await getSafe('/api/health');
	if (!d) return;
	el('shardCount').textContent = d.shards ?? '-';
	el('tabletCount').textContent = d.tablets ?? '-';
	el('sessionCount').textContent = d.sessions ?? '-';
	el('goroutines').textContent = d.goroutines ?? '-';
	el('mem').textContent = d.memory_mb ?? '-';
	el('uptime').textContent = d.uptime_guess ? new Date(d.uptime_guess).toLocaleTimeString() : '-';
	const badge = el('statusBadge');
	badge.textContent = d.status ?? 'unknown';
	badge.style.borderColor = d.status === 'ok' ? 'var(--ok)' : 'var(--warn)';
	badge.style.color = d.status === 'ok' ? 'var(--ok)' : 'var(--warn)';
}
async function loadShards() {
	const rows = arr(await getSafe('/api/shards'));
	if (!rows.length) { el('shardsBox').innerHTML = '<div class="empty">No shards found.</div>'; return; }
	let h = '<table><thead><tr><th>Keyspace</th><th>Shard</th><th>Range</th><th>Primary</th><th>Replicas</th></tr></thead><tbody>';
	for (const r of rows) {
		const reps = arr(r.replicas);
		const replicas = reps.length ? reps.map(a => '<span class="tag">' + escapeHtml(a) + '</span>').join('') : '<span style="color:var(--muted);">none</span>';
		h += '<tr><td>' + escapeHtml(r.keyspace ?? '') + '</td><td>' + escapeHtml(r.shard ?? '') + '</td><td>[' + (r.key_range_start ?? '') + ', ' + (r.key_range_end ?? '') + ')</td><td>' + (r.primary ? escapeHtml(r.primary) : '<span style="color:var(--warn)">none</span>') + '</td><td>' + replicas + '</td></tr>';
	}
	h += '</tbody></table>';
	el('shardsBox').innerHTML = h;
}
async function loadTopology() {
	const d = await getSafe('/api/topology');
	const tablets = arr(d?.tablets);
	if (!tablets.length) { el('topologyBox').innerHTML = '<div class="empty">No tablets registered.</div>'; return; }
	let h = '<table><thead><tr><th>Addr</th><th>Cell</th><th>Keyspace</th><th>Shard</th><th>Type</th><th>Range</th></tr></thead><tbody>';
	for (const t of tablets) {
		const typeClass = t.type === 'PRIMARY' ? 'status-ok' : t.type === 'REPLICA' ? 'status-warn' : '';
		h += '<tr><td>' + escapeHtml(t.addr ?? '') + '</td><td>' + escapeHtml(t.cell ?? '') + '</td><td>' + escapeHtml(t.keyspace ?? '') + '</td><td>' + escapeHtml(t.shard ?? '') + '</td><td class="' + typeClass + '">' + escapeHtml(t.type ?? '') + '</td><td>[' + (t.key_range_start ?? '') + ', ' + (t.key_range_end ?? '') + ')</td></tr>';
	}
	h += '</tbody></table>';
	el('topologyBox').innerHTML = h;
}
async function loadSessions() {
	const d = await getSafe('/api/sessions');
	const sessions = arr(d?.sessions);
	if (!sessions.length) { el('sessionsBox').innerHTML = '<div class="empty">No active sessions.</div>'; return; }
	let h = '<table><thead><tr><th>ID</th><th>State</th><th>Shards</th><th>Shard Addrs</th></tr></thead><tbody>';
	for (const s of sessions) {
		const stateClass = s.state === 'in_transaction' ? 'status-warn' : s.state === 'committed' ? 'status-ok' : s.state === 'rolled_back' ? 'status-bad' : '';
		const addrs = arr(s.shard_addrs);
		const addrTags = addrs.length ? addrs.map(a => '<span class="tag">' + escapeHtml(a) + '</span>').join('') : '<span style="color:var(--muted)">-</span>';
		h += '<tr><td>' + (s.id ?? '') + '</td><td class="' + stateClass + '">' + (s.state ?? '') + '</td><td>' + (s.shards ?? '') + '</td><td>' + addrTags + '</td></tr>';
	}
	h += '</tbody></table>';
	el('sessionsBox').innerHTML = h;
}
async function loadSchema() {
	const d = await getSafe('/api/schema');
	el('schemaBox').textContent = d ? JSON.stringify(d, null, 2) : 'Failed to load';
}
async function loadGateway() {
	const d = await getSafe('/api/gateway');
	el('gatewayBox').textContent = d ? JSON.stringify(d, null, 2) : 'Failed to load';
}
async function loadAll() {
	await Promise.all([loadHealth(), loadShards(), loadTopology(), loadSessions(), loadSchema(), loadGateway()]);
}
loadAll();
setInterval(loadAll, 5000);
</script>
</body>
</html>`
