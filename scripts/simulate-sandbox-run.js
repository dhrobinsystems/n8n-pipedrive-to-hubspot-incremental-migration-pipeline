#!/usr/bin/env node
/*
 * simulate-sandbox-run.js
 * ---------------------------------------------------------------------------
 * Offline simulator for n8n-pipedrive-to-hubspot-incremental-migration-pipeline.
 *
 * Zero dependencies and zero network modules: only fs, path and vm are
 * required. Code-node JavaScript is taken verbatim from workflow.json and run
 * in isolated VM contexts by a small engine that follows workflow.json's
 * connections, evaluates IF / HTTP {{ expressions }}, emulates cursor
 * pagination and n8n workflow static data. HTTP nodes are routed to in-memory
 * Pipedrive v2 and HubSpot mocks. The HubSpot mock enforces unique
 * pipedrive_*_id properties and unique contact emails, so a non-idempotent
 * write path produces duplicates or errors and fails the tests.
 *
 * Verifies: structure and static safety, safe defaults and config validation,
 * fixture parity, every sandbox scenario (create / update / unchanged /
 * quarantine, email conflicts, FK resolution, pending associations, activity
 * type mapping), live dry-run reports, armed migration, rerun and crash
 * recovery without duplicates, incremental watermarks, backlog paging, write
 * failures holding watermarks, fail-closed extraction and preflight, corrupt
 * state handling and run guards.
 *
 * Usage:  node scripts/simulate-sandbox-run.js [--quiet]
 * Exit:   0 when every check passes, 1 otherwise.
 * Output: sim-output/ (git-ignored): validation report, run reports, state.
 *
 * Full text: DISCLAIMER.md · Licensed under the MIT License. License: MIT © 2026 dhRobin Systems · Provided as-is, see the disclaimer.
 * ---------------------------------------------------------------------------
 */
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

globalThis.fetch = () => { throw new Error('Network access is not allowed in the simulator'); };

const ROOT = path.resolve(__dirname, '..');
const WORKFLOW_PATH = path.join(ROOT, 'workflow.json');
const FIXTURE_PATH = path.join(ROOT, 'sample-payloads', 'pipedrive-fixture.json');
const OUT_DIR = path.join(ROOT, 'sim-output');
const QUIET = process.argv.includes('--quiet');
const workflow = JSON.parse(fs.readFileSync(WORKFLOW_PATH, 'utf8'));
const fixture = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8'));
const byName = Object.fromEntries(workflow.nodes.map((n) => [n.name, n]));
const clone = (v) => (v === undefined ? v : JSON.parse(JSON.stringify(v)));

let passed = 0, failed = 0;
const failures = [];
function check(label, cond, detail) {
  if (cond) { passed++; if (!QUIET) console.log(`  ✔ ${label}`); }
  else { failed++; failures.push(label + (detail ? ` -- ${detail}` : '')); console.log(`  ✘ ${label}${detail ? `\n      ${detail}` : ''}`); }
}
const section = (t) => { if (!QUIET) console.log(`\n▶ ${t}`); };

// ===================================================================== mocks
const TOKENS = {
  PIPEDRIVE_API_TOKEN: 'sim-placeholder-pipedrive-token-0000',
  HUBSPOT_PRIVATE_APP_TOKEN: 'sim-placeholder-hubspot-token-0000',
};
class HttpError extends Error { constructor(status, message) { super(`${status} - ${message}`); this.status = status; } }
const UNIQUE = { companies: ['pipedrive_org_id'], contacts: ['pipedrive_person_id', 'email'], deals: ['pipedrive_deal_id'], calls: ['pipedrive_activity_id'], meetings: ['pipedrive_activity_id'], tasks: ['pipedrive_activity_id'] };
const ASSOC_OK = new Set(['contacts|companies', 'deals|companies', 'deals|contacts', 'calls|deals', 'calls|contacts', 'calls|companies', 'meetings|deals', 'meetings|contacts', 'meetings|companies', 'tasks|deals', 'tasks|contacts', 'tasks|companies']);
function makeMocks(opts = {}) {
  const pd = clone(opts.pipedrive || fixture.pipedrive);
  const hs = {};
  let nextId = 80000001;
  for (const [t, list] of Object.entries(clone(opts.hubspot || fixture.hubspotExisting))) hs[t] = new Map(list.map((o) => [String(o.id), { id: String(o.id), properties: { ...o.properties } }]));
  for (const t of Object.keys(UNIQUE)) hs[t] = hs[t] || new Map();
  const assoc = new Set();
  const state = { calls: [], pd, hs, assoc, writes: 0 };
  const findBy = (t, prop, val) => [...hs[t].values()].find((o) => o.properties[prop] !== undefined && o.properties[prop] !== null && String(o.properties[prop]).toLowerCase() === String(val).toLowerCase());
  function handle({ method, url, headers, body, node }) {
    const u = new URL(url);
    state.calls.push({ node, method, host: u.host, path: u.pathname, query: u.search, headers: clone(headers), body: clone(body) });
    if (u.host === 'api.pipedrive.com') {
      if (headers['x-api-token'] !== TOKENS.PIPEDRIVE_API_TOKEN) throw new HttpError(401, 'unauthorized');
      const m = u.pathname.match(/^\/api\/v2\/(organizations|persons|deals|activities)$/);
      if (!m || method !== 'GET') throw new HttpError(404, 'unknown route');
      const ent = m[1];
      if (opts.failEntity === ent) throw new HttpError(500, 'Pipedrive internal error');
      if (opts.malformedEntity === ent) return { success: false, error: 'simulated malformed page' };
      const since = u.searchParams.get('updated_since') || '';
      const limit = Number(u.searchParams.get('limit') || 100);
      if (limit > 500) throw new HttpError(400, 'limit > 500');
      if (u.searchParams.get('sort_by') !== 'update_time') throw new HttpError(400, 'expected sort_by=update_time');
      const statuses = (u.searchParams.get('status') || '').split(',').filter(Boolean);
      const all = (pd[ent] || []).filter((r) => r.update_time >= since).filter((r) => ent !== 'deals' || !statuses.length || statuses.includes(r.status))
        .sort((a, b) => (a.update_time < b.update_time ? -1 : a.update_time > b.update_time ? 1 : a.id - b.id));
      const off = Number(u.searchParams.get('cursor') || 0);
      const data = all.slice(off, off + limit);
      return { success: true, data: clone(data), additional_data: { next_cursor: off + limit < all.length ? String(off + limit) : null } };
    }
    if (u.host !== 'api.hubapi.com') throw new HttpError(502, 'unmocked host ' + u.host);
    if (headers.Authorization !== `Bearer ${TOKENS.HUBSPOT_PRIVATE_APP_TOKEN}`) throw new HttpError(401, 'bad token');
    let m = u.pathname.match(/^\/crm\/v3\/objects\/([a-z]+)\/batch\/(read|upsert|update)$/);
    if (m && method === 'POST') {
      const [, t, op] = m;
      if (!hs[t]) throw new HttpError(400, 'unknown object type ' + t);
      if (body.inputs.length > 100) throw new HttpError(400, 'batch > 100');
      if (op === 'read') {
        if (opts.failLookup) throw new HttpError(503, 'lookup unavailable');
        if (body.idProperty !== 'email' && !UNIQUE[t].includes(body.idProperty)) throw new HttpError(400, `${body.idProperty} is not a unique property`);
        const results = body.inputs.map((i) => findBy(t, body.idProperty, i.id)).filter(Boolean).map((o) => ({ id: o.id, properties: Object.fromEntries(body.properties.map((p) => [p, o.properties[p] === undefined ? null : o.properties[p]])) }));
        return { status: 'COMPLETE', results };
      }
      if (opts.failWriteType === t) throw new HttpError(500, `simulated ${t} write outage`);
      state.writes++;
      // validate uniqueness across the whole batch first (HubSpot rejects the batch)
      const plan = body.inputs.map((i) => {
        const existing = op === 'upsert' ? findBy(t, i.idProperty, i.id) : hs[t].get(String(i.id));
        if (op === 'update' && !existing) throw new HttpError(404, `object ${i.id} not found`);
        for (const up of UNIQUE[t]) {
          const v = i.properties[up];
          if (v === undefined || v === null || v === '') continue;
          const clash = findBy(t, up, v);
          if (clash && (!existing || clash.id !== existing.id)) throw new HttpError(409, `${t === 'contacts' && up === 'email' ? 'Contact already exists' : 'Duplicate unique value'}. Existing ID: ${clash.id}`);
        }
        return { i, existing };
      });
      const results = plan.map(({ i, existing }) => {
        if (existing) { Object.assign(existing.properties, i.properties); return { id: existing.id, properties: { ...existing.properties }, new: false }; }
        const id = String(nextId++);
        const o = { id, properties: { ...i.properties } };
        if (op === 'upsert') o.properties[i.idProperty] = String(i.id);
        hs[t].set(id, o);
        return { id, properties: { ...o.properties }, new: true };
      });
      return { status: 'COMPLETE', results };
    }
    m = u.pathname.match(/^\/crm\/v4\/associations\/([a-z]+)\/([a-z]+)\/batch\/associate\/default$/);
    if (m && method === 'POST') {
      const [, from, to] = m;
      if (!ASSOC_OK.has(`${from}|${to}`)) throw new HttpError(400, `no default association ${from} -> ${to}`);
      if (opts.failAssoc) throw new HttpError(500, 'association outage');
      for (const p of body.inputs) {
        if (!hs[from].has(String(p.from.id)) || !hs[to].has(String(p.to.id))) throw new HttpError(400, `association endpoint missing ${from}:${p.from.id} -> ${to}:${p.to.id}`);
        assoc.add(`${from}:${p.from.id}->${to}:${p.to.id}`);
      }
      return { status: 'COMPLETE', results: body.inputs.map((p) => ({ from: p.from, to: p.to })) };
    }
    throw new HttpError(404, `unmocked route ${method} ${u.pathname}`);
  }
  return { state, handle };
}
// ==================================================================== engine
function evalExpr(template, ctx) {
  if (typeof template !== 'string' || !template.startsWith('=')) return template;
  const t = template.slice(1);
  const run = (expr) => vm.runInNewContext(`(${expr})`, { ...ctx, encodeURIComponent, JSON, String, Object, Number }, { timeout: 2000 });
  const single = t.match(/^\{\{([\s\S]*)\}\}$/);
  if (single && !single[1].includes('}}')) return run(single[1]);
  return t.replace(/\{\{([\s\S]*?)\}\}/g, (_, e) => { const v = run(e); return v === undefined || v === null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v); });
}
function runWorkflow({ trigger = 'manual', webhookItem, env = {}, mocks, staticData = {} }) {
  const runData = {};
  let webhookResponse = null;
  const envProxy = new Proxy({}, { get(_t, k) { if (env === 'BLOCKED') throw new Error('access to env vars denied'); return typeof k === 'string' ? env[k] : undefined; } });
  const accessor = (name) => {
    const runs = runData[name];
    if (!runs || !runs.length) throw new Error(`Referenced node "${name}" has not executed`);
    const items = (runs[runs.length - 1][0] || []).map(clone);
    return { first: () => items[0], last: () => items[items.length - 1], all: () => items, item: items[0] };
  };
  const ctxFor = (item) => ({ $json: clone(item.json), $: accessor, $env: envProxy });
  function exec(name, items) {
    const node = byName[name];
    let outputs;
    switch (node.type) {
      case 'n8n-nodes-base.manualTrigger': case 'n8n-nodes-base.webhook': case 'n8n-nodes-base.scheduleTrigger':
        outputs = [items]; break;
      case 'n8n-nodes-base.respondToWebhook':
        webhookResponse = { statusCode: node.parameters.options.responseCode || 200, body: JSON.parse(evalExpr(node.parameters.responseBody, ctxFor(items[0]))) };
        outputs = [items]; break;
      case 'n8n-nodes-base.code': {
        const sandbox = {
          $input: { first: () => clone(items[0]), all: () => items.map(clone), item: clone(items[0]) },
          $env: envProxy, $: accessor, $getWorkflowStaticData: () => staticData,
          require: (m) => { throw new Error(`Cannot find module '${m}'`); },
          console: { log() {}, warn() {}, error() {} },
        };
        const res = vm.runInNewContext(`(() => {\n${node.parameters.jsCode}\n})()`, sandbox, { timeout: 20000, filename: `${name}.js` });
        if (!Array.isArray(res)) throw new Error(`Code node "${name}" did not return an array`);
        outputs = [res.map((i) => clone({ json: i.json }))];
        break;
      }
      case 'n8n-nodes-base.if': {
        const t = [], f = [];
        for (const it of items) (evalExpr(node.parameters.conditions.conditions[0].leftValue, ctxFor(it)) === true ? t : f).push(it);
        outputs = [t, f]; break;
      }
      case 'n8n-nodes-base.httpRequest': {
        const out = [];
        const pg = node.parameters.options && node.parameters.options.pagination && node.parameters.options.pagination.pagination;
        for (const it of items) {
          const c = ctxFor(it);
          let url = evalExpr(node.parameters.url, c);
          const headers = Object.fromEntries((node.parameters.headerParameters?.parameters || []).map((h) => [h.name, evalExpr(h.value, c)]));
          let body = node.parameters.sendBody ? JSON.parse(evalExpr(node.parameters.jsonBody, c)) : undefined;
          const send = () => mocks.handle({ method: node.parameters.method || 'GET', url, headers, body, node: name });
          try {
            if (!pg) { out.push({ json: send() }); continue; }
            for (let n = 0; n < Number(evalExpr(pg.maxRequests, c)); n++) {
              const resp = send();
              out.push({ json: resp });
              const rc = { ...c, $response: { body: resp } };
              if (evalExpr(pg.completeExpression, rc) === true) break;
              for (const prm of pg.parameters.parameters) {
                const v = evalExpr(prm.value, rc);
                if (prm.type === 'body') body = { ...body, [prm.name]: v };
                if (prm.type === 'qs') { const uu = new URL(url); uu.searchParams.set(prm.name, v); url = uu.toString(); }
              }
            }
          } catch (e) {
            if (node.onError === 'continueRegularOutput') out.push({ json: { error: { message: e.message, status: e.status } } });
            else throw Object.assign(new Error(`${name}: ${e.message}`), { node: name });
          }
        }
        outputs = [out]; break;
      }
      default: throw new Error(`Engine does not support ${node.type}`);
    }
    (runData[name] = runData[name] || []).push(outputs.map((o) => (o || []).map(clone)));
    const conns = (workflow.connections[name] || {}).main || [];
    outputs.forEach((o, idx) => { if (o && o.length) for (const cn of conns[idx] || []) exec(cn.node, o); });
  }
  let error = null;
  try {
    if (trigger === 'schedule') exec('Incremental Sync (hourly :17)', [{ json: { timestamp: '2026-11-17T10:17:00.000+00:00' } }]);
    else exec('Manual Test Trigger', [{ json: {} }]);
  } catch (e) { error = e; }
  const last = (n) => (runData[n] ? runData[n][runData[n].length - 1][0].map((i) => i.json) : undefined);
  return { runData, error, last, mocks, staticData, webhookResponse };
}


// ============================================================== helpers
const NOW = '2026-11-17T10:00:00.000Z';
const FOOT = 'Full text: DISCLAIMER.md · Licensed under the MIT License. License: MIT © 2026 dhRobin Systems · Provided as-is, see the disclaimer.';
const MAPS = { OWNER_MAP_JSON: JSON.stringify(fixture.sandboxMapping.ownerMap), STAGE_MAP_JSON: JSON.stringify(fixture.sandboxMapping.stageMap), PIPEDRIVE_ORG_DOMAIN_FIELD: fixture._meta.pipedriveDomainFieldKey };
const LIVE = { SANDBOX_MODE: 'false', NOW_OVERRIDE: NOW, ...TOKENS, ...MAPS };
const ARMED = { ...LIVE, DRY_RUN: 'false', HUBSPOT_WRITES_ENABLED: 'true' };
const seedSd = () => ({ migration: clone(fixture.stateSeed) });
const run = (env, mocks, staticData = seedSd()) => runWorkflow({ env, mocks, staticData });
const planOf = (r) => r.last('Migration Plan & Validation Report')[0];
const rec = (r, ent, id) => planOf(r).planned.find((x) => x.entity === ent && x.pdId === String(id));
const reportOf = (r) => r.last('Migration Run Report')[0];
const hsFind = (m, t, prop, val) => [...m.state.hs[t].values()].filter((o) => String(o.properties[prop]) === String(val));
const counts = (m) => Object.fromEntries(Object.entries(m.state.hs).map(([k, v]) => [k, v.size]));
const hsIdOf = (m, t, prop, val) => (hsFind(m, t, prop, val)[0] || {}).id;
const linked = (m, from, fid, to, tid) => m.state.assoc.has(`${from}:${fid}->${to}:${tid}`);
function noDupes(m) {
  for (const [t, props] of Object.entries(UNIQUE)) for (const p of props) {
    const vals = [...m.state.hs[t].values()].map((o) => o.properties[p]).filter((v) => v !== undefined && v !== null && v !== '').map((v) => String(v).toLowerCase());
    if (new Set(vals).size !== vals.length) return `${t}.${p}`;
  }
  return null;
}

// ====================================================================== main
(function main() {
  console.log('n8n-pipedrive-to-hubspot-incremental-migration-pipeline :: offline simulator');
  console.log(`workflow: workflow.json (${workflow.nodes.length} nodes)`);
  fs.rmSync(OUT_DIR, { recursive: true, force: true });
  fs.mkdirSync(OUT_DIR, { recursive: true });

  // ----------------------------------------------------------- 1. structure
  section('1. Workflow structure and static safety checks');
  const names = workflow.nodes.map((n) => n.name);
  check('Node names are unique', new Set(names).size === names.length);
  check('Workflow is inactive on import', workflow.active === false);
  const bad = [];
  for (const [s, c] of Object.entries(workflow.connections)) { if (!byName[s]) bad.push(s); for (const b of c.main || []) for (const x of b) if (!byName[x.node]) bad.push(`${s}->${x.node}`); }
  check('All connections reference existing nodes', bad.length === 0, bad.join(', '));
  const ifs = workflow.nodes.filter((n) => n.type === 'n8n-nodes-base.if');
  check(`All ${ifs.length} IF nodes wire both outputs`, ifs.every((n) => (workflow.connections[n.name]?.main || []).length === 2 && workflow.connections[n.name].main.every((b) => b.length)));
  const reach = new Set();
  const walk = (n) => { if (reach.has(n)) return; reach.add(n); for (const b of workflow.connections[n]?.main || []) for (const c of b) walk(c.node); };
  ['Manual Test Trigger', 'Incremental Sync (hourly :17)'].forEach(walk);
  const unreach = workflow.nodes.filter((n) => n.type !== 'n8n-nodes-base.stickyNote' && !reach.has(n.name)).map((n) => n.name);
  check('Every functional node is reachable from a trigger', unreach.length === 0, unreach.join(', '));
  const http = workflow.nodes.filter((n) => n.type === 'n8n-nodes-base.httpRequest');
  const pdNodes = http.filter((n) => n.name.startsWith('Pipedrive:'));
  const hsNodes = http.filter((n) => n.name.startsWith('HubSpot:'));
  check(`${pdNodes.length} Pipedrive nodes: GET only, token only via {{ $env.PIPEDRIVE_API_TOKEN }} header`, pdNodes.length === 4 && pdNodes.every((n) => n.parameters.method === 'GET' && n.parameters.headerParameters.parameters.find((h) => h.name === 'x-api-token').value === '={{ $env.PIPEDRIVE_API_TOKEN }}'));
  check(`${hsNodes.length} HubSpot nodes authenticate only via Bearer {{ $env.HUBSPOT_PRIVATE_APP_TOKEN }}`, hsNodes.length === 3 && hsNodes.every((n) => n.parameters.headerParameters.parameters.find((h) => h.name === 'Authorization').value === '=Bearer {{ $env.HUBSPOT_PRIVATE_APP_TOKEN }}'));
  check('Preflight lookup stops the run on failure (no continue-on-fail); writes continue and record failures', !byName['HubSpot: Preflight Batch Read'].onError && byName['HubSpot: Batch Upsert'].onError === 'continueRegularOutput');
  check('No stored n8n credentials', workflow.nodes.every((n) => !n.credentials));
  const codeSrc = workflow.nodes.filter((n) => n.type === 'n8n-nodes-base.code').map((n) => [n.name, n.parameters.jsCode]);
  const writers = codeSrc.filter(([, s]) => /batch\/(\$\{mode\}|upsert|update|create|archive)|batch\/associate/.test(s.replace(/\/\/.*$/gm, ''))).map(([n]) => n).sort();
  check('Only "Build Upsert Batches" and "Build Association Batches" build HubSpot write requests', writers.join() === 'Build Association Batches,Build Upsert Batches', writers.join());
  check('Pipedrive is never written to (no POST / PUT / PATCH / DELETE to Pipedrive)', !/method:\s*'(POST|PUT|PATCH|DELETE)'[^}]*pipedrive/i.test(JSON.stringify(pdNodes)) && codeSrc.every(([, s]) => !/api\/v[12]\/[a-z]+\/\d+/.test(s)));
  check(`Every Code node starts with the license footer line (${codeSrc.length} nodes)`, codeSrc.every(([, s]) => s.startsWith(`// ${FOOT}\n`)));
  check('Simulator header carries the license footer', fs.readFileSync(__filename, 'utf8').includes(FOOT));
  const raw = fs.readFileSync(WORKFLOW_PATH, 'utf8');
  const PAT = [[/pat-(na|eu|ap)\d?-[0-9a-f-]{20,}/i, 'HubSpot private app token'], [/\b[0-9a-f]{40}\b(?![\s\S]{0,40}placeholderwebsitekey)/, '40-hex Pipedrive token'], [/api_token=[A-Za-z0-9]{10,}/, 'token in URL'], [/Bearer (?!your_[a-z_]+_here)[A-Za-z0-9\-_.]{16,}/, 'literal bearer'], [/hooks\.slack\.com\/services\//, 'webhook URL'], [/\b(?!555)\d{3}[\s.-]\d{3}[\s.-]\d{4}\b/, 'phone number (non-555)']];
  const texts = { 'workflow.json': raw };
  for (const f of ['README.md', 'DISCLAIMER.md', '.env.example', 'sample-payloads/pipedrive-fixture.json']) if (fs.existsSync(path.join(ROOT, f))) texts[f] = fs.readFileSync(path.join(ROOT, f), 'utf8');
  const hits = [];
  for (const [f, t] of Object.entries(texts)) for (const [re, l] of PAT) if (re.test(t)) hits.push(`${l} in ${f}`);
  check('No tokens, token-bearing URLs or real-looking phone numbers in shipped files', hits.length === 0, hits.join(', '));
  const emails = [...new Set((raw + texts['sample-payloads/pipedrive-fixture.json']).match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [])];
  check('Every email in workflow / fixture uses example.com', emails.length > 0 && emails.every((e) => /@example\.com$/i.test(e)), emails.join(','));
  const exprErr = [];
  const visit = (v, w) => { if (typeof v === 'string' && v.startsWith('=')) for (const mm of v.matchAll(/\{\{([\s\S]*?)\}\}/g)) { try { new Function('$json', '$', '$env', '$response', `return (${mm[1]});`); } catch (e) { exprErr.push(`${w}: ${e.message}`); } } else if (v && typeof v === 'object') for (const [k, x] of Object.entries(v)) visit(x, `${w}.${k}`); };
  workflow.nodes.forEach((n) => visit(n.parameters, n.name));
  check('Every {{ expression }} parses as JavaScript', exprErr.length === 0, exprErr.join('; '));
  const cErr = [];
  for (const [n, s] of codeSrc) { try { new vm.Script(`(() => {\n${s}\n})`); } catch (e) { cErr.push(`${n}: ${e.message}`); } }
  check('Every Code node compiles', cErr.length === 0, cErr.join('; '));
  check('Code nodes call no require() and read no tokens from $env (outside Config)', codeSrc.every(([, s]) => !/\brequire\s*\(/.test(s)) && codeSrc.filter(([n]) => n !== 'Config & Safety Gate').every(([, s]) => !/\$env/.test(s.replace(/\/\/.*$/gm, ''))));

  // ------------------------------------------------------ 2. config
  section('2. Safe defaults and config validation');
  const cfgOf = (env) => { const r = runWorkflow({ env, mocks: makeMocks() }); return { r, c: r.last('Config & Safety Gate') && r.last('Config & Safety Gate')[0].config }; };
  const { c: d0 } = cfgOf({ NOW_OVERRIDE: NOW });
  check('Empty env -> SANDBOX + DRY_RUN, writes blocked, associations on, all four entities', d0.sandboxMode && d0.dryRun && !d0.writes.allowedByConfig && d0.writes.associationsEnabled && d0.entities.join() === 'organizations,persons,deals,activities');
  check('Defaults: page size 500, batch 100, 5000 writes / run, 25% quarantine guard, quarantine email conflicts', d0.pipedrive.pageSize === 500 && d0.hubspot.batchSize === 100 && d0.guards.maxWritesPerRun === 5000 && d0.guards.maxQuarantinePct === 25 && d0.mapping.emailConflictPolicy === 'quarantine');
  check('Empty STAGE_MAP_JSON is warned about (deals would be quarantined, never guessed)', d0.warnings.some((w) => /STAGE_MAP_JSON is empty/.test(w)));
  const { c: typo } = cfgOf({ NOW_OVERRIDE: NOW, SANDBOX_MODE: 'flase', DRY_RUN: 'nah', HUBSPOT_WRITES_ENABLED: 'please' });
  check('Typos keep SANDBOX / DRY_RUN on and writes off', typo.sandboxMode && typo.dryRun && !typo.writes.enabled);
  const { c: blk } = cfgOf('BLOCKED');
  check('Blocked $env -> sandbox, writes blocked', blk.envBlocked && blk.sandboxMode && !blk.writes.allowedByConfig);
  const t1 = runWorkflow({ env: { SANDBOX_MODE: 'false', HUBSPOT_PRIVATE_APP_TOKEN: 'x' }, mocks: makeMocks() });
  check('Live without PIPEDRIVE_API_TOKEN is refused before any call', t1.error && /PIPEDRIVE_API_TOKEN/.test(t1.error.message) && t1.mocks.state.calls.length === 0);
  const t2 = runWorkflow({ env: { SANDBOX_MODE: 'false', PIPEDRIVE_API_TOKEN: 'x' }, mocks: makeMocks() });
  check('Live without HUBSPOT_PRIVATE_APP_TOKEN is refused (needed for the read-only preflight)', t2.error && /HUBSPOT_PRIVATE_APP_TOKEN/.test(t2.error.message));
  const { c: hb } = cfgOf({ NOW_OVERRIDE: NOW, PIPEDRIVE_API_BASE_URL: 'https://api.pipedrive.com.attacker.example', HUBSPOT_API_BASE_URL: 'https://user:pw@api.hubapi.com' });
  check('Non-allowlisted Pipedrive host and credentialed HubSpot URL are config errors', hb.configErrors.length >= 2 && !hb.writes.allowedByConfig);
  const { c: bj } = cfgOf({ NOW_OVERRIDE: NOW, OWNER_MAP_JSON: '{"abc":"1"}', STAGE_MAP_JSON: '{"5":{"pipeline":"default"}}', ACTIVITY_TYPE_MAP_JSON: '{"call":"emails"}', ENTITIES: 'organizations,leads', MIGRATION_START_DATE: '2024-01-01', CONTACT_EMAIL_CONFLICT_POLICY: 'merge', STAGE_MAP_JSON_X: '' });
  check('Invalid owner / stage / activity maps, entity, start date and policy are all config errors', ['OWNER_MAP_JSON', 'STAGE_MAP_JSON.5', 'ACTIVITY_TYPE_MAP_JSON', 'ENTITIES', 'MIGRATION_START_DATE', 'CONTACT_EMAIL_CONFLICT_POLICY'].every((k) => bj.configErrors.some((e) => e.includes(k))), JSON.stringify(bj.configErrors));
  const { r: secretRun } = cfgOf(ARMED);
  check('Config output contains no token (only "is set" booleans)', Object.values(TOKENS).every((t) => !JSON.stringify(secretRun.runData['Config & Safety Gate']).includes(t)) && secretRun.last('Config & Safety Gate')[0].config.pipedrive.tokenSet === true);
  const ce = run({ ...ARMED, BATCH_SIZE: '500' }, makeMocks());
  check('Config error blocks writes even when armed (BATCH_SIZE > 100)', reportOf(ce).status !== 'MIGRATED' && ce.mocks.state.writes === 0);

  // ---------------------------------------------------- 3. fixture
  section('3. Fixture parity and synthetic data');
  const emb = byName['Load Synthetic Pipedrive Fixture'].parameters.jsCode.match(/const FIXTURE = ([\s\S]*?);\n\nconst loaded/);
  let parity = false;
  try { parity = JSON.stringify(JSON.parse(emb[1])) === JSON.stringify(fixture); } catch (e) { parity = false; }
  check('Fixture embedded in workflow.json equals sample-payloads/pipedrive-fixture.json', parity);
  check('Fixture flagged synthetic; names contain "Example"', fixture._meta.synthetic && fixture.pipedrive.organizations.filter((o) => o.name.trim()).every((o) => /Example/.test(o.name)));

  // ------------------------------------------------- 4. sandbox e2e
  section('4. Sandbox end-to-end: every validation scenario');
  const sb = runWorkflow({ env: { NOW_OVERRIDE: NOW }, mocks: makeMocks(), staticData: {} });
  check('Sandbox run completes', !sb.error, sb.error && sb.error.message);
  check('Sandbox run makes ZERO HTTP calls and leaves static data untouched', sb.mocks.state.calls.length === 0 && Object.keys(sb.staticData).length === 0);
  const expected = {
    'organizations:9100001': 'SKIP_UNCHANGED', 'organizations:9100002': 'CREATE', 'organizations:9100003': 'QUARANTINE', 'organizations:9100004': 'UPDATE',
    'persons:9200001': 'CREATE', 'persons:9200002': 'CREATE', 'persons:9200003': 'QUARANTINE', 'persons:9200004': 'QUARANTINE', 'persons:9200005': 'QUARANTINE', 'persons:9200006': 'CREATE', 'persons:9200007': 'CREATE', 'persons:9200008': 'UPDATE',
    'deals:9300001': 'CREATE', 'deals:9300002': 'CREATE', 'deals:9300003': 'QUARANTINE', 'deals:9300004': 'CREATE', 'deals:9300005': 'CREATE',
    'activities:9400001': 'CREATE', 'activities:9400002': 'CREATE', 'activities:9400003': 'CREATE', 'activities:9400004': 'CREATE', 'activities:9400005': 'CREATE', 'activities:9400006': 'CREATE', 'activities:9400007': 'CREATE',
  };
  const got = Object.fromEntries(planOf(sb).planned.map((r) => [`${r.entity}:${r.pdId}`, r.action]));
  check('All 24 extracted records get the expected action', JSON.stringify(got) === JSON.stringify(expected), JSON.stringify(Object.entries(got).filter(([k, v]) => expected[k] !== v)));
  check('Incremental: 9100005 (older than watermark) not extracted; deleted deal 9300006 excluded', !rec(sb, 'organizations', 9100005) && !rec(sb, 'deals', 9300006));
  const codes = (ent, id) => rec(sb, ent, id).issues.map((i) => i.code);
  check('Quarantine reasons: missing name, email conflict, duplicate email, no name/email, unmapped stage', codes('organizations', 9100003).includes('ORG_MISSING_NAME') && codes('persons', 9200003).includes('CONTACT_EMAIL_CONFLICT') && codes('persons', 9200004).includes('DUPLICATE_EMAIL_IN_SOURCE') && codes('persons', 9200005).includes('PERSON_MISSING_NAME_AND_EMAIL') && codes('deals', 9300003).includes('DEAL_UNMAPPED_STAGE'));
  const c2 = rec(sb, 'organizations', 9100002).properties;
  check('Company: name, domain normalised from the website custom field, unique pipedrive_org_id', c2.name === 'Acme Example Holdings' && c2.domain === 'acme-example.example.com' && c2.pipedrive_org_id === '9100002');
  const c1 = rec(sb, 'organizations', 9100001).properties;
  check('Company address fields mapped (address, city, country, zip) and owner mapped', c1.city === 'Springfield' && c1.zip === '00001' && c1.country === 'United States' && c1.hubspot_owner_id === '88000001');
  check('Unmapped owner is a warning, owner left blank (9100004)', codes('organizations', 9100004).includes('OWNER_UNMAPPED') && !rec(sb, 'organizations', 9100004).properties.hubspot_owner_id);
  const p1 = rec(sb, 'persons', 9200001);
  check('Contact: first / last name, lower-cased primary email, phone, FK to organization', p1.properties.firstname === 'Ada' && p1.properties.lastname === 'Example' && p1.properties.email === 'ada@example.com' && p1.properties.phone === '+1 555 010 0101' && p1.refs.organizations === '9100001');
  check('Invalid email is dropped with a warning, contact still created (9200006)', !rec(sb, 'persons', 9200006).properties.email && codes('persons', 9200006).includes('PERSON_INVALID_EMAIL'));
  const dWon = rec(sb, 'deals', 9300002).properties;
  check('Won deal -> STAGE_MAP_JSON.won (closedwon), close date from won_time, amount normalised', dWon.dealstage === 'closedwon' && dWon.pipeline === 'default' && dWon.closedate === '2026-11-11T16:00:00.000Z' && dWon.amount === '48000.5');
  check('Open deal -> mapped stage, close date from expected_close_date; invalid amount omitted with warning', rec(sb, 'deals', 9300001).properties.dealstage === 'appointmentscheduled' && rec(sb, 'deals', 9300001).properties.closedate === '2026-12-15T00:00:00.000Z' && !rec(sb, 'deals', 9300004).properties.amount && codes('deals', 9300004).includes('DEAL_INVALID_AMOUNT'));
  const a1 = rec(sb, 'activities', 9400001);
  check('Completed call -> calls: title, body, COMPLETED, 25 min = 1500000 ms, hs_timestamp', a1.objectType === 'calls' && a1.properties.hs_call_status === 'COMPLETED' && a1.properties.hs_call_duration === '1500000' && a1.properties.hs_timestamp === '2026-11-12T15:00:00.000Z');
  check('Planned call -> CALL task (HubSpot calls have no scheduled status)', rec(sb, 'activities', 9400002).objectType === 'tasks' && rec(sb, 'activities', 9400002).properties.hs_task_type === 'CALL' && rec(sb, 'activities', 9400002).properties.hs_task_status === 'NOT_STARTED');
  const a3 = rec(sb, 'activities', 9400003).properties;
  check('Meeting -> meetings with start / end (+60 min) and outcome', a3.hs_meeting_start_time === '2026-11-12T09:00:00.000Z' && a3.hs_meeting_end_time === '2026-11-12T10:00:00.000Z' && a3.hs_meeting_outcome === 'COMPLETED');
  check('Unknown type -> task (warning); no due date -> add_time (warning); no parent (warning)', codes('activities', 9400004).includes('ACTIVITY_TYPE_DEFAULTED') && rec(sb, 'activities', 9400005).properties.hs_timestamp === '2026-11-05T08:00:00.000Z' && codes('activities', 9400006).includes('ACTIVITY_NO_PARENT'));
  check('HTML stripped from activity notes', rec(sb, 'activities', 9400007).properties.hs_task_body === 'Legal review required');
  const rpt = sb.last('Dry-Run Validation Report')[0].report;
  check('Report totals: 18 writes (16 create, 2 update), 1 unchanged, 5 quarantined (20.8%)', JSON.stringify([rpt.totals.writes, rpt.totals.creates, rpt.totals.updates, rpt.totals.unchanged, rpt.totals.quarantined, rpt.totals.quarantinePct]) === '[18,16,2,1,5,"20.8"]');
  check('Report lists unresolved FKs: person -> missing org 9100099, deal -> quarantined person', JSON.stringify(rpt.fkUnresolved) === JSON.stringify([{ from: 'persons:9200007', to: 'organizations:9100099' }, { from: 'deals:9300005', to: 'persons:9200003' }]));
  check('Report: 25 planned associations incl. the pending one from the state seed', rpt.totals.associationsPlanned === 25 && planOf(sb).intents.some((i) => i.fromPending && i.fromPd === '9200101'));
  check('Report: quarantine list carries readable reasons; email conflicts listed', rpt.quarantine.length === 5 && rpt.quarantine.every((q) => q.reasons.length) && rpt.emailConflicts.some((e) => e.pipedriveId === '9200003'));
  check('Report: proposed watermarks = max update_time per entity', rpt.proposedWatermarks.activities === '2026-11-13T12:30:00Z' && rpt.proposedWatermarks.organizations === '2026-11-13T12:00:00Z');
  check('Run report status SANDBOX_DRY_RUN with blockers listed', reportOf(sb).status === 'SANDBOX_DRY_RUN' && reportOf(sb).writeBlockedBy.some((b) => /SANDBOX_MODE/.test(b)));
  const sb2 = runWorkflow({ env: { NOW_OVERRIDE: NOW }, mocks: makeMocks(), staticData: {} });
  check('Content hashes are deterministic across runs', JSON.stringify(planOf(sb).planned.map((r) => r.hash)) === JSON.stringify(planOf(sb2).planned.map((r) => r.hash)));
  check('Report contains no tokens and no phone numbers', !/sim-placeholder|555 010/.test(JSON.stringify(rpt)));
  fs.writeFileSync(path.join(OUT_DIR, 'sandbox-validation-report.json'), JSON.stringify(rpt, null, 2) + '\n');
  fs.writeFileSync(path.join(OUT_DIR, 'sandbox-run-report.json'), JSON.stringify(reportOf(sb), null, 2) + '\n');

  // --------------------------------------------- 5. live dry run
  section('5. Live dry run (mocked): read-only extract and preflight');
  const dm = makeMocks();
  const dsd = seedSd();
  const dr = run(LIVE, dm, dsd);
  check('Live dry run completes', !dr.error, dr.error && dr.error.message);
  const pdCalls = dm.state.calls.filter((c) => c.host === 'api.pipedrive.com');
  check('Pipedrive: 4 GET list calls with updated_since=<watermark>, sort_by=update_time asc, limit=500', pdCalls.length === 4 && pdCalls.every((c) => c.method === 'GET' && /updated_since=2026-11-01T00%3A00%3A00Z/.test(c.query) && /sort_by=update_time&sort_direction=asc&limit=500/.test(c.query)));
  check('Deals requested with status=open,won,lost; token only in the x-api-token header', /status=open%2Cwon%2Clost/.test(pdCalls.find((c) => c.path.endsWith('/deals')).query) && pdCalls.every((c) => !/token/i.test(c.query)));
  const hsCalls = dm.state.calls.filter((c) => c.host === 'api.hubapi.com');
  check('HubSpot: only batch/read preflight calls (read-only), max 100 ids each', hsCalls.length > 0 && hsCalls.every((c) => c.path.endsWith('/batch/read') && c.body.inputs.length <= 100));
  check('Preflight reads by unique property and by contact email', hsCalls.some((c) => c.body.idProperty === 'pipedrive_org_id') && hsCalls.some((c) => c.body.idProperty === 'email'));
  check('Live dry-run plan equals the sandbox plan', JSON.stringify(planOf(dr).planned.map((r) => [r.entity, r.pdId, r.action, r.hash])) === JSON.stringify(planOf(sb).planned.map((r) => [r.entity, r.pdId, r.action, r.hash])));
  check('Dry run: zero writes, state unchanged, status DRY_RUN', dm.state.writes === 0 && JSON.stringify(dsd.migration) === JSON.stringify(fixture.stateSeed) && reportOf(dr).status === 'DRY_RUN');
  check('Pipedrive / HubSpot tokens never appear in item data', Object.values(TOKENS).every((t) => !JSON.stringify(dr.runData).includes(t)));

  // --------------------------------------------- 6. armed migration
  section('6. Armed migration (mocked): writes, FK associations, idempotency');
  const m = makeMocks();
  const sd = seedSd();
  const r1 = run(ARMED, m, sd);
  check('Armed run completes with status MIGRATED', !r1.error && reportOf(r1).status === 'MIGRATED', r1.error ? r1.error.message : reportOf(r1).status);
  check('HubSpot now holds 3 companies, 7 contacts, 4 deals, 1 call, 1 meeting, 5 tasks', JSON.stringify(counts(m)) === JSON.stringify({ companies: 3, contacts: 7, deals: 4, calls: 1, meetings: 1, tasks: 5 }), JSON.stringify(counts(m)));
  check('18 records written in 6 batch upserts (one per object type), no duplicates', m.state.writes === 6 && noDupes(m) === null);
  check('Existing records were updated in place (9100004 -> 71000004, 9200008 -> 72000008)', hsFind(m, 'companies', 'pipedrive_org_id', '9100004')[0].id === '71000004' && hsFind(m, 'contacts', 'pipedrive_person_id', '9200008')[0].id === '72000008');
  check('Quarantined records were not written; unchanged 9100001 not rewritten', !hsFind(m, 'contacts', 'pipedrive_person_id', '9200003').length && !hsFind(m, 'deals', 'pipedrive_deal_id', '9300003').length && !hsFind(m, 'companies', 'name', '').length);
  const co = (pd) => hsIdOf(m, 'companies', 'pipedrive_org_id', pd), ct = (pd) => hsIdOf(m, 'contacts', 'pipedrive_person_id', pd), dl = (pd) => hsIdOf(m, 'deals', 'pipedrive_deal_id', pd);
  check('FK: contact 9200001 -> mapped company 71000001; contact 9200002 -> company created this run', linked(m, 'contacts', ct('9200001'), 'companies', '71000001') && linked(m, 'contacts', ct('9200002'), 'companies', co('9100002')));
  check('FK: deal 9300001 -> company + contact; won deal 9300002 -> company + contact', linked(m, 'deals', dl('9300001'), 'companies', '71000001') && linked(m, 'deals', dl('9300001'), 'contacts', ct('9200001')) && linked(m, 'deals', dl('9300002'), 'contacts', ct('9200002')));
  check('FK: call 9400001 -> deal, contact and company', linked(m, 'calls', hsIdOf(m, 'calls', 'pipedrive_activity_id', '9400001'), 'deals', dl('9300001')) && linked(m, 'calls', hsIdOf(m, 'calls', 'pipedrive_activity_id', '9400001'), 'contacts', ct('9200001')));
  check('Pending association from an earlier run resolved (contact 72000101 -> new company 9100002)', linked(m, 'contacts', '72000101', 'companies', co('9100002')));
  check('23 associations created; 2 unresolved kept as PENDING', reportOf(r1).writes.associationsCreated === 23 && sd.migration.pending.length === 2 && sd.migration.pending.every((p) => ['TARGET_NOT_IN_HUBSPOT', 'SOURCE_NOT_IN_HUBSPOT'].includes(p.reason)));
  check('Mapping tables persisted with HubSpot ids, hashes and pinned activity types', sd.migration.maps.persons['9200001'].id === ct('9200001') && sd.migration.maps.activities['9400002'].t === 'tasks' && Object.keys(sd.migration.maps.activities).length === 7);
  check('Watermarks advanced to max update_time per entity', sd.migration.watermarks.organizations === '2026-11-13T12:00:00Z' && sd.migration.watermarks.activities === '2026-11-13T12:30:00Z');
  check('Quarantine registry persisted with reasons', Object.keys(sd.migration.quarantine).length === 5 && sd.migration.quarantine['deals:9300003'].codes.includes('DEAL_UNMAPPED_STAGE'));
  const before = JSON.stringify(counts(m)), assocBefore = m.state.assoc.size, writesBefore = m.state.writes;
  const r2 = run(ARMED, m, sd);
  check('Rerun: boundary records re-fetched but skipped as unchanged -> zero writes, zero new objects', !r2.error && m.state.writes === writesBefore && JSON.stringify(counts(m)) === before && planOf(r2).planned.every((r) => r.action === 'SKIP_UNCHANGED'));
  check('Rerun: associations unchanged; still 2 pending', m.state.assoc.size === assocBefore && sd.migration.pending.length === 2);
  // crash recovery: HubSpot writes happened but the state commit was lost
  const cm = makeMocks();
  run(ARMED, cm, seedSd());
  const crash = run(ARMED, cm, seedSd());
  check('Crash recovery: state lost after writes -> rerun upserts in place, ZERO duplicates', !crash.error && JSON.stringify(counts(cm)) === before && noDupes(cm) === null);
  check('Crash recovery: preflight rebuilds the mapping (records classified UPDATE, not CREATE)', planOf(crash).planned.filter((r) => r.action === 'CREATE').length === 0 && planOf(crash).planned.filter((r) => r.action === 'UPDATE').length === 18);
  // incremental change
  const changed = clone(fixture.pipedrive);
  Object.assign(changed.persons.find((p) => p.id === 9200002), { first_name: 'Benjamin', update_time: '2026-11-16T09:00:00Z' });
  m.state.pd.persons = changed.persons;
  const r3 = run(ARMED, m, sd);
  const p3 = planOf(r3).planned.filter((r) => r.action !== 'SKIP_UNCHANGED');
  check('Incremental: only the changed person is written (UPDATE, same HubSpot id)', p3.length === 1 && p3[0].pdId === '9200002' && p3[0].action === 'UPDATE' && hsFind(m, 'contacts', 'pipedrive_person_id', '9200002')[0].properties.firstname === 'Benjamin' && JSON.stringify(counts(m)) === before);
  check('Incremental: persons watermark moved to the new update_time', sd.migration.watermarks.persons === '2026-11-16T09:00:00Z');
  // pinned activity type
  const ac = clone(changed); Object.assign(ac.activities.find((a) => a.id === 9400002), { done: true, update_time: '2026-11-16T10:00:00Z' });
  m.state.pd.activities = ac.activities;
  run(ARMED, m, sd);
  check('Planned call marked done stays the same HubSpot task (type pinned, no duplicate call)', hsFind(m, 'tasks', 'pipedrive_activity_id', '9400002')[0].properties.hs_task_status === 'COMPLETED' && !hsFind(m, 'calls', 'pipedrive_activity_id', '9400002').length);
  fs.writeFileSync(path.join(OUT_DIR, 'migration-state-after-run.json'), JSON.stringify(sd.migration, null, 2) + '\n');
  fs.writeFileSync(path.join(OUT_DIR, 'armed-run-report.json'), JSON.stringify(reportOf(r1), null, 2) + '\n');

  // --------------------------------------------- 7. scale, backlog, failures
  section('7. Pagination, backlog, write failures and fail-closed paths');
  const big = clone(fixture.pipedrive);
  for (let i = 0; i < 1203; i++) big.organizations.push({ id: 9500000 + i, name: `Bulk Example Org ${i}`, owner_id: 9900001, update_time: new Date(Date.UTC(2026, 10, 14) + i * 1000).toISOString().replace('.000', ''), address: null, custom_fields: {} });
  const bm = makeMocks({ pipedrive: big });
  const bsd = seedSd();
  const br = run({ ...ARMED, MAX_QUARANTINE_PCT: '50' }, bm, bsd);
  check('Pagination: 1207 organizations fetched over 3 pages of 500 and written in 13 batches of <= 100', !br.error && bm.state.calls.filter((c) => c.path === '/api/v2/organizations').length === 3 && hsFind(bm, 'companies', 'name', 'Bulk Example Org 1202').length === 1 && bm.state.calls.filter((c) => c.path === '/crm/v3/objects/companies/batch/upsert').length === 13, br.error && br.error.message);
  const tm = makeMocks({ pipedrive: big });
  const tsd = seedSd();
  const tr = run({ ...ARMED, PIPEDRIVE_MAX_PAGES_PER_ENTITY: '2', MAX_QUARANTINE_PCT: '50' }, tm, tsd);
  check('Backlog: page cap 2 -> first 1000 orgs by update_time processed (4 fixture + 996 bulk), flagged backlogRemaining', !tr.error && planOf(tr).report.backlogRemaining.organizations === true && planOf(tr).planned.filter((r) => r.entity === 'organizations').length === 1000 && tm.state.hs.companies.size === 2 + 1 + 996);
  run({ ...ARMED, PIPEDRIVE_MAX_PAGES_PER_ENTITY: '2', MAX_QUARANTINE_PCT: '50' }, tm, tsd);
  check('Backlog: next run continues from the watermark, all 1203 bulk orgs present once', [...tm.state.hs.companies.values()].filter((o) => /Bulk Example/.test(o.properties.name)).length === 1203 && noDupes(tm) === null);
  const fm = makeMocks({ failWriteType: 'deals' });
  const fsd = seedSd();
  const fr = run(ARMED, fm, fsd);
  check('Deal batch outage -> PARTIAL, 4 deal failures recorded, other entities written', reportOf(fr).status === 'PARTIAL' && reportOf(fr).writes.failures.filter((f) => f.entity === 'deals').length === 4 && fm.state.hs.contacts.size === 7);
  check('Deal watermark HELD (others advanced) so the next run retries the deals', fsd.migration.watermarks.deals === '2026-11-01T00:00:00Z' && fsd.migration.watermarks.persons === '2026-11-13T08:00:00Z');
  check('Associations to the failed deals stay pending', fsd.migration.pending.some((p) => p.fromEntity === 'activities' && p.toEntity === 'deals'));
  const fm2 = makeMocks();
  for (const t of Object.keys(fm.state.hs)) fm2.state.hs[t] = fm.state.hs[t];
  const fr2 = run(ARMED, fm2, fsd);
  check('Retry after outage: deals created, pending deal associations resolved, no duplicates', reportOf(fr2).status === 'MIGRATED' && fm2.state.hs.deals.size === 4 && noDupes(fm2) === null && linked(fm2, 'calls', hsIdOf(fm2, 'calls', 'pipedrive_activity_id', '9400001'), 'deals', hsIdOf(fm2, 'deals', 'pipedrive_deal_id', '9300001')));
  const am = makeMocks({ failAssoc: true });
  const asd = seedSd();
  const ar = run(ARMED, am, asd);
  check('Association outage -> PARTIAL, every pair queued as pending (retried next run)', reportOf(ar).status === 'PARTIAL' && asd.migration.pending.length === 25 && asd.migration.pending.some((p) => /ASSOCIATION_FAILED/.test(p.reason)));
  const e1 = run(ARMED, makeMocks({ failEntity: 'deals' }));
  check('Fail closed: Pipedrive 500 on deals stops the run before any HubSpot write', e1.error && e1.mocks.state.writes === 0 && !e1.last('Normalize & Validate'));
  const e2 = run(ARMED, makeMocks({ malformedEntity: 'persons' }));
  check('Fail closed: unsuccessful Pipedrive page stops the run', e2.error && /malformed or unsuccessful/.test(e2.error.message) && e2.mocks.state.writes === 0);
  const e3 = run(ARMED, makeMocks({ failLookup: true }));
  check('Fail closed: preflight lookup failure stops the run (no blind writes)', e3.error && e3.mocks.state.writes === 0);
  const e4 = run(ARMED, makeMocks(), { migration: { ...clone(fixture.stateSeed), maps: { ...fixture.stateSeed.maps, persons: { 'abc': { id: 'x', h: 1 } } } } });
  check('Fail closed: corrupt mapping table stops the run before extraction', e4.error && /failed validation/.test(e4.error.message) && e4.mocks.state.calls.length === 0);
  const rm = makeMocks();
  run(ARMED, rm, seedSd());
  const rsd = { migration: { version: 7 } };
  const rr = run({ ...ARMED, STATE_RESET: 'true' }, rm, rsd);
  check('STATE_RESET=true rebuilds state from MIGRATION_START_DATE; existing records UPDATE in place, no duplicates', !rr.error && rsd.migration.version === 1 && noDupes(rm) === null && rm.state.hs.companies.size === 4 && rec(rr, 'organizations', 9100002).action === 'UPDATE', rr.error && rr.error.message);

  // --------------------------------------------- 8. policies and guards
  section('8. Policies and guards');
  const g1 = run({ ...ARMED, MAX_WRITES_PER_RUN: '5' }, makeMocks());
  check('MAX_WRITES_PER_RUN=5 -> BLOCKED_BY_GUARD, zero writes', reportOf(g1).status === 'BLOCKED_BY_GUARD' && g1.mocks.state.writes === 0);
  const g2 = run({ ...ARMED, MAX_QUARANTINE_PCT: '10' }, makeMocks());
  check('MAX_QUARANTINE_PCT=10 (20.8% quarantined) -> BLOCKED_BY_GUARD, zero writes', reportOf(g2).status === 'BLOCKED_BY_GUARD' && g2.mocks.state.writes === 0);
  const sm = makeMocks();
  run({ ...ARMED, CONTACT_EMAIL_CONFLICT_POLICY: 'strip_email' }, sm);
  check('CONTACT_EMAIL_CONFLICT_POLICY=strip_email: conflicting person created without email', hsFind(sm, 'contacts', 'pipedrive_person_id', '9200003').length === 1 && !hsFind(sm, 'contacts', 'pipedrive_person_id', '9200003')[0].properties.email && noDupes(sm) === null);
  const ad = makeMocks();
  run({ ...ARMED, CONTACT_EMAIL_CONFLICT_POLICY: 'adopt' }, ad);
  check('CONTACT_EMAIL_CONFLICT_POLICY=adopt: existing contact 72000003 tagged with the Pipedrive id, no new contact', ad.state.hs.contacts.get('72000003').properties.pipedrive_person_id === '9200003' && hsFind(ad, 'contacts', 'pipedrive_person_id', '9200003').length === 1 && ad.state.hs.contacts.size === 7);
  const om = makeMocks();
  const osd = seedSd();
  run({ ...ARMED, ENTITIES: 'organizations' }, om, osd);
  check('ENTITIES=organizations: only companies written, other watermarks untouched', om.state.calls.filter((c) => /batch\/(upsert|update)/.test(c.path)).every((c) => c.path.includes('/companies/')) && osd.migration.watermarks.persons === '2026-11-01T00:00:00Z');
  const fu = makeMocks();
  const fusd = seedSd();
  const fur = run({ ...ARMED, FULL_RESYNC: 'true' }, fu, fusd);
  check('FULL_RESYNC=true: extracts from MIGRATION_START_DATE (old org included), ignores hashes', !!rec(fur, 'organizations', 9100005) && rec(fur, 'organizations', 9100001).action === 'UPDATE');
  const na = makeMocks();
  run({ ...ARMED, ASSOCIATIONS_ENABLED: 'false' }, na);
  check('ASSOCIATIONS_ENABLED=false: records written, zero association calls', na.state.writes > 0 && !na.state.calls.some((c) => c.path.includes('/associations/')));
  const nm = makeMocks();
  run({ ...ARMED, STAGE_MAP_JSON: '', DEFAULT_DEAL_PIPELINE: 'default', DEFAULT_DEAL_STAGE: 'appointmentscheduled' }, nm);
  check('DEFAULT_DEAL_STAGE used for unmapped stages when configured (warning, not quarantine)', hsFind(nm, 'deals', 'pipedrive_deal_id', '9300003').length === 1);
  const nothing = run(ARMED, makeMocks(), { migration: { ...clone(fixture.stateSeed), watermarks: { organizations: '2026-12-01T00:00:00Z', persons: '2026-12-01T00:00:00Z', deals: '2026-12-01T00:00:00Z', activities: '2026-12-01T00:00:00Z' }, pending: [] } });
  check('Nothing new since the watermark -> NOTHING_TO_MIGRATE, zero writes', reportOf(nothing).status === 'NOTHING_TO_MIGRATE' && nothing.mocks.state.writes === 0, reportOf(nothing).status);
  const sch = runWorkflow({ trigger: 'schedule', env: { NOW_OVERRIDE: NOW }, mocks: makeMocks(), staticData: {} });
  check('Hourly schedule trigger runs the same pipeline (sandbox)', !sch.error && reportOf(sch).status === 'SANDBOX_DRY_RUN');

  console.log(`\n${failed === 0 ? '✅' : '❌'} ${passed} passed, ${failed} failed`);
  const t = planOf(sb).report.totals;
  console.log(`Sandbox plan: ${t.creates} create, ${t.updates} update, ${t.unchanged} unchanged, ${t.quarantined} quarantined; ${t.associationsPlanned} associations planned, ${t.associationsUnresolved} unresolved`);
  console.log('Output written to: sim-output/ (git-ignored)');
  if (failed) { console.log('\nFailures:\n' + failures.map((f) => ` - ${f}`).join('\n')); process.exit(1); }
})();
