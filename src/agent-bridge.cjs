'use strict';

/**
 * The agent bridge (prototype): a local MCP server that lets an AI coding
 * agent (Claude Code, Codex, Orca, …) drive the same handlers the window
 * drives, without clicking.
 *
 * Off unless the contributor turns it on in Settings. When on, it listens on
 * 127.0.0.1 only, on a port the OS picks, and every request must carry the
 * random token written beside the store in `agent-bridge.json` (owner-only).
 * `scripts/agent-mcp-stdio.cjs` reads that file and is what an agent is
 * configured with, so neither the port nor the token is ever typed.
 *
 * It does not reimplement anything. `captureHandlers` remembers each
 * `ipcMain.handle` callback as main.js registers it, and a tool calls that
 * callback with a stand-in for the IPC event whose `sender` records what the
 * handler streams (install and script output, the server's log) so the agent
 * can read it back by id. The rules the window is held to — a registered
 * site, the legacy and merge-in-progress refusals, one store write at a time
 * — are the handlers' own, so they hold here unchanged.
 *
 * Deliberately left out: anything that throws work away or leaves the
 * machine (the discards, deleting a site, the trunk update, opening a pull
 * request, GitHub sign-in, settings). Those stay buttons a person presses.
 *
 * Pure apart from `http` and `fs`, so `node --test` covers it without
 * Electron: main.js hands it the registry, the store reads and the window.
 */

const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const PROTOCOL_VERSIONS = ['2025-06-18', '2025-03-26', '2024-11-05'];
const BRIDGE_FILE = 'agent-bridge.json';
const PROXY_FILE = 'agent-mcp-stdio.cjs';
const RUN_TEXT_LIMIT = 256 * 1024;
const MAX_BODY = 4 * 1024 * 1024;
const MAX_WAIT_SECONDS = 600;

// The id field each streaming channel correlates on. The server's log has no
// id of its own; it is keyed by site.
const RUN_CHANNELS = {
	'npm:install:log': { idKey: 'installId', kind: 'install' },
	'npm:install:done': { idKey: 'installId', kind: 'install', done: true },
	'npm:run-script:log': { idKey: 'runId', kind: 'script' },
	'npm:run-script:done': { idKey: 'runId', kind: 'script', done: true },
	'git:apply-patch:log': { idKey: 'applyId', kind: 'patch' },
	'git:apply-patch:done': { idKey: 'applyId', kind: 'patch', done: true }
};

// What is passed on to the window as well as recorded: a server the agent
// started shows as running there, and a branch switch shows its progress.
const FORWARDED = new Set(['playground:log', 'playground:url', 'playground:stopped', 'switch:progress', 'ticket:carried-work']);

/**
 * Wraps `ipcMain.handle` so every handler registered afterwards can also be
 * called from here. Must run before main.js registers its first handler.
 *
 * @param {Object} ipcMain Electron's ipcMain, or anything with `handle`.
 * @return {{has: Function, invoke: Function, channels: Function}} The registry.
 */
function captureHandlers(ipcMain) {
	const handlers = new Map();
	const original = ipcMain.handle.bind(ipcMain);
	ipcMain.handle = (channel, fn) => {
		handlers.set(channel, fn);
		return original(channel, fn);
	};
	return {
		has: (channel) => handlers.has(channel),
		channels: () => [...handlers.keys()],
		invoke(channel, sender, ...args) {
			const fn = handlers.get(channel);
			if (!fn) throw new Error(`No handler for ${channel}`);
			return fn({ sender }, ...args);
		}
	};
}

// What the handlers streamed, by run id (and by site for the server), with
// only the tail of a long output kept.
function createRunLog({ limit = RUN_TEXT_LIMIT, now = () => Date.now() } = {}) {
	const runs = new Map();
	const waiters = new Map();

	const ensure = (id, kind, sitePath) => {
		let run = runs.get(id);
		if (!run) {
			run = { id, kind, sitePath: sitePath || null, text: '', truncated: false, done: false, result: null, startedAt: now(), finishedAt: null };
			runs.set(id, run);
		}
		if (sitePath && !run.sitePath) run.sitePath = sitePath;
		return run;
	};
	const append = (run, data) => {
		run.text += data === null || data === undefined ? '' : String(data);
		if (run.text.length > limit) {
			run.text = run.text.slice(run.text.length - limit);
			run.truncated = true;
		}
	};
	const finish = (run, result) => {
		run.done = true;
		run.result = result;
		run.finishedAt = now();
		for (const resolve of waiters.get(run.id) || []) resolve(run);
		waiters.delete(run.id);
	};

	return {
		start(id, kind, sitePath) { return ensure(id, kind, sitePath); },
		record(channel, payload = {}) {
			const spec = RUN_CHANNELS[channel];
			if (spec) {
				const id = payload[spec.idKey];
				if (!id) return;
				const run = ensure(String(id), spec.kind);
				if (spec.done) {
					const { [spec.idKey]: _id, ...result } = payload;
					finish(run, result);
				} else {
					append(run, payload.data);
				}
				return;
			}
			if (channel.startsWith('playground:') && payload.sitePath) {
				const run = ensure(`server:${payload.sitePath}`, 'server', payload.sitePath);
				if (channel === 'playground:log') append(run, payload.data);
				if (channel === 'playground:url') append(run, `\n[agent-bridge] server ready at ${payload.url}\n`);
				if (channel === 'playground:stopped') append(run, `\n[agent-bridge] server stopped (code ${payload.code})\n`);
			}
		},
		get: (id) => runs.get(String(id)) || null,
		wait(id, ms) {
			const run = runs.get(String(id));
			if (!run) return Promise.resolve(null);
			if (run.done) return Promise.resolve(run);
			return new Promise((resolve) => {
				const timer = setTimeout(() => {
					const list = waiters.get(run.id) || [];
					waiters.set(run.id, list.filter((fn) => fn !== done));
					resolve(run);
				}, ms);
				const done = (r) => { clearTimeout(timer); resolve(r); };
				const list = waiters.get(run.id) || [];
				list.push(done);
				waiters.set(run.id, list);
			});
		}
	};
}

function runState(run) {
	if (!run.done) return 'running';
	return run.result && (run.result.code === 0 || run.result.ok === true) ? 'succeeded' : 'failed';
}

// A run as an agent reads it: the state, and the last `tail` characters.
function describeRun(run, tail = 8000) {
	if (!run) return null;
	const text = tail > 0 && run.text.length > tail ? run.text.slice(run.text.length - tail) : run.text;
	return {
		id: run.id,
		kind: run.kind,
		site: run.sitePath,
		state: runState(run),
		result: run.result,
		seconds: Math.round(((run.finishedAt || Date.now()) - run.startedAt) / 1000),
		output: text,
		outputTruncated: run.truncated || text.length < run.text.length
	};
}

const siteProp = { type: 'string', description: 'The site: its full path, its label, or its folder name, as list_sites shows it.' };

// The tools, each `{ name, description, inputSchema, write, run(args, ctx) }`.
// `write` marks a tool that changes the checkout or starts a process; the
// window is told to re-read the site after one.
const TOOLS = [
	{
		name: 'list_sites',
		description: 'List the WordPress sites this app manages, with their ticket, setup state and dev server URL. Call this first.',
		inputSchema: { type: 'object', properties: {} },
		async run(_args, ctx) {
			const { sites, siteMeta } = await ctx.registered();
			return sites.map((p) => {
				const m = siteMeta[p] || {};
				return { path: p, label: m.label || path.basename(p), projectType: m.projectType || null, ticket: m.tracTicket || null, branch: m.currentBranch || null, initialized: Boolean(m.initialized), serverUrl: ctx.serverUrl(p) };
			});
		}
	},
	{
		name: 'site_status',
		description: 'The full state of one site: dependencies installed, built, linked ticket, applied patch, pull request, whether a merge or legacy clone blocks writes, and the dev server URL.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		async run({ site }, ctx) {
			const p = await ctx.site(site);
			const status = await ctx.call('site:status', p);
			return { path: p, ...status, serverUrl: ctx.serverUrl(p) };
		}
	},
	{
		name: 'get_patch',
		description: 'The contributor\'s change as a unified diff, measured from the ticket\'s branch point, exactly as the app would export it for Trac.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		async run({ site }, ctx) { return ctx.call('git:get-patch', await ctx.site(site)); }
	},
	{
		name: 'list_branches',
		description: 'The ticket branches in a site and which one is checked out.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		async run({ site }, ctx) { return ctx.call('branches:list', await ctx.site(site)); }
	},
	{
		name: 'list_ticket_prs',
		description: 'The GitHub pull requests linked to the site\'s ticket.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		async run({ site }, ctx) { return ctx.call('git:list-ticket-patches', await ctx.site(site)); }
	},
	{
		name: 'link_ticket',
		description: 'Link a Trac ticket (or Gutenberg issue) to a site: creates or switches to its ticket branch, the same as typing it into the ticket field. Refuses with code "dirty-trunk" when trunk has uncommitted edits, unless carryTrunkWork is true. Does not apply any patch.',
		inputSchema: { type: 'object', properties: { site: siteProp, ticket: { type: 'string', description: 'A ticket number or URL.' }, carryTrunkWork: { type: 'boolean', description: 'Bring trunk\'s uncommitted edits into the new ticket branch.' } }, required: ['site', 'ticket'] },
		write: true,
		async run({ site, ticket, carryTrunkWork }, ctx) {
			if (!String(ticket || '').trim()) throw new Error('ticket is required; unlinking is not offered to agents');
			return ctx.call('sites:set-ticket', await ctx.site(site), String(ticket), { carryTrunkWork: Boolean(carryTrunkWork) });
		}
	},
	{
		name: 'apply_patch',
		description: 'Apply a patch to a site, all or nothing, the same as the Apply button. Give the diff text, or the https URL of a core.trac.wordpress.org attachment. Only one patch can be applied at a time; it is recorded so revert_patch can undo it.',
		inputSchema: { type: 'object', properties: { site: siteProp, patchText: { type: 'string' }, tracAttachmentUrl: { type: 'string' }, label: { type: 'string', description: 'A name for the patch, such as its file name.' } }, required: ['site'] },
		write: true,
		async run({ site, patchText, tracAttachmentUrl, label }, ctx) {
			const p = await ctx.site(site);
			let text = patchText;
			if (!text && tracAttachmentUrl) {
				const fetched = await ctx.call('trac:fetch-attachment', String(tracAttachmentUrl));
				if (!fetched || !fetched.ok) return fetched;
				text = fetched.text;
			}
			if (!text) throw new Error('Give patchText or tracAttachmentUrl');
			const name = label || (tracAttachmentUrl ? path.basename(String(tracAttachmentUrl).split('?')[0]) : 'patch from agent');
			const { applyId } = await ctx.call('git:apply-patch', p, { patchText: String(text), label: name });
			ctx.runs.start(applyId, 'patch', p);
			return describeRun(await ctx.runs.wait(applyId, 120000));
		}
	},
	{
		name: 'revert_patch',
		description: 'Revert the patch the app applied to a site.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		write: true,
		async run({ site }, ctx) {
			const p = await ctx.site(site);
			const { applyId } = await ctx.call('git:apply-patch', p, { reverse: true });
			ctx.runs.start(applyId, 'patch', p);
			return describeRun(await ctx.runs.wait(applyId, 120000));
		}
	},
	{
		name: 'npm_install',
		description: 'Install the site\'s npm dependencies on the app\'s bundled Node. Returns a run id at once; follow it with wait_run or get_run. Takes minutes.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		write: true,
		async run({ site }, ctx) {
			const p = await ctx.site(site);
			const { installId } = await ctx.call('npm:install', p);
			ctx.runs.start(installId, 'install', p);
			return { runId: installId, next: 'wait_run' };
		}
	},
	{
		name: 'run_script',
		description: 'Run one of the site\'s package.json scripts (build, build:dev, grunt tasks, test:php is not available: it needs Docker) on the app\'s bundled Node. Returns a run id at once. A watch script never finishes; stop it with stop_run.',
		inputSchema: { type: 'object', properties: { site: siteProp, script: { type: 'string' }, args: { type: 'array', items: { type: 'string' } } }, required: ['site', 'script'] },
		write: true,
		async run({ site, script, args }, ctx) {
			const p = await ctx.site(site);
			const scripts = ctx.packageScripts(p);
			if (!Object.hasOwn(scripts, script)) throw new Error(`No script "${script}" in package.json. Known: ${Object.keys(scripts).join(', ')}`);
			const { runId } = await ctx.call('npm:run-script', p, script, Array.isArray(args) ? args.map(String) : []);
			ctx.runs.start(runId, 'script', p);
			return { runId, next: 'wait_run' };
		}
	},
	{
		name: 'stop_run',
		description: 'Stop the install or script running in a site.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		write: true,
		async run({ site }, ctx) { return ctx.call('npm:kill', { directoryPath: await ctx.site(site) }); }
	},
	{
		name: 'get_run',
		description: 'The state and the end of the output of an install, script or patch run, by the id a tool returned.',
		inputSchema: { type: 'object', properties: { runId: { type: 'string' }, tail: { type: 'number', description: 'Characters of output to return from the end (default 8000).' } }, required: ['runId'] },
		async run({ runId, tail }, ctx) {
			const run = ctx.runs.get(runId);
			if (!run) throw new Error(`No run ${runId} since the bridge started`);
			return describeRun(run, tail ?? 8000);
		}
	},
	{
		name: 'wait_run',
		description: 'Wait until a run finishes or the timeout passes, then return its state and output tail.',
		inputSchema: { type: 'object', properties: { runId: { type: 'string' }, timeoutSeconds: { type: 'number', description: `Up to ${MAX_WAIT_SECONDS} (default 300).` }, tail: { type: 'number' } }, required: ['runId'] },
		async run({ runId, timeoutSeconds, tail }, ctx) {
			const seconds = Math.min(Math.max(Number(timeoutSeconds) || 300, 1), MAX_WAIT_SECONDS);
			const run = await ctx.runs.wait(runId, seconds * 1000);
			if (!run) throw new Error(`No run ${runId} since the bridge started`);
			return describeRun(run, tail ?? 8000);
		}
	},
	{
		name: 'start_server',
		description: 'Start the site\'s development server (WordPress Playground, no Docker) and return its URL once it answers. Log in at <url>/wp-admin with admin / password.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		write: true,
		async run({ site }, ctx) {
			const p = await ctx.site(site);
			ctx.runs.start(`server:${p}`, 'server', p);
			return ctx.call('playground:start', p);
		}
	},
	{
		name: 'stop_server',
		description: 'Stop the site\'s development server.',
		inputSchema: { type: 'object', properties: { site: siteProp }, required: ['site'] },
		write: true,
		async run({ site }, ctx) { return ctx.call('playground:stop', await ctx.site(site)); }
	},
	{
		name: 'server_logs',
		description: 'The end of the development server\'s output (PHP errors land here) since the bridge started.',
		inputSchema: { type: 'object', properties: { site: siteProp, tail: { type: 'number' } }, required: ['site'] },
		async run({ site, tail }, ctx) {
			const p = await ctx.site(site);
			return { serverUrl: ctx.serverUrl(p), ...(describeRun(ctx.runs.get(`server:${p}`), tail ?? 8000) || { output: '' }) };
		}
	},
	{
		name: 'node_env',
		description: 'The PATH entry holding the app\'s bundled node, npm and npx, for running a command in a terminal without a Node install. Prepend it to PATH.',
		inputSchema: { type: 'object', properties: {} },
		async run(_args, ctx) { return ctx.nodeEnv(); }
	}
];

const INSTRUCTIONS = [
	'This server is the WordPress Contributor Toolkit desktop app, which owns the wordpress-develop checkouts it manages.',
	'Start with list_sites. Let the app own Git: link tickets, apply and revert patches through these tools, not with git checkout, commit, stash or rebase in the checkout, or the app loses track of the ticket branch.',
	'Editing files in the checkout directly is fine; get_patch shows the result. Long runs (npm_install, run_script) return a runId: follow it with wait_run.',
	'Discarding work, deleting sites, updating trunk and opening pull requests are deliberately not offered; ask the person to use the app for those.'
].join(' ');

// The tool context: how a tool reads the store, resolves a site and calls a
// handler. Split out so the unit suite can build one from fakes.
function createContext({ registry, readStore, serverUrl, nodeEnv, readFile = fs.readFileSync, onForward = () => {}, runs }) {
	const sender = {
		send(channel, payload) {
			runs.record(channel, payload);
			if (FORWARDED.has(channel)) onForward(channel, payload);
		},
		isDestroyed: () => false
	};
	const registered = async () => {
		const { sites = [], siteMeta = {} } = (await readStore()) || {};
		return { sites: Array.isArray(sites) ? sites : [], siteMeta: siteMeta || {} };
	};
	return {
		runs,
		registered,
		serverUrl: (p) => serverUrl(p) || null,
		nodeEnv: () => nodeEnv(),
		call: (channel, ...args) => registry.invoke(channel, sender, ...args),
		// Only a site the app manages: npm:install, for one, takes any folder,
		// and the bridge must not become a way to run npm somewhere else.
		async site(ref) {
			const wanted = String(ref || '').trim();
			if (!wanted) throw new Error('site is required');
			const { sites, siteMeta } = await registered();
			const exact = sites.find((p) => p === wanted || path.resolve(wanted) === p);
			if (exact) return exact;
			const named = sites.filter((p) => (siteMeta[p]?.label === wanted) || path.basename(p) === wanted);
			if (named.length === 1) return named[0];
			if (named.length > 1) throw new Error(`"${wanted}" names more than one site; give the full path`);
			throw new Error(`"${wanted}" is not a site this app manages. Call list_sites.`);
		},
		packageScripts(p) {
			try {
				const pkg = JSON.parse(readFile(path.join(p, 'package.json'), 'utf8'));
				return pkg && typeof pkg.scripts === 'object' && pkg.scripts ? pkg.scripts : {};
			} catch {
				return {};
			}
		}
	};
}

function toolResult(value, isError = false) {
	const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
	return { content: [{ type: 'text', text: text === undefined ? 'null' : text }], isError };
}

// One JSON-RPC message in, its response out (or null for a notification).
async function handleMessage(message, { ctx, tools = TOOLS, onWrite = () => {}, version = '0' }) {
	const reply = (result) => ({ jsonrpc: '2.0', id: message.id, result });
	const fail = (code, msg) => ({ jsonrpc: '2.0', id: message.id ?? null, error: { code, message: msg } });
	if (!message || message.jsonrpc !== '2.0' || typeof message.method !== 'string') return fail(-32600, 'Invalid Request');
	const isNotification = message.id === undefined;
	if (isNotification) return null;

	switch (message.method) {
		case 'initialize': {
			const asked = message.params && message.params.protocolVersion;
			return reply({
				protocolVersion: PROTOCOL_VERSIONS.includes(asked) ? asked : PROTOCOL_VERSIONS[0],
				capabilities: { tools: { listChanged: false } },
				serverInfo: { name: 'wordpress-contributor-toolkit', version },
				instructions: INSTRUCTIONS
			});
		}
		case 'ping':
			return reply({});
		case 'tools/list':
			return reply({ tools: tools.map(({ name, description, inputSchema, write }) => ({ name, description, inputSchema, annotations: { readOnlyHint: !write, destructiveHint: false } })) });
		case 'tools/call': {
			const { name, arguments: args = {} } = message.params || {};
			const tool = tools.find((t) => t.name === name);
			if (!tool) return fail(-32602, `Unknown tool: ${name}`);
			try {
				const value = await tool.run(args || {}, ctx);
				if (tool.write) onWrite(name, value);
				// A handler's own refusal, or a run that ended in failure.
				const refused = Boolean(value && typeof value === 'object' && (value.ok === false || value.state === 'failed'));
				return reply(toolResult(value, refused));
			} catch (e) {
				return reply(toolResult(String(e && e.message ? e.message : e), true));
			}
		}
		default:
			return fail(-32601, `Method not found: ${message.method}`);
	}
}

// Whether a request may talk to the bridge: the token, a loopback Host (so a
// web page cannot reach it by DNS rebinding) and no browser Origin.
function authorize(headers, token) {
	const host = String(headers.host || '').replace(/:\d+$/, '');
	if (!['127.0.0.1', 'localhost', '[::1]'].includes(host)) return false;
	if (headers.origin) return false;
	const given = String(headers.authorization || '').replace(/^Bearer\s+/i, '');
	const a = Buffer.from(given);
	const b = Buffer.from(token);
	return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// Starts the HTTP side (MCP Streamable HTTP, JSON responses only) and writes
// the bridge file. Resolves to `{ url, token, stop() }`.
function startBridge({ ctx, dir, tools = TOOLS, onWrite, version, log = () => {}, port = 0, nodeCommand = null }) {
	const token = crypto.randomBytes(32).toString('hex');
	const file = path.join(dir, BRIDGE_FILE);
	const server = http.createServer((req, res) => {
		const end = (status, body) => {
			res.writeHead(status, body ? { 'content-type': 'application/json' } : {});
			res.end(body ? JSON.stringify(body) : undefined);
		};
		if (!authorize(req.headers, token)) return end(401, { error: 'unauthorized' });
		if (req.url !== '/mcp') return end(404, { error: 'not found' });
		if (req.method !== 'POST') return end(405, { error: 'POST only' });
		let size = 0;
		const chunks = [];
		req.on('data', (c) => {
			size += c.length;
			if (size > MAX_BODY) { req.destroy(); return; }
			chunks.push(c);
		});
		req.on('end', async () => {
			let parsed;
			try { parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { return end(400, { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } }); }
			const batch = Array.isArray(parsed) ? parsed : [parsed];
			for (const m of batch) if (m && m.method === 'tools/call') log(`tool ${m.params && m.params.name}`);
			const replies = (await Promise.all(batch.map((m) => handleMessage(m, { ctx, tools, onWrite, version })))).filter(Boolean);
			if (replies.length === 0) return end(202);
			end(200, Array.isArray(parsed) ? replies : replies[0]);
		});
	});
	return new Promise((resolve, reject) => {
		server.once('error', reject);
		server.listen(port, '127.0.0.1', () => {
			const url = `http://127.0.0.1:${server.address().port}/mcp`;
			// The agent's stdio end, copied out of the bundle where any Node can
			// read it, under a path that stays the same between launches.
			const proxy = path.join(dir, PROXY_FILE);
			try { fs.copyFileSync(path.join(__dirname, PROXY_FILE), proxy); } catch (e) { log(`could not copy ${PROXY_FILE}: ${e.message}`); }
			const stdio = nodeCommand ? { command: nodeCommand.command, args: [proxy], env: nodeCommand.env } : { command: 'node', args: [proxy] };
			fs.writeFileSync(file, JSON.stringify({ url, token, pid: process.pid, stdio }, null, 2), { mode: 0o600 });
			// writeFileSync's mode applies only when it creates the file.
			try { fs.chmodSync(file, 0o600); } catch {}
			resolve({
				url,
				token,
				file,
				stop: () => new Promise((done) => {
					try { fs.unlinkSync(file); } catch {}
					server.close(() => done());
					server.closeAllConnections?.();
				})
			});
		});
	});
}

module.exports = { captureHandlers, createRunLog, createContext, describeRun, handleMessage, authorize, startBridge, TOOLS, BRIDGE_FILE, PROXY_FILE, INSTRUCTIONS };
