'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { spawn } = require('node:child_process');

const {
	captureHandlers, createRunLog, createContext, describeRun, handleMessage, authorize, startBridge, TOOLS, BRIDGE_FILE, PROXY_FILE
} = require('../../src/agent-bridge.cjs');

const SITE = path.resolve('/sites/wp-one');
const OTHER = path.resolve('/elsewhere/wp-one');

// A fake ipcMain whose handlers stream through the sender like main.js's do.
function fakeApp({ sites = [SITE], siteMeta = { [SITE]: { label: 'Core', tracTicket: '62281' } }, handlers = {} } = {}) {
	const ipcMain = { registered: new Map(), handle(channel, fn) { this.registered.set(channel, fn); } };
	const registry = captureHandlers(ipcMain);
	for (const [channel, fn] of Object.entries(handlers)) ipcMain.handle(channel, fn);
	const runs = createRunLog();
	const forwarded = [];
	const ctx = createContext({
		registry,
		runs,
		readStore: async () => ({ sites, siteMeta }),
		serverUrl: (p) => (p === SITE ? 'http://127.0.0.1:9400/' : null),
		nodeEnv: () => ({ pathEntry: '/tmp/shims' }),
		readFile: () => JSON.stringify({ scripts: { build: 'grunt build', 'build:dev': 'grunt build --dev' } }),
		onForward: (channel, payload) => forwarded.push([channel, payload])
	});
	return { ipcMain, registry, runs, ctx, forwarded };
}

const call = (ctx, name, args = {}, extra = {}) => handleMessage({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }, { ctx, ...extra });
const parse = (reply) => JSON.parse(reply.result.content[0].text);

test('captureHandlers keeps registering with ipcMain and can call each handler with a stand-in sender', async () => {
	const { ipcMain, registry } = fakeApp({ handlers: { 'x:y': (event, a) => ({ a, hasSender: typeof event.sender.send }) } });
	assert.ok(ipcMain.registered.has('x:y'));
	assert.deepEqual(await registry.invoke('x:y', { send() {} }, 5), { a: 5, hasSender: 'function' });
	assert.throws(() => registry.invoke('nope', {}), /No handler/);
});

test('a site is resolved by path, label or folder name, and only among the sites the app manages', async () => {
	const { ctx } = fakeApp();
	assert.equal(await ctx.site(SITE), SITE);
	assert.equal(await ctx.site('Core'), SITE);
	assert.equal(await ctx.site('wp-one'), SITE);
	await assert.rejects(ctx.site('/etc'), /not a site this app manages/);
	await assert.rejects(ctx.site(''), /required/);
	const two = fakeApp({ sites: [SITE, OTHER], siteMeta: {} });
	await assert.rejects(two.ctx.site('wp-one'), /more than one site/);
});

test('npm_install refuses a folder the app does not manage, so the bridge cannot run npm elsewhere', async () => {
	let called = false;
	const { ctx } = fakeApp({ handlers: { 'npm:install': () => { called = true; return { installId: 'i1' }; } } });
	const reply = await call(ctx, 'npm_install', { site: os.homedir() });
	assert.equal(reply.result.isError, true);
	assert.equal(called, false);
});

test('a long run is followed by id: output is recorded from the sender and wait_run returns when it is done', async () => {
	const { ctx } = fakeApp({
		handlers: {
			'npm:run-script': (event, dir, script) => {
				setTimeout(() => event.sender.send('npm:run-script:log', { runId: 'r1', type: 'stdout', data: `running ${script} in ${dir}\n` }), 5);
				setTimeout(() => event.sender.send('npm:run-script:done', { runId: 'r1', code: 0 }), 10);
				return { runId: 'r1' };
			}
		}
	});
	const started = parse(await call(ctx, 'run_script', { site: 'Core', script: 'build' }));
	assert.equal(started.runId, 'r1');
	const done = parse(await call(ctx, 'wait_run', { runId: 'r1', timeoutSeconds: 5 }));
	assert.equal(done.state, 'succeeded');
	assert.match(done.output, /running build in/);
});

test('run_script names the scripts there are when asked for one that is not in package.json', async () => {
	const { ctx } = fakeApp({ handlers: { 'npm:run-script': () => assert.fail('must not start') } });
	const reply = await call(ctx, 'run_script', { site: 'Core', script: 'env:start; rm -rf /' });
	assert.equal(reply.result.isError, true);
	assert.match(reply.result.content[0].text, /Known: build, build:dev/);
});

test('apply_patch waits for the handler\'s done event and reports a refusal as an error', async () => {
	const { ctx } = fakeApp({
		handlers: {
			'git:apply-patch': (event, dir, options) => {
				setTimeout(() => event.sender.send('git:apply-patch:done', { applyId: 'a1', ok: false, error: `cannot apply ${options.label}` }), 5);
				return { applyId: 'a1' };
			}
		}
	});
	const reply = await call(ctx, 'apply_patch', { site: 'Core', patchText: 'diff --git a/x b/x', label: '62281.diff' });
	const run = parse(reply);
	assert.equal(reply.result.isError, true);
	assert.equal(run.state, 'failed');
	assert.match(run.result.error, /cannot apply 62281.diff/);
});

test('a refusal from a handler ({ ok: false }) comes back as a tool error with its code intact', async () => {
	const { ctx } = fakeApp({ handlers: { 'sites:set-ticket': () => ({ ok: false, code: 'dirty-trunk', canCarry: true }) } });
	const reply = await call(ctx, 'link_ticket', { site: 'Core', ticket: '62281' });
	assert.equal(reply.result.isError, true);
	assert.equal(parse(reply).code, 'dirty-trunk');
});

test('a write tool tells the window; a read tool does not', async () => {
	const writes = [];
	const { ctx } = fakeApp({ handlers: { 'playground:start': () => ({ ok: true, url: 'http://127.0.0.1:9400/' }), 'site:status': () => ({ hasBuilt: true }) } });
	await call(ctx, 'site_status', { site: 'Core' }, { onWrite: (t) => writes.push(t) });
	await call(ctx, 'start_server', { site: 'Core' }, { onWrite: (t) => writes.push(t) });
	assert.deepEqual(writes, ['start_server']);
});

test('the server\'s events are recorded by site and forwarded to the window', async () => {
	const { ctx, forwarded } = fakeApp({
		handlers: {
			'playground:start': (event, sitePath) => {
				event.sender.send('playground:log', { sitePath, type: 'stderr', data: 'PHP Warning: x\n' });
				event.sender.send('playground:url', { sitePath, url: 'http://127.0.0.1:9400/' });
				return { ok: true, url: 'http://127.0.0.1:9400/' };
			}
		}
	});
	await call(ctx, 'start_server', { site: 'Core' });
	const logs = parse(await call(ctx, 'server_logs', { site: 'Core' }));
	assert.match(logs.output, /PHP Warning/);
	assert.deepEqual(forwarded.map(([c]) => c), ['playground:log', 'playground:url']);
});

test('no tool throws work away or leaves the machine', () => {
	const names = TOOLS.map((t) => t.name);
	for (const forbidden of ['discard', 'delete', 'update_trunk', 'open_pr', 'sign_in', 'settings']) {
		assert.ok(!names.some((n) => n.includes(forbidden)), `${forbidden} must not be a tool`);
	}
});

test('the run log keeps only the tail of a long output and says so', () => {
	const runs = createRunLog({ limit: 10 });
	runs.start('r', 'script');
	runs.record('npm:run-script:log', { runId: 'r', data: '0123456789abcdef' });
	const run = describeRun(runs.get('r'));
	assert.equal(run.output, '6789abcdef');
	assert.equal(run.outputTruncated, true);
});

test('initialize answers the protocol version asked for when it knows it, and notifications get no reply', async () => {
	const { ctx } = fakeApp();
	const init = await handleMessage({ jsonrpc: '2.0', id: 0, method: 'initialize', params: { protocolVersion: '2025-03-26' } }, { ctx });
	assert.equal(init.result.protocolVersion, '2025-03-26');
	assert.ok(init.result.capabilities.tools);
	assert.equal(await handleMessage({ jsonrpc: '2.0', method: 'notifications/initialized' }, { ctx }), null);
	const list = await handleMessage({ jsonrpc: '2.0', id: 2, method: 'tools/list' }, { ctx });
	assert.ok(list.result.tools.find((t) => t.name === 'list_sites').annotations.readOnlyHint);
	assert.equal(list.result.tools.find((t) => t.name === 'npm_install').annotations.readOnlyHint, false);
	const unknown = await handleMessage({ jsonrpc: '2.0', id: 3, method: 'resources/list' }, { ctx });
	assert.equal(unknown.error.code, -32601);
});

test('authorize wants the token, a loopback Host and no browser Origin', () => {
	const ok = { host: '127.0.0.1:5000', authorization: 'Bearer secret' };
	assert.equal(authorize(ok, 'secret'), true);
	assert.equal(authorize({ ...ok, authorization: 'Bearer nope!' }, 'secret'), false);
	assert.equal(authorize({ ...ok, authorization: undefined }, 'secret'), false);
	assert.equal(authorize({ ...ok, host: 'evil.example:5000' }, 'secret'), false);
	assert.equal(authorize({ ...ok, origin: 'https://evil.example' }, 'secret'), false);
});

function postJson(url, token, body) {
	return new Promise((resolve, reject) => {
		const req = http.request(url, { method: 'POST', headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) } }, (res) => {
			const chunks = [];
			res.on('data', (c) => chunks.push(c));
			res.on('end', () => resolve({ status: res.statusCode, text: Buffer.concat(chunks).toString('utf8') }));
		});
		req.on('error', reject);
		req.end(JSON.stringify(body));
	});
}

test('startBridge listens on loopback behind the token, writes an owner-only bridge file and removes it on stop', async (t) => {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-'));
	t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
	const { ctx } = fakeApp();
	const bridge = await startBridge({ ctx, dir, version: 'test' });
	try {
		assert.match(bridge.url, /^http:\/\/127\.0\.0\.1:\d+\/mcp$/);
		const file = path.join(dir, BRIDGE_FILE);
		const written = JSON.parse(fs.readFileSync(file, 'utf8'));
		assert.equal(written.url, bridge.url);
		assert.equal(written.token, bridge.token);
		assert.ok(fs.existsSync(path.join(dir, PROXY_FILE)));
		if (process.platform !== 'win32') assert.equal((fs.statSync(file).mode % 0o1000).toString(8), '600');

		assert.equal((await postJson(bridge.url, null, { jsonrpc: '2.0', id: 1, method: 'ping' })).status, 401);
		const listed = await postJson(bridge.url, bridge.token, { jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'list_sites', arguments: {} } });
		assert.equal(listed.status, 200);
		const sites = JSON.parse(JSON.parse(listed.text).result.content[0].text);
		assert.deepEqual(sites.map((s) => [s.label, s.ticket, s.serverUrl]), [['Core', '62281', 'http://127.0.0.1:9400/']]);
		assert.equal((await postJson(bridge.url, bridge.token, { jsonrpc: '2.0', method: 'notifications/initialized' })).status, 202);
	} finally {
		await bridge.stop();
	}
	assert.equal(fs.existsSync(path.join(dir, BRIDGE_FILE)), false);
});

test('the stdio proxy forwards to the running bridge, and says the app is not running when it is not', async (t) => {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-'));
	t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
	const { ctx } = fakeApp();
	const exchange = (lines) => new Promise((resolve) => {
		const child = spawn(process.execPath, [path.join(__dirname, '../../src/agent-mcp-stdio.cjs')], { env: { ...process.env, WPCT_AGENT_BRIDGE_FILE: path.join(dir, BRIDGE_FILE) } });
		let out = '';
		child.stdout.on('data', (d) => {
			out += d;
			if (out.split('\n').filter(Boolean).length >= lines.filter((l) => l.id !== undefined).length) child.stdin.end();
		});
		child.on('close', () => resolve(out.split('\n').filter(Boolean).map((l) => JSON.parse(l))));
		for (const l of lines) child.stdin.write(`${JSON.stringify(l)}\n`);
	});

	const offline = await exchange([{ jsonrpc: '2.0', id: 1, method: 'initialize', params: {} }]);
	assert.match(offline[0].error.message, /not running/);

	const bridge = await startBridge({ ctx, dir, version: 'test' });
	try {
		const online = await exchange([
			{ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-06-18' } },
			{ jsonrpc: '2.0', method: 'notifications/initialized' },
			{ jsonrpc: '2.0', id: 2, method: 'tools/list' }
		]);
		assert.equal(online.length, 2);
		assert.equal(online[0].result.serverInfo.name, 'wordpress-contributor-toolkit');
		assert.ok(online[1].result.tools.length > 10);
	} finally {
		await bridge.stop();
	}
});
