#!/usr/bin/env node
'use strict';

/**
 * The agent's end of the agent bridge (agent-bridge.cjs): an MCP server over
 * stdio that forwards each message to the running app.
 *
 * The app copies this file beside `agent-bridge.json` in its user-data folder
 * when the bridge starts, so an agent is configured once with a path that
 * does not change, while the port and the token change on every launch. The
 * file is read on every message, so the agent can be started before the app.
 *
 * No dependencies, so it runs on any Node, including the app's own Electron
 * with ELECTRON_RUN_AS_NODE=1 (the command agent-bridge.json suggests).
 */

const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const readline = require('node:readline');

const bridgeFile = process.env.WPCT_AGENT_BRIDGE_FILE || path.join(__dirname, 'agent-bridge.json');

function readBridge() {
	try {
		const { url, token } = JSON.parse(fs.readFileSync(bridgeFile, 'utf8'));
		return url && token ? { url, token } : null;
	} catch {
		return null;
	}
}

function post({ url, token }, body) {
	return new Promise((resolve, reject) => {
		const req = http.request(url, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` } }, (res) => {
			const chunks = [];
			res.on('data', (c) => chunks.push(c));
			res.on('end', () => resolve({ status: res.statusCode, text: Buffer.concat(chunks).toString('utf8') }));
		});
		// No timeout of its own: wait_run holds a request open for minutes on purpose.
		req.on('error', reject);
		req.end(body);
	});
}

const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
const notRunning = 'WordPress Contributor Toolkit is not running, or "Let AI coding agents control this app" is off in its Settings. Ask the person to open the app and turn it on.';

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', async (line) => {
	if (!line.trim()) return;
	let message;
	try { message = JSON.parse(line); } catch { return write({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } }); }
	const isRequest = message && message.id !== undefined;
	const bridge = readBridge();
	if (!bridge) {
		if (isRequest) write({ jsonrpc: '2.0', id: message.id, error: { code: -32000, message: notRunning } });
		return;
	}
	try {
		const { status, text } = await post(bridge, line);
		if (status === 202 || !text) return;
		if (status === 401) {
			if (isRequest) write({ jsonrpc: '2.0', id: message.id, error: { code: -32000, message: 'The app refused the key in agent-bridge.json. Restart the app or turn the setting off and on.' } });
			return;
		}
		process.stdout.write(`${text}\n`);
	} catch {
		if (isRequest) write({ jsonrpc: '2.0', id: message.id, error: { code: -32000, message: notRunning } });
	}
});
