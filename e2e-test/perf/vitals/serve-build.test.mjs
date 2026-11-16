import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { after, before, describe, it } from 'node:test';
import { isSpaRoute, loopbackOrigin, shouldStubBackend, startServer } from './serve-build.mjs';

describe('isSpaRoute', () => {
    it('treats dotted entity URNs as client routes and real assets as files', () => {
        const urn = '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)/';
        assert.equal(isSpaRoute(urn), true);
        assert.equal(isSpaRoute('/search'), true);
        assert.equal(isSpaRoute('/assets/index-abc123.js'), false);
        assert.equal(isSpaRoute('/assets/missing.css'), false);
    });
});

describe('shouldStubBackend', () => {
    it('stubs missing, stub, none, and anonymous targets', () => {
        assert.equal(shouldStubBackend(undefined), true);
        assert.equal(shouldStubBackend('stub'), true);
        assert.equal(shouldStubBackend('NONE'), true);
        assert.equal(shouldStubBackend('anonymous'), true);
        assert.equal(shouldStubBackend('http://localhost:9002'), false);
    });
});

describe('startServer stub API', () => {
    let tmp;
    let server;

    before(async () => {
        tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'vitals-dist-'));
        fs.writeFileSync(path.join(tmp, 'index.html'), '<html><body>boot @basePath @sentryConfig</body></html>');
        server = await startServer({ distDir: tmp, port: 18181, targetBase: 'stub', label: 'test' });
    });

    after(async () => {
        if (server) await server.close();
        fs.rmSync(tmp, { recursive: true, force: true });
    });

    it('serves the SPA entry', async () => {
        const resp = await fetch('http://127.0.0.1:18181/');
        const body = await resp.text();
        assert.equal(resp.status, 200);
        assert.match(body, /boot/);
        assert.doesNotMatch(body, /@basePath/);
    });

    it('returns JSON for /config without hanging', async () => {
        const resp = await fetch('http://127.0.0.1:18181/config');
        const body = await resp.json();
        assert.equal(resp.status, 200);
        assert.equal(body.appVersion, 'boot-only');
    });

    it('redirects GET /authenticate to the SPA login route like Play does', async () => {
        const resp = await fetch(`${loopbackOrigin(server.port)}/authenticate?redirect_uri=%2F`, {
            redirect: 'manual',
        });
        assert.equal(resp.status, 302);
        assert.equal(resp.headers.get('location'), '/login?redirect_uri=%2F');
    });

    it('serves the SPA for a dataset URN whose name contains dots', async () => {
        const urn = '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)/';
        const resp = await fetch(`${loopbackOrigin(server.port)}${urn}`);
        const body = await resp.text();
        assert.equal(resp.status, 200);
        assert.match(body, /boot/);
        assert.match(resp.headers.get('content-type') || '', /text\/html/);
    });

    it('serves the SPA for the client-side /login route', async () => {
        const resp = await fetch(`${loopbackOrigin(server.port)}/login`);
        const body = await resp.text();
        assert.equal(resp.status, 200);
        assert.match(body, /boot/);
        assert.match(resp.headers.get('content-type') || '', /text\/html/);
    });

    it('still 404s POST /logIn in stub mode', async () => {
        const resp = await fetch(`${loopbackOrigin(server.port)}/logIn`, { method: 'POST', body: '{}' });
        const body = await resp.json();
        assert.equal(resp.status, 404);
        assert.equal(body.errors[0].message, 'boot-only: no backend');
    });

    it('returns a fast 404 JSON for GraphQL', async () => {
        const started = Date.now();
        const resp = await fetch('http://127.0.0.1:18181/api/graphql', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: '{}',
        });
        const body = await resp.json();
        assert.equal(resp.status, 404);
        assert.equal(body.errors[0].message, 'boot-only: no backend');
        assert.ok(Date.now() - started < 1000);
    });
});
