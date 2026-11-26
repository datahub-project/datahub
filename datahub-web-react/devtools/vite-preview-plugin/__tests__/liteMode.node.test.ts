import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { createServer } from 'http';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { createServer as createViteServer } from 'vite';
import { afterEach, describe, expect, it, vi } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- server tooling has no src alias
import { actorFromDataHubToken, liteModePlugin } from '../liteMode';

async function listen(server: Server): Promise<string> {
    await new Promise<void>((resolve) => {
        server.listen(0, '127.0.0.1', resolve);
    });
    return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

async function close(server: Server): Promise<void> {
    await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
    });
}

afterEach(() => vi.unstubAllEnvs());

describe('lite stack proxy', () => {
    it.each([true, false])('routes API requests with auth=%s without intercepting entity pages', async (auth) => {
        const directory = mkdtempSync(join(tmpdir(), 'vite-lite-'));
        const requests: { path?: string; authorization?: string; actor?: string | string[] }[] = [];
        const gms = createServer((req, res) => {
            requests.push({
                path: req.url,
                authorization: req.headers.authorization,
                actor: req.headers['x-datahub-actor'],
            });
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ data: { appConfig: { featureFlags: { showBrowseV2: false } } } }));
        });
        const gmsUrl = await listen(gms);
        const stackFile = join(directory, 'stack.json');
        writeFileSync(
            stackFile,
            JSON.stringify({
                bootId: 'test',
                auth,
                default: 'gms-0',
                instances: { 'gms-0': gmsUrl },
                systemClientId: 'system',
                systemClientSecret: 'secret',
            }),
        );
        vi.stubEnv('PREVIEW_MODE', 'lite');
        vi.stubEnv('GMS_LITE_STACK_FILE', stackFile);
        vi.stubEnv('FEATURE_FLAG_OVERRIDES', 'showBrowseV2=true');
        const vite = await createViteServer({
            configFile: false,
            appType: 'custom',
            plugins: [liteModePlugin()],
            optimizeDeps: { noDiscovery: true, include: [] },
            server: { middlewareMode: true, watch: null },
        });
        vite.middlewares.use((req, res) => res.end(`SPA ${req.url}`));
        const app = createServer(vite.middlewares);
        const origin = await listen(app);
        try {
            const page = await fetch(`${origin}/platform/urn:li:dataPlatform:hive`, {
                headers: { Accept: 'text/html' },
            });
            expect(await page.text()).toContain('SPA /platform/');
            const response = await fetch(`${origin}/api/v2/graphql?operationName=appConfig`, {
                method: 'POST',
                body: '{}',
                headers: {
                    Cookie: 'actor=urn:li:corpuser:alice; PLAY_SESSION=eyJ.session',
                    Authorization: 'Bearer caller',
                },
            });
            expect(await response.json()).toEqual({ data: { appConfig: { featureFlags: { showBrowseV2: true } } } });
            expect(requests?.[0]?.path).toBe('/api/graphql?operationName=appConfig');
            expect(requests?.[0]?.authorization).toBe(auth ? 'Bearer eyJ.session' : 'Bearer caller');
            expect(requests?.[0]?.actor).toBe(auth ? undefined : 'urn:li:corpuser:alice');
            await fetch(`${origin}/api/gms/config`, { headers: { Authorization: 'Bearer caller' } });
            expect(requests?.[1]?.path).toBe('/config');
            expect(requests?.[1]?.authorization).toBe('Bearer caller');
            await fetch(`${origin}/openapi/v3/entity/dataset`);
            expect(requests?.[2]?.path).toBe('/openapi/v3/entity/dataset');
            expect(requests?.[2]?.authorization).toBeUndefined();
            const health = await fetch(`${origin}/health`);
            expect(await health.json()).toEqual({ status: 'ok', mode: 'gms-lite-stack', bootId: 'test' });
        } finally {
            await close(app);
            await vite.close();
            await close(gms);
            rmSync(directory, { recursive: true });
        }
    });

    it('fails clearly when explicit stack metadata is absent', () => {
        vi.stubEnv('PREVIEW_MODE', 'lite');
        vi.stubEnv('GMS_LITE_STACK_FILE', '');
        expect(() => liteModePlugin()).toThrow(/GMS_LITE_STACK_FILE/);
    });
});

describe('no-auth token actor', () => {
    const token = (payload: object): string =>
        `eyJ.${Buffer.from(JSON.stringify(payload)).toString('base64')}.signature`;

    it('maps user tokens to corpuser URNs', () => {
        expect(actorFromDataHubToken(token({ actorType: 'USER', actorId: 'alice' }))).toBe('urn:li:corpuser:alice');
    });

    it('preserves service actor URNs', () => {
        expect(actorFromDataHubToken(token({ actorType: 'SERVICE', actorId: 'urn:li:corpuser:service' }))).toBe(
            'urn:li:corpuser:service',
        );
    });

    it('ignores malformed tokens and missing actors', () => {
        expect(actorFromDataHubToken('invalid')).toBeUndefined();
        expect(actorFromDataHubToken('a.invalid.c')).toBeUndefined();
        expect(actorFromDataHubToken(token({ sub: 'alice' }))).toBeUndefined();
    });
});
