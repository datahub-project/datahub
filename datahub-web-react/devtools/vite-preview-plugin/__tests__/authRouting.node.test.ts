import { createServer } from 'http';
import type { AddressInfo } from 'net';
import { createServer as createViteServer } from 'vite';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- devtools/ is outside src/ and has no path alias
import { registerAuthStubs } from '../authStubs';

describe('login routing', () => {
    let vite: Awaited<ReturnType<typeof createViteServer>>;
    const server = createServer();
    let origin: string;

    beforeAll(async () => {
        vite = await createViteServer({
            configFile: false,
            cacheDir: 'node_modules/.vite-auth-routing-test',
            appType: 'custom',
            optimizeDeps: { noDiscovery: true, include: [] },
            server: { middlewareMode: true, watch: null },
        });
        const app = vite.middlewares;
        registerAuthStubs(app);
        app.use((req, res) => res.end(req.url));
        server.on('request', app);
        await new Promise<void>((resolve) => {
            server.listen(0, '127.0.0.1', resolve);
        });
        origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    });

    afterAll(async () => {
        await new Promise<void>((resolve, reject) => {
            server.close((error) => {
                if (error) reject(error);
                else resolve();
            });
        });
        await vite.close();
    });

    it('preserves the SPA login path while handling login submissions', async () => {
        const page = await fetch(`${origin}/login?redirect=%2F`);
        expect(await page.text()).toBe('/login?redirect=%2F');
        const login = await fetch(`${origin}/logIn`, {
            method: 'POST',
            body: JSON.stringify({ username: 'datahub' }),
            headers: { 'Content-Type': 'application/json' },
        });
        expect(login.status).toBe(200);
        expect(login.headers.get('set-cookie')).toContain('PLAY_SESSION');
        const config = await fetch(`${origin}/mfe/config`);
        expect(await config.json()).toEqual({ microFrontends: [] });
    });

    it.each([
        ['/dataset/example?tab=schema', '/dataset/example?tab=schema'],
        ['/logOut', '/logOut'],
        [undefined, '/'],
    ])('redirects an authenticated user to %s', async (path, expected) => {
        const query = path === undefined ? '' : `?redirect_uri=${encodeURIComponent(path)}`;
        const response = await fetch(`${origin}/authenticate${query}`, {
            headers: { Cookie: 'actor=urn:li:corpuser:datahub' },
            redirect: 'manual',
        });
        expect(response.status).toBe(302);
        expect(response.headers.get('location')).toBe(expected);
    });

    it('sends an unauthenticated user to login with an encoded redirect_uri', async () => {
        const path = '/dataset/example?tab=schema&view=columns';
        const response = await fetch(`${origin}/authenticate?redirect_uri=${encodeURIComponent(path)}`, {
            redirect: 'manual',
        });
        expect(response.status).toBe(302);
        expect(response.headers.get('location')).toBe(`/login?redirect_uri=${encodeURIComponent(path)}`);
    });

    it.each([
        'https://example.com/path',
        '//example.com/path',
        '///example.com',
        'mailto:user@example.com',
        '/\\\\example.com',
    ])('rejects an off-site redirect %s', async (path) => {
        await Promise.all(
            ['actor=urn:li:corpuser:datahub', ''].map(async (cookie) => {
                const response = await fetch(`${origin}/authenticate?redirect_uri=${encodeURIComponent(path)}`, {
                    headers: { Cookie: cookie },
                    redirect: 'manual',
                });
                expect(response.status).toBe(302);
                expect(response.headers.get('location')).toBe(cookie ? '/' : '/login?redirect_uri=%2F');
            }),
        );
    });
});
