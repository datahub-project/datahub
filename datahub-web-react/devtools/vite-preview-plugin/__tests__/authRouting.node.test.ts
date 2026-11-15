import { createServer } from 'http';
import type { AddressInfo } from 'net';
import { createServer as createViteServer } from 'vite';
import { describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- devtools/ is outside src/ and has no path alias
import { registerAuthStubs } from '../authStubs';

describe('login routing', () => {
    it('preserves the SPA login path while handling login submissions', async () => {
        const vite = await createViteServer({
            configFile: false,
            cacheDir: 'node_modules/.vite-auth-routing-test',
            appType: 'custom',
            optimizeDeps: { noDiscovery: true, include: [] },
            server: { middlewareMode: true, watch: null },
        });
        const app = vite.middlewares;
        registerAuthStubs(app);
        app.use((req, res) => res.end(req.url));
        const server = createServer(app);
        await new Promise<void>((resolve) => {
            server.listen(0, '127.0.0.1', resolve);
        });
        const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
        try {
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
        } finally {
            await new Promise<void>((resolve, reject) => {
                server.close((error) => {
                    if (error) reject(error);
                    else resolve();
                });
            });
            await vite.close();
        }
    });
});
