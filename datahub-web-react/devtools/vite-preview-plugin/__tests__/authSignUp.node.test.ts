import { IncomingMessage, Server, ServerResponse, createServer } from 'http';
import type { AddressInfo } from 'net';
import { createServer as createViteServer } from 'vite';
import { describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- devtools/ is outside src/ and has no path alias
import { registerAuthStubs } from '../authStubs';

const SYSTEM_AUTH = 'Basic __datahub_system:secret';
const INVITE = 'invite-token';

/** A GMS that holds native credentials, answering only the system client, as GMS with auth does. */
function fakeGms(): Server {
    const passwords: Record<string, string> = {};
    return createServer((req: IncomingMessage, res: ServerResponse) => {
        let raw = '';
        req.on('data', (chunk) => {
            raw += chunk;
        });
        req.on('end', () => {
            if (req.headers.authorization !== SYSTEM_AUTH) {
                res.writeHead(401).end();
                return;
            }
            const body = JSON.parse(raw || '{}') as {
                userUrn: string;
                userId: string;
                password: string;
                inviteToken: string;
            };
            const reply = (value: object) => res.writeHead(200).end(JSON.stringify(value));
            if (req.url === '/auth/signUp') {
                if (body.inviteToken !== INVITE) {
                    res.writeHead(400).end();
                    return;
                }
                passwords[body.userUrn] = body.password;
                reply({ isNativeUserCreated: true });
            } else if (req.url === '/auth/verifyNativeUserCredentials') {
                reply({ doesPasswordMatch: passwords[body.userUrn] === body.password });
            } else if (req.url === '/auth/generateSessionTokenForUser') {
                reply({ accessToken: `eyJ.${body.userId}` });
            } else {
                res.writeHead(404).end();
            }
        });
    });
}

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

const post = (url: string, body: object) =>
    fetch(url, { method: 'POST', body: JSON.stringify(body), headers: { 'Content-Type': 'application/json' } });

describe('sign up with real auth', () => {
    it('creates a native user through GMS who can then log in', async () => {
        const gms = fakeGms();
        const gmsUrl = await listen(gms);
        const vite = await createViteServer({
            configFile: false,
            cacheDir: 'node_modules/.vite-auth-signup-test',
            appType: 'custom',
            optimizeDeps: { noDiscovery: true, include: [] },
            server: { middlewareMode: true, watch: null },
        });
        registerAuthStubs(vite.middlewares, undefined, {
            gmsUrl,
            systemClientId: '__datahub_system',
            systemClientSecret: 'secret',
            userProps: { datahub: 'datahub' },
        });
        const app = createServer(vite.middlewares);
        const origin = await listen(app);
        const user = { fullName: 'New User', email: 'new.user@example.com', password: 'pw-1', inviteToken: INVITE };
        try {
            const signUp = await post(`${origin}/signUp`, user);
            expect(signUp.status).toBe(200);
            expect(signUp.headers.get('set-cookie')).toContain('PLAY_SESSION=eyJ.new.user@example.com');

            const login = await post(`${origin}/logIn`, { username: user.email, password: user.password });
            expect(login.status).toBe(200);
            const wrong = await post(`${origin}/logIn`, { username: user.email, password: 'wrong' });
            expect(wrong.status).toBe(400);

            const badInvite = await post(`${origin}/signUp`, { ...user, email: 'other@example.com', inviteToken: 'x' });
            expect(badInvite.status).toBe(400);
        } finally {
            await close(app);
            await vite.close();
            await close(gms);
        }
    });
});
