/**
 * Auth stubs for lite mode — replaces the Play frontend server's auth endpoints.
 *
 * In lite mode there is no Play server, so these middleware handlers serve:
 *   POST /logIn         — sets actor + PLAY_SESSION cookies; 200 (400 for a user the directory
 *                         does not know, as Play answers bad native credentials)
 *   POST /signUp        — creates the user (through GMS native auth with real auth, else the
 *                         directory), then logs it in; 200
 *   GET  /authenticate  — 200 if actor cookie present, else 302 → /login
 *   GET  /admin         — 200 (Play health endpoint; smoke-test healthcheck)
 *   GET  /logOut        — clears cookies, 302 → /
 *   POST /track         — 204 (analytics sink)
 *   GET  /mfe/config    — 200 with an empty micro frontend configuration
 */
import type { Connect } from 'vite';

export type SignUpDetails = {
    fullName: string;
    email: string;
    title?: string;
};

/**
 * Where users live when the stub fronts a real GMS: `/logIn` only admits users that exist and
 * `/signUp` creates them, mirroring Play's native auth (which never provisions on login). Without
 * a directory any username is admitted.
 */
export type UserDirectory = {
    exists(req: Connect.IncomingMessage, actorUrn: string): Promise<boolean>;
    create(req: Connect.IncomingMessage, actorUrn: string, details: SignUpDetails): Promise<void>;
};

/**
 * Real GMS auth backend. When provided, `/logIn` verifies credentials against GMS's native
 * auth and returns a real JWT session token — policies, privileges, and token expiry all work.
 */
export type GmsAuth = {
    gmsUrl: string;
    systemClientId: string;
    systemClientSecret: string;
    /** username → password from user.props (same JAAS format as Docker's Play frontend). */
    userProps: Record<string, string>;
};

function sessionResponse(
    res: import('http').ServerResponse,
    actorUrn: string,
    message: string,
    accessToken?: string,
): void {
    const token = accessToken || 'lite';
    res.writeHead(200, {
        'Content-Type': 'application/json',
        'Set-Cookie': [`actor=${actorUrn}; Path=/; SameSite=Lax`, `PLAY_SESSION=${token}; Path=/; SameSite=Lax`],
    });
    res.end(JSON.stringify({ message }));
}

function systemAuthHeaders(gmsAuth: GmsAuth): Record<string, string> {
    return {
        'Content-Type': 'application/json',
        Authorization: `Basic ${gmsAuth.systemClientId}:${gmsAuth.systemClientSecret}`,
    };
}

/** Whether GMS holds native credentials for the user that match, as Play checks signed-up users. */
async function verifyNativeCredentials(gmsAuth: GmsAuth, actorUrn: string, password: string): Promise<boolean> {
    const resp = await fetch(`${gmsAuth.gmsUrl}/auth/verifyNativeUserCredentials`, {
        method: 'POST',
        headers: systemAuthHeaders(gmsAuth),
        body: JSON.stringify({ userUrn: actorUrn, password }),
    });
    if (!resp.ok) return false;
    const body = (await resp.json()) as { doesPasswordMatch?: boolean };
    return body.doesPasswordMatch === true;
}

/**
 * Create a native user through GMS's /auth/signUp, the endpoint Play's /signUp calls: GMS checks
 * the invite token and stores the password, so the user can log in afterwards.
 */
async function gmsSignUp(
    gmsAuth: GmsAuth,
    actorUrn: string,
    details: SignUpDetails,
    password: string,
    inviteToken: string,
): Promise<string | undefined> {
    const resp = await fetch(`${gmsAuth.gmsUrl}/auth/signUp`, {
        method: 'POST',
        headers: systemAuthHeaders(gmsAuth),
        body: JSON.stringify({
            userUrn: actorUrn,
            fullName: details.fullName,
            email: details.email,
            ...(details.title ? { title: details.title } : {}),
            password,
            inviteToken,
        }),
    });
    if (resp.ok) return undefined;
    return `${resp.status}: ${(await resp.text()).slice(0, 160)}`;
}

/**
 * Authenticate as Docker's Play frontend does, against the JAAS user.props file or, for
 * signed-up users, GMS's native credentials, and mint a real JWT session token via GMS's
 * /generateSessionTokenForUser (same endpoint Play uses).
 *
 * This creates a stateless SESSION token (v1) validated by signature only — not a stateful
 * PERSONAL token (v2) that requires a DB entity lookup. The distinction matters: PERSONAL
 * tokens created via `createAccessToken` need the token hash to exist in the DB, which can
 * fail on warm restarts. SESSION tokens just need the signing key.
 */
async function gmsLogin(
    gmsAuth: GmsAuth,
    username: string,
    password: string,
): Promise<{ actorUrn: string; accessToken: string } | string> {
    const actorUrn = `urn:li:corpuser:${username}`;

    // Step 1: Check credentials against user.props (plaintext username:password pairs), then
    // against the native credentials GMS stores for users who signed up.
    const expectedPassword = gmsAuth.userProps[username];
    const valid =
        expectedPassword !== undefined
            ? expectedPassword === password
            : !!password && (await verifyNativeCredentials(gmsAuth, actorUrn, password));
    if (!valid) {
        return 'Invalid Credentials';
    }

    // Step 2: Generate a stateless session token via the same REST endpoint Play uses.
    // Play sends X-DataHubLoginSource so GMS stamps loginSource on the LogInEvent audit record.
    const tokenResp = await fetch(`${gmsAuth.gmsUrl}/auth/generateSessionTokenForUser`, {
        method: 'POST',
        headers: { ...systemAuthHeaders(gmsAuth), 'X-DataHubLoginSource': 'PASSWORD_LOGIN' },
        body: JSON.stringify({ userId: username }),
    });
    if (!tokenResp.ok) return `Failed to generate session token: ${tokenResp.status}`;
    const tokenBody = (await tokenResp.json()) as { accessToken?: string };
    const { accessToken } = tokenBody;
    if (!accessToken) return 'No access token returned';

    return { actorUrn, accessToken };
}

/**
 * Mint a stateless SESSION token for a user without password verification.
 * Used after native sign up to establish the same session as password login.
 */
export async function mintSessionToken(gmsAuth: GmsAuth, username: string): Promise<string | null> {
    try {
        const resp = await fetch(`${gmsAuth.gmsUrl}/auth/generateSessionTokenForUser`, {
            method: 'POST',
            headers: systemAuthHeaders(gmsAuth),
            body: JSON.stringify({ userId: username }),
        });
        if (!resp.ok) return null;
        const body = (await resp.json()) as { accessToken?: string };
        return body.accessToken || null;
    } catch {
        return null;
    }
}

/** Sign a user up through GMS, then mint their session as /logIn would. */
async function signUpNative(
    gmsAuth: GmsAuth,
    actorUrn: string,
    details: SignUpDetails,
    password: string,
    inviteToken: string,
): Promise<{ accessToken: string } | string> {
    const failure = await gmsSignUp(gmsAuth, actorUrn, details, password, inviteToken);
    if (failure) {
        console.error(`[lite] GMS sign up refused for ${details.email}: ${failure}`);
        return 'Failed to sign up';
    }
    const accessToken = await mintSessionToken(gmsAuth, details.email);
    return accessToken ? { accessToken } : 'Failed to generate session token';
}

function badRequest(res: import('http').ServerResponse, message: string): void {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ message }));
}

/**
 * Register auth stub middleware on a Vite dev server.
 *
 * @param middlewares - Vite's connect middleware stack
 * @param users - optional directory that gates /logIn and backs /signUp
 */
export function registerAuthStubs(middlewares: Connect.Server, users?: UserDirectory, gmsAuth?: GmsAuth): void {
    // Connect mounts are case-insensitive and restore /login as /logIn on next(),
    // causing the SPA page to hit the Play proxy. Match this endpoint explicitly.
    middlewares.use(((req, res, next) => {
        if (req.method !== 'POST' || req.url?.split('?')?.[0] !== '/logIn') {
            next();
            return;
        }

        collectBody(req, (err, body) => {
            if (err) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ message: 'Bad request' }));
                return;
            }

            let username: string;
            let password: string;

            try {
                const parsed = JSON.parse(body) as { username?: unknown; password?: unknown } | null;
                username = typeof parsed?.username === 'string' ? parsed.username : '';
                password = typeof parsed?.password === 'string' ? parsed.password : '';
            } catch {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ message: 'Invalid JSON body' }));
                return;
            }

            if (!username || /[\r\n;]/.test(username)) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ message: 'Missing username' }));
                return;
            }

            // Real auth: verify credentials against GMS and get a JWT
            if (gmsAuth) {
                gmsLogin(gmsAuth, username, password).then(
                    (result) => {
                        if (typeof result === 'string') {
                            badRequest(res, result);
                        } else {
                            sessionResponse(res, result.actorUrn, 'Login successful', result.accessToken);
                        }
                    },
                    (e: unknown) => {
                        console.error(`[lite] GMS login failed for ${username}: ${String(e)}`);
                        res.writeHead(500, { 'Content-Type': 'application/json' });
                        res.end(JSON.stringify({ message: 'Login failed' }));
                    },
                );
                return;
            }

            const actorUrn = `urn:li:corpuser:${username}`;

            if (!users) {
                sessionResponse(res, actorUrn, 'Login successful');
                return;
            }
            users.exists(req, actorUrn).then(
                (known) => {
                    if (known) sessionResponse(res, actorUrn, 'Login successful');
                    else badRequest(res, 'Invalid Credentials');
                },
                (e: unknown) => {
                    console.error(`[lite] user lookup failed for ${username}: ${String(e)}`);
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ message: 'User lookup failed' }));
                },
            );
        });
    }) as Connect.NextHandleFunction);

    // POST /signUp — Play validates the invite token against GMS and creates native credentials.
    // With real auth the stub does the same through GMS; otherwise it has no credentials to
    // create, so it only requires the token to be present and adds the user to the directory.
    middlewares.use('/signUp', ((req, res, next) => {
        if (req.method !== 'POST') {
            next();
            return;
        }
        if (!users && !gmsAuth) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ message: 'Sign up is not available without a user directory' }));
            return;
        }
        collectBody(req, (err, body) => {
            let parsed: { fullName?: string; email?: string; title?: string; password?: string; inviteToken?: string };
            try {
                if (err) throw err;
                parsed = JSON.parse(body);
            } catch {
                badRequest(res, 'Invalid JSON body');
                return;
            }
            const fullName = (parsed.fullName || '').trim();
            const email = (parsed.email || '').trim();
            const missing = [
                [!fullName, 'Full name must not be empty.'],
                [!email, 'Email must not be empty.'],
                [!parsed.password, 'Password must not be empty.'],
                [!parsed.inviteToken, 'Invite token must not be empty.'],
            ].find(([absent]) => absent);
            if (missing) {
                badRequest(res, String(missing?.[1]));
                return;
            }
            const actorUrn = `urn:li:corpuser:${email}`;
            const details = { fullName, email, title: parsed.title?.trim() || undefined };
            if (gmsAuth) {
                signUpNative(gmsAuth, actorUrn, details, String(parsed.password), String(parsed.inviteToken)).then(
                    (result) => {
                        if (typeof result === 'string') badRequest(res, result);
                        else sessionResponse(res, actorUrn, 'Sign up successful', result.accessToken);
                    },
                    (e: unknown) => {
                        console.error(`[lite] sign up failed for ${email}: ${String(e)}`);
                        res.writeHead(500, { 'Content-Type': 'application/json' });
                        res.end(JSON.stringify({ message: 'Sign up failed' }));
                    },
                );
                return;
            }
            if (!users) return;
            users.create(req, actorUrn, details).then(
                () => sessionResponse(res, actorUrn, 'Sign up successful'),
                (e: unknown) => {
                    console.error(`[lite] sign up failed for ${email}: ${String(e)}`);
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ message: 'Sign up failed' }));
                },
            );
        });
    }) as Connect.NextHandleFunction);

    // GET /authenticate
    middlewares.use('/authenticate', ((req, res, next) => {
        if (req.method !== 'GET') {
            next();
            return;
        }

        let redirectPath = new URL(req.url || '/', 'http://localhost').searchParams.get('redirect_uri') ?? '/';
        // Play keeps /logOut as the target (there is no base path in lite mode), and rejects
        // schemes, authorities and invalid URI characters before using the native login page.
        if (
            /^\/\//.test(redirectPath.trim()) ||
            /^[a-z][a-z\d+.-]*:/i.test(redirectPath) ||
            /[\s\\]/.test(redirectPath)
        ) {
            redirectPath = '/';
        }
        const cookies = parseCookies(req.headers.cookie);
        res.writeHead(302, {
            Location: cookies.actor ? redirectPath : `/login?redirect_uri=${encodeURIComponent(redirectPath)}`,
        });
        res.end();
    }) as Connect.NextHandleFunction);

    // GET /admin — Play's health endpoint; the smoke-test healthcheck asserts 200 on it.
    middlewares.use('/admin', ((req, res, next) => {
        if (req.method !== 'GET') {
            next();
            return;
        }
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('OK');
    }) as Connect.NextHandleFunction);

    // GET /logOut
    middlewares.use('/logOut', ((req, res, next) => {
        if (req.method !== 'GET') {
            next();
            return;
        }

        res.writeHead(302, {
            Location: '/',
            'Set-Cookie': [
                'actor=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT',
                'PLAY_SESSION=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT',
            ],
        });
        res.end();
    }) as Connect.NextHandleFunction);

    // POST /track
    middlewares.use('/track', ((req, res, next) => {
        if (req.method !== 'POST') {
            next();
            return;
        }
        res.writeHead(204);
        res.end();
    }) as Connect.NextHandleFunction);

    // GET /mfe/config
    middlewares.use('/mfe/config', ((req, res, next) => {
        if (req.method !== 'GET') {
            next();
            return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ microFrontends: [] }));
    }) as Connect.NextHandleFunction);
}

// ── Helpers ──────────────────────────────────────────────────────────────────

export function parseCookies(cookieHeader: string | undefined): Record<string, string> {
    if (!cookieHeader) return {};
    return cookieHeader.split(';').reduce<Record<string, string>>((acc, pair) => {
        const eqIdx = pair.indexOf('=');
        if (eqIdx > 0) {
            const key = pair.slice(0, eqIdx).trim();
            const val = pair.slice(eqIdx + 1).trim();
            try {
                acc[key] = decodeURIComponent(val);
            } catch {
                // A malformed browser cookie must not break every proxied request.
            }
        }
        return acc;
    }, {});
}

function collectBody(req: Connect.IncomingMessage, cb: (err: Error | null, body: string) => void): void {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => cb(null, Buffer.concat(chunks).toString('utf-8')));
    req.on('error', (err: Error) => cb(err, ''));
}
