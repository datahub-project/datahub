/**
 * Auth helpers for the Lighthouse web-vitals runner.
 *
 * Local A/B uses Playwright storageState or /logIn cookies on localhost.
 * Remote instances (e.g. https://preview.example.com) typically sit behind
 * SSO, so username/password /logIn is not available. A personal access token
 * is injected as `Authorization: Bearer` on every request; the SPA still
 * needs an `actor` cookie to treat the session as logged-in.
 *
 * Never commit tokens. Read them from the environment.
 */

const DEFAULT_TOKEN_ENV = 'DATAHUB_GMS_TOKEN';
const FALLBACK_TOKEN_ENV = 'DATAHUB_ACCESS_TOKEN';

export function decodeJwtPayload(token) {
    if (typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length < 2) return null;
    try {
        const padded = parts[1].replace(/-/g, '+').replace(/_/g, '/');
        const pad = padded.length % 4 === 0 ? '' : '='.repeat(4 - (padded.length % 4));
        return JSON.parse(Buffer.from(padded + pad, 'base64').toString('utf8'));
    } catch {
        return null;
    }
}

export function actorUrnFromToken(token, actorUrnOverride) {
    if (actorUrnOverride) return actorUrnOverride;
    const claims = decodeJwtPayload(token);
    if (!claims || typeof claims !== 'object') {
        throw new Error(
            'Access token is not a JWT (or has no payload). Set auth.actorUrn to the user URN, e.g. urn:li:corpuser:you.',
        );
    }
    if (typeof claims.actorId !== 'string' || !claims.actorId) {
        throw new Error(
            'Access token JWT has no actorId claim. Set auth.actorUrn to the user URN, e.g. urn:li:corpuser:you.',
        );
    }
    const actorType = claims.actorType === 'CORP_GROUP' || claims.actorType === 'GROUP' ? 'corpGroup' : 'corpuser';
    return `urn:li:${actorType}:${claims.actorId}`;
}

export function isAnonymousMode({ args = {}, cfg = {} } = {}) {
    const auth = cfg.auth || {};
    return args['no-auth'] === 'true' || auth.mode === 'anonymous';
}

export function resolveAccessToken({ args = {}, cfg = {}, env = process.env } = {}) {
    if (args.token) {
        throw new Error('Refusing --token because process arguments are observable. Use --token-env or DATAHUB_GMS_TOKEN.');
    }
    const auth = cfg.auth || {};
    const envName = args['token-env'] && args['token-env'] !== 'true' ? args['token-env'] : auth.tokenEnv || DEFAULT_TOKEN_ENV;
    if (env[envName]) return env[envName];
    if (env[FALLBACK_TOKEN_ENV]) return env[FALLBACK_TOKEN_ENV];
    if (typeof auth.token === 'string' && auth.token.length > 0) {
        throw new Error(
            'auth.token in config.json is refused (tokens must not be committed). ' +
                `Export ${envName} or ${FALLBACK_TOKEN_ENV}, or pass --token-env ${envName}.`,
        );
    }
    return null;
}

export function originHost(origin) {
    return new URL(origin).hostname;
}

export function stripTrailingSlash(origin) {
    return String(origin).replace(/\/+$/, '');
}

export function cookiesForTokenAuth({ origin, actorUrn }) {
    const url = new URL(origin);
    const cookie = {
        name: 'actor',
        value: actorUrn,
        domain: url.hostname,
        path: '/',
        sameSite: 'Lax',
    };
    if (url.protocol === 'https:') cookie.secure = true;
    return [cookie];
}

export function extraHeadersForToken(token) {
    return { Authorization: `Bearer ${token}` };
}

export function isAuthRejection(error) {
    const status = error && error.status;
    return status === 401 || status === 403;
}

export function isSameOrigin(requestUrl, pageOrigin) {
    try {
        return new URL(requestUrl).origin === new URL(pageOrigin).origin;
    } catch {
        return false;
    }
}

/**
 * Runs in every new document before app JS. Skips the welcome modal and
 * pre-accepts common system-use-consent policy versions so a SaaS notice
 * does not become the LCP element.
 */
export function sessionBootstrapSource(actorUrn) {
    return `
        try {
            localStorage.setItem('skipWelcomeModal', 'true');
            var urn = ${JSON.stringify(actorUrn)};
            var versions = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'];
            for (var i = 0; i < versions.length; i++) {
                var key = 'datahub.systemUseConsent.' + encodeURIComponent(urn) + '.' + encodeURIComponent(versions[i]);
                sessionStorage.setItem(key, 'accepted');
            }
        } catch (e) {}
    `;
}

export function toPuppeteerCookie(c, fallbackDomain = 'localhost') {
    const out = {
        name: c.name,
        value: c.value,
        domain: c.domain || fallbackDomain,
        path: c.path || '/',
    };
    if (typeof c.expires === 'number' && c.expires > 0) out.expires = c.expires;
    if (c.httpOnly != null) out.httpOnly = c.httpOnly;
    if (c.secure != null) out.secure = c.secure;
    // Skip SameSite=None on http origins (browsers require Secure for None).
    if (c.sameSite === 'Strict' || c.sameSite === 'Lax') out.sameSite = c.sameSite;
    return out;
}

export function resolveTargets(cfg, args = {}) {
    const originArg = args.origin && args.origin !== 'true' ? stripTrailingSlash(args.origin) : null;
    const cfgOrigin = cfg.origin ? stripTrailingSlash(cfg.origin) : null;
    const a = cfg.builds?.A;
    const b = cfg.builds?.B;
    const remoteA = a?.origin ? stripTrailingSlash(a.origin) : null;
    const remoteB = b?.origin ? stripTrailingSlash(b.origin) : null;

    if (originArg || cfgOrigin) {
        const origin = originArg || cfgOrigin;
        return {
            mode: 'remote',
            origins: { A: origin },
            versions: {
                A: { label: cfg.label || originHost(origin), origin, commit: cfg.commit || null },
            },
            backend: origin,
        };
    }

    if (remoteA && !a?.dist) {
        const origins = { A: remoteA };
        const versions = {
            A: { label: a.label || originHost(remoteA), origin: remoteA, commit: a.commit || null },
        };
        if (remoteB && !b?.dist) {
            origins.B = remoteB;
            versions.B = { label: b.label || originHost(remoteB), origin: remoteB, commit: b.commit || null };
        }
        return {
            mode: 'remote',
            origins,
            versions,
            backend: cfg.backendUrl || remoteA,
        };
    }

    if (a?.dist) {
        const versions = {
            A: { label: a.label, dist: a.dist, commit: a.commit || null, port: a.port },
        };
        if (b?.dist) {
            versions.B = { label: b.label, dist: b.dist, commit: b.commit || null, port: b.port };
        }
        return {
            mode: 'ab-local',
            origins: null,
            versions,
            backend: cfg.backendUrl,
        };
    }

    throw new Error(
        'Config must either set `origin` (remote instance), `builds.A.origin` (+ optional B), ' +
            'or local `builds.A.dist` (and optional `builds.B.dist`).',
    );
}

export function buildSchedule(runsPerBuild, warmupPerBuild, builds) {
    const schedule = [];
    const total = runsPerBuild + warmupPerBuild;
    for (let i = 0; i < total; i += 1) {
        for (const build of builds) {
            schedule.push({ build, warmup: i < warmupPerBuild });
        }
    }
    return schedule;
}
