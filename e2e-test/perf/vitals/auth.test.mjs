import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    actorUrnFromToken,
    buildSchedule,
    cookiesForTokenAuth,
    decodeJwtPayload,
    extraHeadersForToken,
    isAnonymousMode,
    isAuthRejection,
    isSameOrigin,
    resolveAccessToken,
    resolveTargets,
    sessionBootstrapSource,
} from './auth.mjs';

function jwtWith(payload) {
    const header = Buffer.from(JSON.stringify({ alg: 'none', typ: 'JWT' })).toString('base64url');
    const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
    return `${header}.${body}.sig`;
}

describe('decodeJwtPayload', () => {
    it('reads a DataHub PAT payload', () => {
        const token = jwtWith({ actorType: 'USER', actorId: 'alice' });
        assert.equal(decodeJwtPayload(token).actorId, 'alice');
    });

    it('returns null for a non-JWT secret', () => {
        assert.equal(decodeJwtPayload('not-a-jwt'), null);
    });
});

describe('actorUrnFromToken', () => {
    it('builds a corpuser URN from actorId', () => {
        const token = jwtWith({ actorType: 'USER', actorId: 'alice' });
        assert.equal(actorUrnFromToken(token), 'urn:li:corpuser:alice');
    });

    it('honours an explicit actorUrn override', () => {
        assert.equal(actorUrnFromToken('opaque', 'urn:li:corpuser:bob'), 'urn:li:corpuser:bob');
    });

    it('throws when a non-JWT has no override', () => {
        assert.throws(() => actorUrnFromToken('opaque'), /auth.actorUrn/);
    });
});

describe('isAnonymousMode', () => {
    it('is true for --no-auth or auth.mode anonymous', () => {
        assert.equal(isAnonymousMode({ args: { 'no-auth': 'true' } }), true);
        assert.equal(isAnonymousMode({ cfg: { auth: { mode: 'anonymous' } } }), true);
        assert.equal(isAnonymousMode({ args: {}, cfg: { auth: {} } }), false);
    });
});

describe('resolveAccessToken', () => {
    it('prefers DATAHUB_GMS_TOKEN', () => {
        const token = resolveAccessToken({
            cfg: { auth: {} },
            env: { DATAHUB_GMS_TOKEN: 'gms', DATAHUB_ACCESS_TOKEN: 'other' },
        });
        assert.equal(token, 'gms');
    });

    it('honours a custom tokenEnv', () => {
        const token = resolveAccessToken({
            cfg: { auth: { tokenEnv: 'MY_PAT' } },
            env: { MY_PAT: 'custom', DATAHUB_GMS_TOKEN: 'gms' },
        });
        assert.equal(token, 'custom');
    });

    it('refuses a token baked into config.json', () => {
        assert.throws(
            () => resolveAccessToken({ cfg: { auth: { token: 'secret' } }, env: {} }),
            /refused/,
        );
    });

    it('refuses a token in process arguments', () => {
        assert.throws(() => resolveAccessToken({ args: { token: 'secret' }, env: {} }), /observable/);
    });
});

describe('cookiesForTokenAuth', () => {
    it('sets a secure actor cookie for https origins', () => {
        const [cookie] = cookiesForTokenAuth({
            origin: 'https://preview.example.com',
            actorUrn: 'urn:li:corpuser:alice',
        });
        assert.equal(cookie.name, 'actor');
        assert.equal(cookie.domain, 'preview.example.com');
        assert.equal(cookie.secure, true);
        assert.equal(cookie.sameSite, 'Lax');
    });

    it('scopes each remote A/B cookie to its own host', () => {
        const actorUrn = 'urn:li:corpuser:alice';
        const [a] = cookiesForTokenAuth({ origin: 'https://a.example.com', actorUrn });
        const [b] = cookiesForTokenAuth({ origin: 'https://b.example.com', actorUrn });
        assert.equal(a.domain, 'a.example.com');
        assert.equal(b.domain, 'b.example.com');
    });
});

describe('extraHeadersForToken', () => {
    it('sends a Bearer header', () => {
        assert.deepEqual(extraHeadersForToken('abc'), { Authorization: 'Bearer abc' });
    });
});

describe('isAuthRejection', () => {
    it('is true only for 401 and 403', () => {
        assert.equal(isAuthRejection({ status: 401 }), true);
        assert.equal(isAuthRejection({ status: 403 }), true);
        assert.equal(isAuthRejection({ status: 404 }), false);
        assert.equal(isAuthRejection(new Error('network')), false);
    });
});

describe('isSameOrigin', () => {
    it('matches only requests to the measured origin', () => {
        assert.equal(
            isSameOrigin('https://preview.example.com/api/graphql', 'https://preview.example.com'),
            true,
        );
        assert.equal(
            isSameOrigin('https://assets.example.com/logo.png', 'https://preview.example.com'),
            false,
        );
        assert.equal(isSameOrigin('not a url', 'https://preview.example.com'), false);
    });
});

describe('sessionBootstrapSource', () => {
    it('embeds the actor URN and consent keys', () => {
        const src = sessionBootstrapSource('urn:li:corpuser:alice');
        assert.match(src, /urn:li:corpuser:alice/);
        assert.match(src, /datahub.systemUseConsent/);
        assert.match(src, /skipWelcomeModal/);
    });
});

describe('resolveTargets', () => {
    it('treats --origin as a single remote instance', () => {
        const t = resolveTargets({}, { origin: 'https://preview.example.com/' });
        assert.equal(t.mode, 'remote');
        assert.equal(t.origins.A, 'https://preview.example.com');
        assert.equal(t.origins.B, undefined);
    });

    it('compares two remote origins when both builds omit dist', () => {
        const t = resolveTargets({
            builds: {
                A: { label: 'test', origin: 'https://preview.example.com' },
                B: { label: 'auto', origin: 'https://other.example.com' },
            },
        });
        assert.equal(t.mode, 'remote');
        assert.equal(t.origins.B, 'https://other.example.com');
    });

    it('measures one local dist when only build A is set', () => {
        const t = resolveTargets({
            backendUrl: 'stub',
            builds: { A: { label: 'main@abc1234', dist: 'builds/head/dist', port: 8181 } },
        });
        assert.equal(t.mode, 'ab-local');
        assert.equal(t.versions.A.dist, 'builds/head/dist');
        assert.equal(t.versions.B, undefined);
    });

    it('keeps local A/B when both dist paths are set', () => {
        const t = resolveTargets({
            backendUrl: 'http://localhost:9002',
            builds: {
                A: { label: 'base', dist: 'builds/base/dist', port: 8181 },
                B: { label: 'pr', dist: 'builds/pr/dist', port: 8182 },
            },
        });
        assert.equal(t.mode, 'ab-local');
        assert.equal(t.backend, 'http://localhost:9002');
    });
});

describe('buildSchedule', () => {
    it('alternates A,B when both builds are present', () => {
        const s = buildSchedule(1, 1, ['A', 'B']);
        assert.deepEqual(
            s.map((x) => `${x.build}:${x.warmup}`),
            ['A:true', 'B:true', 'A:false', 'B:false'],
        );
    });

    it('runs a single remote origin without inventing a B', () => {
        const s = buildSchedule(2, 1, ['A']);
        assert.equal(s.filter((x) => x.build === 'B').length, 0);
        assert.equal(s.filter((x) => !x.warmup).length, 2);
    });
});
