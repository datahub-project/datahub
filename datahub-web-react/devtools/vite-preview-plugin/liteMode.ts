/** Vite's local stack adapter: session endpoints and GMS proxying without a Play server. */
import { existsSync, readFileSync } from 'fs';
import { request as httpRequest } from 'http';
import type { IncomingMessage, OutgoingHttpHeaders, ServerResponse } from 'http';
import { join, resolve } from 'path';
import { promisify } from 'util';
import type { Connect, Plugin, PreviewServer } from 'vite';
import { brotliDecompress, gunzip } from 'zlib';

/* eslint-disable import-alias/import-alias -- server tooling has no src alias */
import { parseCookies, registerAuthStubs } from './authStubs';
import type { GmsAuth, UserDirectory } from './authStubs';
import { applyFlagOverrides, parseFlagOverrides } from './flagOverrides';

/* eslint-enable import-alias/import-alias */

type LiteStack = {
    bootId: string;
    default: string;
    instances: Record<string, string>;
    auth: boolean;
    systemClientId?: string | null;
    systemClientSecret?: string | null;
    frontendPort?: number | null;
};

function findRepoRoot(start: string): string {
    let dir = resolve(start);
    for (let i = 0; i < 8; i += 1) {
        if (existsSync(join(dir, 'settings.gradle'))) return dir;
        const parent = resolve(dir, '..');
        if (parent === dir) break;
        dir = parent;
    }
    throw new Error('Cannot find settings.gradle for lite mode');
}

function readLiteStack(): LiteStack {
    const file = process.env.GMS_LITE_STACK_FILE;
    if (!file) throw new Error('Lite mode requires GMS_LITE_STACK_FILE; run scripts/dev/datahub-dev.sh start --local');
    const stack = JSON.parse(readFileSync(file, 'utf-8')) as LiteStack;
    if (!stack.instances || !stack.default || !stack.instances[stack.default] || typeof stack.auth !== 'boolean') {
        throw new Error(`Invalid local stack metadata: ${file}`);
    }
    Object.values(stack.instances).forEach((url) => {
        const target = new URL(url);
        if (target.protocol !== 'http:' || !['localhost', '127.0.0.1', '[::1]'].includes(target.hostname)) {
            throw new Error('Lite mode requires local HTTP GMS instances');
        }
    });
    if (stack.auth && (!stack.systemClientId || !stack.systemClientSecret)) {
        throw new Error('Local stack metadata is missing system credentials; restart the local stack');
    }
    return stack;
}

function instanceForRequest(req: IncomingMessage): string | undefined {
    const header = req.headers['x-datahub-instance'];
    if (typeof header === 'string' && header) return header;
    return parseCookies(req.headers.cookie).__instance || undefined;
}

function stackTargetFor(stack: LiteStack, instance: string | undefined): string {
    return (instance && stack.instances[instance]) || stack.instances[stack.default];
}

const GMS_PATH_PREFIXES = [
    '/api',
    '/openapi',
    '/actuator',
    '/config',
    '/schema-registry',
    '/aspects',
    '/entities',
    '/entitiesV2',
    '/entitiesVersionedV2',
    '/relationships',
    '/usageStats',
    '/runs',
    '/operations',
    '/platform',
    '/analytics',
];

function isGmsPath(url: string): boolean {
    return GMS_PATH_PREFIXES.some((p) => url === p || url.startsWith(`${p}/`) || url.startsWith(`${p}?`));
}

/** Only the no-auth lane reads unverified token claims; authenticated GMS validates tokens itself. */
export function actorFromDataHubToken(token: string): string | undefined {
    const parts = token.split('.');
    if (parts.length !== 3) return undefined;
    try {
        const payload = JSON.parse(Buffer.from(parts?.[1] || '', 'base64').toString('utf-8')) as {
            actorId?: string;
            actorType?: string;
        };
        if (!payload.actorId) return undefined;
        return payload.actorType === 'SERVICE' && payload.actorId.startsWith('urn:')
            ? payload.actorId
            : `urn:li:corpuser:${payload.actorId}`;
    } catch {
        return undefined;
    }
}

function actorForRequest(req: IncomingMessage): string {
    const cookies = parseCookies(req.headers.cookie);
    if (cookies.actor) return cookies.actor;
    const header = req.headers['x-datahub-actor'];
    if (typeof header === 'string' && header) return header;
    const auth = req.headers.authorization;
    if (auth?.startsWith('Bearer ')) {
        const actor = actorFromDataHubToken(auth.slice('Bearer '.length).trim());
        if (actor) return actor;
    }
    return 'urn:li:corpuser:datahub';
}

function proxyToInstanceGms(
    stack: LiteStack,
    req: IncomingMessage,
    res: ServerResponse,
    flags: Record<string, boolean | string>,
): void {
    const target = new URL(stackTargetFor(stack, instanceForRequest(req)));
    const path = (req.url || '/')
        .replace(/^\/api\/v2\/graphql(?=[/?]|$)/, '/api/graphql')
        .replace(/^\/api\/gms(?=\/)/, '');
    const headers: OutgoingHttpHeaders = { ...req.headers, host: target.host };
    const session = parseCookies(req.headers.cookie).PLAY_SESSION;
    if (stack.auth) {
        // A browser session takes precedence over caller auth, matching Play.
        if (session?.startsWith('eyJ')) headers.authorization = `Bearer ${session}`;
    } else {
        headers['x-datahub-actor'] = actorForRequest(req);
    }
    const upstream = httpRequest(
        { hostname: target.hostname, port: target.port, method: req.method, path, headers },
        (proxyRes) => {
            if (Object.keys(flags).length && path.startsWith('/api/graphql')) {
                const chunks: Buffer[] = [];
                proxyRes.on('data', (chunk: Buffer) => chunks.push(chunk));
                proxyRes.on('end', async () => {
                    const original = Buffer.concat(chunks);
                    try {
                        const encoding = proxyRes.headers['content-encoding'];
                        let body = original;
                        if (encoding === 'gzip') body = await promisify(gunzip)(original);
                        else if (encoding === 'br') body = await promisify(brotliDecompress)(original);
                        const patched = Buffer.from(
                            JSON.stringify(applyFlagOverrides(JSON.parse(body.toString('utf-8')), flags)),
                        );
                        const responseHeaders = { ...proxyRes.headers };
                        delete responseHeaders['content-encoding'];
                        delete responseHeaders['transfer-encoding'];
                        responseHeaders['content-length'] = String(patched.length);
                        res.writeHead(proxyRes.statusCode ?? 502, responseHeaders).end(patched);
                    } catch {
                        res.writeHead(proxyRes.statusCode ?? 502, proxyRes.headers).end(original);
                    }
                });
            } else {
                res.writeHead(proxyRes.statusCode ?? 502, proxyRes.headers);
                proxyRes.pipe(res);
            }
        },
    );
    upstream.on('error', (error: Error) => {
        if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'text/plain' });
        res.end(`Lite proxy error: ${error.message}`);
    });
    req.on('aborted', () => upstream.destroy());
    req.pipe(upstream);
}

function stackUserDirectory(stack: LiteStack): UserDirectory {
    const adminActor = 'urn:li:corpuser:datahub';
    const targetFor = (req: import('http').IncomingMessage) => stackTargetFor(stack, instanceForRequest(req));

    const proposal = (actorUrn: string, aspectName: string, value: Record<string, unknown>) => ({
        entityType: 'corpuser',
        entityUrn: actorUrn,
        changeType: 'UPSERT',
        aspectName,
        aspect: { contentType: 'application/json', value: JSON.stringify(value) },
    });

    const ingestAs = async (target: string, body: string): Promise<void> => {
        const resp = await fetch(`${target}/aspects?action=ingestProposalBatch`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-DataHub-Actor': adminActor },
            body,
        });
        if (resp.ok) return;
        const detail = `${resp.status} as ${adminActor}: ${(await resp.text()).slice(0, 160)}`;
        throw new Error(`ingestProposalBatch corpuser: ${detail}`);
    };

    return {
        async exists(req, actorUrn) {
            const resp = await fetch(
                `${targetFor(req)}/aspects/${encodeURIComponent(actorUrn)}?aspect=corpUserInfo&version=0`,
                { headers: { 'X-DataHub-Actor': adminActor } },
            );
            if (resp.ok) return true;
            if (resp.status === 404) return false;
            throw new Error(`corpUserInfo lookup for ${actorUrn}: ${resp.status}`);
        },
        async create(req, actorUrn, details) {
            const now = Date.now();
            const proposals = [
                proposal(actorUrn, 'corpUserInfo', {
                    active: true,
                    displayName: details.fullName,
                    fullName: details.fullName,
                    email: details.email,
                    ...(details.title ? { title: details.title } : {}),
                }),
                proposal(actorUrn, 'corpUserStatus', {
                    status: 'ACTIVE',
                    lastModified: { time: now, actor: adminActor },
                }),
            ];
            await ingestAs(targetFor(req), JSON.stringify({ proposals }));
        },
    };
}

function gmsAuthFor(stack: LiteStack): GmsAuth | undefined {
    if (!stack.auth || !stack.systemClientId || !stack.systemClientSecret) return undefined;
    const userPropsPath = join(findRepoRoot(process.cwd()), 'datahub-frontend/conf/user.props');
    const userProps: Record<string, string> = {};
    readFileSync(userPropsPath, 'utf-8')
        .split('\n')
        .forEach((line) => {
            const trimmed = line.trim();
            if (!trimmed || trimmed.startsWith('#')) return;
            const sep = trimmed.indexOf(':');
            if (sep > 0) userProps[trimmed.slice(0, sep)] = trimmed.slice(sep + 1);
        });
    return {
        gmsUrl: stackTargetFor(stack, undefined),
        systemClientId: stack.systemClientId,
        systemClientSecret: stack.systemClientSecret,
        userProps,
    };
}

function serveSubstitutedIndexHtml(server: PreviewServer): void {
    const outDir = resolve(server.config.root, server.config.build.outDir);
    const indexPath = join(outDir, 'index.html');
    if (!existsSync(indexPath)) throw new Error(`Missing ${indexPath}; build the frontend before preview`);
    const html = readFileSync(indexPath, 'utf-8').replace('@basePath', '/').replace('@sentryConfig', 'null');
    server.middlewares.use((req, res, next) => {
        const url = (req.url || '/').split('?')?.[0] || '/';
        const wantsHtml = (req.headers.accept || '').includes('text/html');
        const isDistFile = url !== '/' && url !== '/index.html' && existsSync(join(outDir, decodeURIComponent(url)));
        if (req.method !== 'GET' || !wantsHtml || isDistFile) {
            next();
            return;
        }
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.setHeader('Cache-Control', 'no-store');
        res.end(html);
    });
}

export function liteModePlugin(): Plugin {
    if (process.env.PREVIEW_MODE !== 'lite') return { name: 'datahub-lite-mode' };
    const stack = readLiteStack();
    const flags = parseFlagOverrides(process.env.FEATURE_FLAG_OVERRIDES || '');
    const gmsAuth = gmsAuthFor(stack);
    const register = (middlewares: Connect.Server): void => {
        middlewares.use('/health', (_req, res) => {
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ status: 'ok', mode: 'gms-lite-stack', bootId: stack.bootId }));
        });
        // Register before Vite's transforms, which otherwise interpret API paths as modules.
        middlewares.use((req, res, next) => {
            if (
                (req.method === 'GET' && (req.headers.accept || '').includes('text/html')) ||
                !isGmsPath(req.url || '')
            ) {
                next();
                return;
            }
            proxyToInstanceGms(stack, req, res, flags);
        });
        registerAuthStubs(middlewares, stackUserDirectory(stack), gmsAuth);
    };
    return {
        name: 'datahub-lite-mode',
        configureServer(server) {
            register(server.middlewares);
        },
        configurePreviewServer(server) {
            register(server.middlewares);
            serveSubstitutedIndexHtml(server);
        },
    };
}
