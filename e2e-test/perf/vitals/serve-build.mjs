/**
 * Static server for one production frontend build (a `vite build` output dir),
 * with a reverse proxy so all API/auth traffic reaches the ONE running DataHub
 * stack. This is what makes the serial A/B possible: two of these servers (one
 * per build) share a single backend, so the only thing that differs between the
 * two measured origins is the frontend bundle itself.
 *
 * Why a custom server instead of the datahub-frontend Play server:
 *   Swapping the Play server's baked-in assets would require rebuilding a
 *   container between every alternating run. Serving each dist/ on its own port
 *   and proxying API calls to the shared backend lets us flip between builds
 *   instantly, which is a hard requirement for A,B,A,B alternation.
 *
 * The production index.html ships with @basePath / @sentryConfig placeholders
 * that the Play server normally substitutes at serve time (see
 * datahub-web-react/vite.config.ts). We substitute them here the same way the
 * dev server does, otherwise the app boots with a broken <base> tag.
 */
import * as fs from 'fs';
import * as http from 'http';
import * as https from 'https';
import * as path from 'path';
import * as zlib from 'zlib';
import { URL } from 'url';

// Compressible types the production Play server serves gzipped. We must gzip too
// so Lighthouse/Lantern model the *gzip* transfer size on the wire — the whole
// point of a bundle-size PR is its gzip bytes, not the raw bytes.
const COMPRESSIBLE = new Set(['.js', '.mjs', '.css', '.html', '.json', '.svg', '.map', '.wasm']);

function clientAcceptsGzip(req) {
    return /\bgzip\b/.test(req.headers['accept-encoding'] || '');
}

// Prefixes the built app calls at runtime. Kept in sync with the `proxyOptions`
// map in datahub-web-react/vite.config.ts, plus Play's `/config` (appVersion).
const API_PREFIXES = ['/api', '/openapi', '/integrations', '/mfe/config', '/config'];
const AUTH_PREFIXES = ['/logIn', '/authenticate'];
const PROXY_PREFIXES = [...AUTH_PREFIXES, ...API_PREFIXES];

// Unauthenticated SPA routes do a real navigation to Play's /authenticate, which
// (JAAS/native auth) 302s to the client-side /login route. Stub mode must issue
// that redirect, or the SPA bounces on /authenticate forever and never paints.
const SPA_LOGIN_ROUTE = '/login';

export function loopbackOrigin(port) {
    return `http://127.0.0.1:${port}`;
}

const STUB_BACKENDS = new Set(['', 'stub', 'none', 'anonymous']);

export function shouldStubBackend(targetBase) {
    if (targetBase == null) return true;
    return STUB_BACKENDS.has(String(targetBase).trim().toLowerCase());
}

const CONTENT_TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.ttf': 'font/ttf',
    '.map': 'application/json; charset=utf-8',
    '.wasm': 'application/wasm',
};

function contentTypeFor(filePath) {
    return CONTENT_TYPES[path.extname(filePath).toLowerCase()] || 'application/octet-stream';
}

// Entity URNs in client routes contain dots (`my_db.my_schema.events`).
// path.extname treats that as a file extension and would 404 the SPA document.
const STATIC_EXTENSIONS = new Set(Object.keys(CONTENT_TYPES));

export function isSpaRoute(pathname) {
    const ext = path.extname(pathname).toLowerCase();
    return ext === '' || !STATIC_EXTENSIONS.has(ext);
}

function matchesPrefix(pathname, prefixes) {
    return prefixes.some((p) => pathname === p || pathname.startsWith(`${p}/`) || pathname.startsWith(`${p}?`));
}

function isProxied(pathname) {
    return matchesPrefix(pathname, PROXY_PREFIXES);
}

function isAuthenticatePath(pathname) {
    return pathname === '/authenticate' || pathname.startsWith('/authenticate?');
}

function renderIndexHtml(distDir) {
    const raw = fs.readFileSync(path.join(distDir, 'index.html'), 'utf-8');
    // Match the dev server's substitution: serve at root, no Sentry config.
    return raw.replace('@basePath', '/').replace('@sentryConfig', 'null');
}

function stubBackendResponse(req, res) {
    const { pathname } = new URL(req.url, 'http://127.0.0.1');
    if (pathname === '/config' || pathname === '/mfe/config') {
        const body = JSON.stringify({ config: { appVersion: 'boot-only' }, appVersion: 'boot-only' });
        res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
        res.end(body);
        return;
    }
    // Fast 404 so Lighthouse does not wait on a connection timeout to a missing GMS.
    res.writeHead(404, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ errors: [{ message: 'boot-only: no backend' }] }));
}

function proxyRequest(req, res, targetBase) {
    const target = new URL(req.url, targetBase);
    const headers = { ...req.headers, host: target.host };
    delete headers.connection;
    const transport = target.protocol === 'https:' ? https : http;
    const defaultPort = target.protocol === 'https:' ? 443 : 80;
    const upstream = transport.request(
        {
            protocol: target.protocol,
            hostname: target.hostname,
            port: target.port || defaultPort,
            method: req.method,
            path: target.pathname + target.search,
            headers,
        },
        (upstreamRes) => {
            res.writeHead(upstreamRes.statusCode || 502, upstreamRes.headers);
            upstreamRes.pipe(res);
        },
    );
    upstream.on('error', (err) => {
        if (!res.headersSent) res.writeHead(502, { 'content-type': 'text/plain' });
        res.end(`web-vitals proxy error: ${err.message}`);
    });
    req.pipe(upstream);
}

/**
 * Start a static+proxy server for a single build.
 * @param {{distDir: string, port: number, targetBase: string, label?: string}} opts
 * @returns {Promise<{server: http.Server, port: number, close: () => Promise<void>}>}
 */
export function startServer({ distDir, port, targetBase, label, stubApi }) {
    const resolvedDist = path.resolve(distDir);
    if (!fs.existsSync(path.join(resolvedDist, 'index.html'))) {
        throw new Error(`No index.html under dist dir: ${resolvedDist}`);
    }
    const stub = stubApi === true || shouldStubBackend(targetBase);
    const indexHtml = renderIndexHtml(resolvedDist);

    const sendHtml = (req, res) => {
        const headers = { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' };
        if (clientAcceptsGzip(req)) {
            headers['content-encoding'] = 'gzip';
            res.writeHead(200, headers);
            res.end(zlib.gzipSync(indexHtml));
        } else {
            res.writeHead(200, headers);
            res.end(indexHtml);
        }
    };

    const sendFile = (req, res, filePath) => {
        const type = contentTypeFor(filePath);
        // no-store keeps every run cold regardless of context reuse.
        const headers = { 'content-type': type, 'cache-control': 'no-store' };
        const ext = path.extname(filePath).toLowerCase();
        if (clientAcceptsGzip(req) && COMPRESSIBLE.has(ext)) {
            headers['content-encoding'] = 'gzip';
            res.writeHead(200, headers);
            fs.createReadStream(filePath).pipe(zlib.createGzip()).pipe(res);
        } else {
            res.writeHead(200, headers);
            fs.createReadStream(filePath).pipe(res);
        }
    };

    const server = http.createServer((req, res) => {
        const { pathname } = new URL(req.url, `http://localhost:${port}`);

        if (isProxied(pathname)) {
            const method = (req.method || 'GET').toUpperCase();
            if (stub && isAuthenticatePath(pathname) && (method === 'GET' || method === 'HEAD')) {
                const redirectUri = new URL(req.url, `http://localhost:${port}`).searchParams.get('redirect_uri');
                const location = redirectUri
                    ? `${SPA_LOGIN_ROUTE}?redirect_uri=${encodeURIComponent(redirectUri)}`
                    : SPA_LOGIN_ROUTE;
                res.writeHead(302, { location, 'cache-control': 'no-store' });
                res.end();
                return;
            }
            if (stub) stubBackendResponse(req, res);
            else proxyRequest(req, res, targetBase);
            return;
        }

        // Root or extension-less path → SPA entry point.
        if (pathname === '/' || pathname === '') {
            sendHtml(req, res);
            return;
        }

        const safePath = path.normalize(pathname).replace(/^(\.\.[/\\])+/, '');
        const filePath = path.join(resolvedDist, safePath);
        if (!filePath.startsWith(resolvedDist)) {
            res.writeHead(403);
            res.end('forbidden');
            return;
        }

        fs.stat(filePath, (err, stat) => {
            if (!err && stat.isFile()) {
                sendFile(req, res, filePath);
                return;
            }
            // Unknown non-asset path → SPA client-side route, serve index.
            if (isSpaRoute(pathname)) {
                sendHtml(req, res);
                return;
            }
            res.writeHead(404);
            res.end('not found');
        });
    });

    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, '127.0.0.1', () => {
            resolve({
                server,
                port,
                label,
                close: () =>
                    new Promise((r) => {
                        server.close(() => r());
                    }),
            });
        });
    });
}

function parseArgs(argv) {
    const args = {};
    for (let i = 0; i < argv.length; i += 1) {
        const a = argv[i];
        if (a.startsWith('--')) {
            const key = a.slice(2);
            const val = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[(i += 1)] : 'true';
            args[key] = val;
        }
    }
    return args;
}

// CLI: node serve-build.mjs --dist ./distA --port 8181 --target http://localhost:9002
if (import.meta.url === `file://${process.argv[1]}`) {
    const args = parseArgs(process.argv.slice(2));
    const dist = args.dist;
    const port = Number(args.port || 8181);
    const target = args.target || 'http://localhost:9002';
    if (!dist) {
        process.stderr.write('usage: node serve-build.mjs --dist <dir> --port <n> --target <url>\n');
        process.exit(2);
    }
    startServer({ distDir: dist, port, targetBase: target, label: args.label })
        .then((s) => {
            process.stdout.write(`serving ${dist} on http://localhost:${s.port} → proxy ${target}\n`);
        })
        .catch((err) => {
            process.stderr.write(`failed to start server: ${err.message}\n`);
            process.exit(1);
        });
}
