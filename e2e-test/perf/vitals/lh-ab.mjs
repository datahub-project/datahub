/**
 * Lighthouse web-vitals A/B runner.
 *
 * Runs Lighthouse against TWO production frontend builds served by ONE running
 * DataHub backend, and reports a pre/post comparison (FCP, LCP, CLS, TBT, TTI,
 * transferred bytes, plus the LCP element). Built to rank/gate frontend perf
 * PRs — NOT to forecast fleet impact.
 *
 * Why Lighthouse instead of a bespoke CDP collector: Lighthouse's simulated
 * (Lantern) throttling MODELS critical-path download time from a single trace
 * rather than measuring wall-clock under real-time host contention. On a loaded
 * CI box that is dramatically lower variance, and it is exactly the right model
 * for bundle-size changes (a −909 KB eager bundle shows up as modeled download
 * time on the pre-paint path). It also gives TBT/TTI and a battle-tested
 * `largest-contentful-paint-element` audit for free.
 *
 * Non-negotiable methodology (see README.md):
 *   - ALTERNATING A,B,A,B. Batching lets background drift land on whichever
 *     build ran second and fake a delta.
 *   - Fresh incognito context + injected auth cookies per run. This is the ONE
 *     detail that makes the known-answer test work: it gives a cold cache (so
 *     Lantern sees the eager bundle's bytes) AND a valid session at the same
 *     time. The fresh context provides cache isolation; `disableStorageReset`
 *     avoids mutating the prepared browser state before navigation.
 *   - n >= 5 measured runs per build, 2 warm-ups discarded; median + p95 + every
 *     raw run.
 *   - Throttling method + settings recorded in the output. Lantern default,
 *     --throttling-method=devtools for a spot-check.
 *   - No projected fleet number.
 *
 * Usage:
 *   node lh-ab.mjs --config config.json [--out results/report.json]
 *                  [--throttling-method simulate|devtools]
 *   # live SaaS / shared env (token from the environment, never committed):
 *   DATAHUB_GMS_TOKEN=… node lh-ab.mjs --config config.remote.json \
 *       --origin https://preview.example.com
 *   # CI boot-only (no GMS): two local dist/ trees, unauthenticated /login
 *   node lh-ab.mjs --config config.ci.json --no-auth --md-out results/report.md
 */
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import lighthouse from 'lighthouse';
import puppeteer from 'puppeteer';
import {
    actorUrnFromToken,
    buildSchedule,
    cookiesForTokenAuth,
    extraHeadersForToken,
    isAnonymousMode,
    isAuthRejection,
    isSameOrigin,
    resolveAccessToken,
    resolveTargets,
    sessionBootstrapSource,
    toPuppeteerCookie,
} from './auth.mjs';
import { resourceCategoryTypes } from './resource-summary.mjs';
import {
    METRIC_KEYS,
    aggregate,
    extractMetrics,
    medianBenchmarkIndex,
    reportRunner,
    selectReportNote,
} from './report.mjs';
import { measuredSampleNote } from './sample-size.mjs';
import { loopbackOrigin, shouldStubBackend, startServer } from './serve-build.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
    const args = {};
    for (let i = 0; i < argv.length; i += 1) {
        if (argv[i].startsWith('--')) {
            const key = argv[i].slice(2);
            const val = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[(i += 1)] : 'true';
            args[key] = val;
        }
    }
    return args;
}

async function fetchAppVersion(origin, extraHTTPHeaders = {}) {
    const url = `${origin.replace(/\/+$/, '')}/config`;
    try {
        const resp = await fetch(url, { headers: { ...extraHTTPHeaders } });
        if (!resp.ok) return null;
        const body = await resp.json();
        return body?.config?.appVersion || body?.appVersion || null;
    } catch {
        return null;
    }
}

async function probeGraphql(origin, token) {
    const base = origin.replace(/\/+$/, '');
    const paths = ['/api/graphql', '/api/v2/graphql'];
    let lastErr = null;
    for (const p of paths) {
        const url = `${base}${p}`;
        try {
            const resp = await fetch(url, {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    authorization: `Bearer ${token}`,
                },
                body: JSON.stringify({ query: '{ me { corpUser { urn } } }' }),
            });
            if (!resp.ok) {
                const err = new Error(`token probe against ${url} failed: ${resp.status} ${await resp.text()}`);
                err.status = resp.status;
                if (isAuthRejection(err)) throw err;
                lastErr = err;
                continue;
            }
            const body = await resp.json();
            const urn = body?.data?.me?.corpUser?.urn;
            if (!urn) {
                lastErr = new Error(`token probe against ${url} did not return me.corpUser.urn`);
                continue;
            }
            process.stdout.write(`token accepted by ${origin} as ${urn} via ${p}\n`);
            return urn;
        } catch (e) {
            lastErr = e;
        }
    }
    throw lastErr || new Error(`token probe against ${base} failed on ${paths.join(', ')}`);
}

async function resolveAuth({ cfg, args, cookieOrigin, graphqlOrigin }) {
    const authCfg = cfg.auth || {};
    if (isAnonymousMode({ args, cfg })) {
        process.stdout.write('anonymous mode: measuring the unauthenticated boot path (/login)\n');
        return { cookies: [], extraHTTPHeaders: {}, sessionBootstrap: null, actorUrn: null, tokenAuth: false };
    }
    const token = resolveAccessToken({ args, cfg, env: process.env });
    if (token) {
        process.stdout.write(`using access token from env (never logged)\n`);
        let actorUrn = authCfg.actorUrn || null;
        let authVerified = true;
        let authWarning = null;
        if (graphqlOrigin) {
            try {
                actorUrn = await probeGraphql(graphqlOrigin, token);
            } catch (e) {
                if (isAuthRejection(e)) {
                    throw new Error(
                        `Access token was rejected by ${graphqlOrigin} (${e.status}). ` +
                            'Refusing to measure an unauthenticated page.',
                    );
                }
                actorUrn = actorUrn || actorUrnFromToken(token, authCfg.actorUrn);
                authVerified = false;
                authWarning = e.message;
                process.stdout.write(`token GraphQL probe failed (${e.message}); actor URN was not verified\n`);
            }
        } else {
            actorUrn = actorUrnFromToken(token, authCfg.actorUrn);
            authVerified = false;
            authWarning = 'No GraphQL origin to verify the access token.';
        }
        return {
            cookies: cookiesForTokenAuth({ origin: cookieOrigin, actorUrn }),
            extraHTTPHeaders: extraHeadersForToken(token),
            sessionBootstrap: sessionBootstrapSource(actorUrn),
            actorUrn,
            tokenAuth: true,
            authVerified,
            authWarning,
        };
    }

    const fallbackDomain = new URL(cookieOrigin).hostname;
    const ssPath = authCfg.storageStatePath ? path.resolve(__dirname, authCfg.storageStatePath) : null;
    if (ssPath && fs.existsSync(ssPath)) {
        try {
            const state = JSON.parse(fs.readFileSync(ssPath, 'utf-8'));
            const cookies = (state.cookies || []).filter((c) => c.name);
            if (cookies.some((c) => c.name === 'actor')) {
                process.stdout.write(`reusing auth cookies from storageState: ${ssPath}\n`);
                return {
                    cookies: cookies.map((c) => toPuppeteerCookie(c, fallbackDomain)),
                    extraHTTPHeaders: {},
                    sessionBootstrap: null,
                    actorUrn: cookies.find((c) => c.name === 'actor').value,
                    tokenAuth: false,
                    authVerified: true,
                };
            }
        } catch {
            // fall through to /logIn
        }
    }

    const loginBase = cfg.backendUrl || cookieOrigin;
    process.stdout.write(`minting auth via /logIn against ${loginBase}\n`);
    const resp = await fetch(`${loginBase}/logIn`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ username: authCfg.username, password: authCfg.password }),
    });
    if (!resp.ok) {
        throw new Error(
            `login failed: ${resp.status} ${await resp.text()}. ` +
                'SSO instances need DATAHUB_GMS_TOKEN (or DATAHUB_ACCESS_TOKEN) instead of /logIn.',
        );
    }
    const setCookies = typeof resp.headers.getSetCookie === 'function' ? resp.headers.getSetCookie() : [];
    if (setCookies.length === 0) {
        throw new Error('login returned no Set-Cookie headers');
    }
    return {
        cookies: setCookies
            .map((raw) => {
                const [pair] = raw.split(';');
                const idx = pair.indexOf('=');
                return { name: pair.slice(0, idx).trim(), value: pair.slice(idx + 1).trim() };
            })
            .filter((c) => c.name)
            .map((c) => toPuppeteerCookie(c, fallbackDomain)),
        extraHTTPHeaders: {},
        sessionBootstrap: null,
        actorUrn: null,
        tokenAuth: false,
        authVerified: true,
    };
}

function buildThrottling(cfg, methodOverride) {
    const t = cfg.throttling || {};
    const method = methodOverride || t.method || 'simulate';
    // Full throttling block; Lighthouse uses rttMs/throughputKbps for 'simulate'
    // and requestLatencyMs/download+uploadThroughputKbps for 'devtools'.
    // Desktop broadband profile by default (10 Mbps / 40 ms), with a 4x CPU
    // slowdown to surface the parse/execute cost of extra JS. Realistic for a
    // desktop app while still cleanly exposing a bundle-size change on both the
    // download (network) and parse (CPU) axes.
    const throttling = {
        rttMs: t.rttMs ?? 40,
        throughputKbps: t.throughputKbps ?? 10240,
        cpuSlowdownMultiplier: t.cpuSlowdownMultiplier ?? 4,
        requestLatencyMs: t.requestLatencyMs ?? 150,
        downloadThroughputKbps: t.downloadThroughputKbps ?? t.throughputKbps ?? 10240,
        uploadThroughputKbps: t.uploadThroughputKbps ?? 10240,
    };
    return { method, throttling };
}

async function runOnce({ browser, cookies, extraHTTPHeaders, sessionBootstrap, origin, route, flags }) {
    // Fresh incognito context → cold cache. Inject cookies → authenticated.
    const context = await browser.createBrowserContext();
    let metrics = null;
    let error = null;
    let lighthouseVersion = null;
    try {
        const page = await context.newPage();
        if (sessionBootstrap) {
            await page.evaluateOnNewDocument(sessionBootstrap);
        }
        if (extraHTTPHeaders && Object.keys(extraHTTPHeaders).length > 0) {
            await page.setRequestInterception(true);
            page.on('request', (request) => {
                const headers = isSameOrigin(request.url(), origin)
                    ? { ...request.headers(), ...extraHTTPHeaders }
                    : request.headers();
                request.continue({ headers }).catch(() => {});
            });
        }
        if (cookies.length) await page.setCookie(...cookies);
        const url = `${origin}${route.path}`;
        const runnerResult = await lighthouse(url, { ...flags }, undefined, page);
        if (!runnerResult || !runnerResult.lhr) throw new Error('lighthouse returned no result');
        const { lhr } = runnerResult;
        lighthouseVersion = lhr.lighthouseVersion;
        const runtimeError = lhr.runtimeError && lhr.runtimeError.code !== 'NO_ERROR' ? lhr.runtimeError : null;
        if (runtimeError) throw new Error(`${runtimeError.code}: ${runtimeError.message}`);
        metrics = extractMetrics(lhr);
    } catch (e) {
        error = String(e && e.message ? e.message : e);
    } finally {
        await context.close();
    }
    return { metrics, error, lighthouseVersion };
}

function fmt(v, key) {
    if (v == null) return '—';
    if (key === 'transferredBytes') return `${(v / 1024).toFixed(1)} KB`;
    if (key === 'cls') return v.toFixed(4);
    return `${Math.round(v)} ms`;
}

function fmtBytes(value) {
    return value == null ? '—' : `${(value / 1024).toFixed(1)} KB`;
}

function appendResourceSummary(lines, data, isAb) {
    lines.push('Resource transfer by type (p50):');
    lines.push('');
    if (isAb) {
        lines.push('| type | A bytes | B bytes | Δ bytes | A requests | B requests |');
        lines.push('| --- | ---: | ---: | ---: | ---: | ---: |');
        for (const type of resourceCategoryTypes()) {
            const a = data.A.resourceSummary?.[type];
            const b = data.B.resourceSummary?.[type];
            if (!a && !b) continue;
            const aBytes = a?.transferSize.p50 ?? null;
            const bBytes = b?.transferSize.p50 ?? null;
            const delta = aBytes != null && bBytes != null ? bBytes - aBytes : null;
            lines.push(
                `| ${type} | ${fmtBytes(aBytes)} | ${fmtBytes(bBytes)} | ${fmtBytes(delta)} | ` +
                    `${a?.requestCount.p50 ?? '—'} | ${b?.requestCount.p50 ?? '—'} |`,
            );
        }
    } else {
        const summary = data.A.resourceSummary;
        const total = summary?.total?.transferSize.p50 ?? null;
        lines.push('| type | bytes | % total | requests |');
        lines.push('| --- | ---: | ---: | ---: |');
        for (const type of resourceCategoryTypes()) {
            const item = summary?.[type];
            if (!item) continue;
            const bytes = item.transferSize.p50;
            const percent = total ? `${((bytes / total) * 100).toFixed(1)}%` : '—';
            lines.push(`| ${type} | ${fmtBytes(bytes)} | ${percent} | ${item.requestCount.p50} |`);
        }
    }
    lines.push('');
}

function toMarkdown(report) {
    const lines = [];
    const { versions } = report;
    const builds = Object.keys(versions);
    const isAb = builds.includes('A') && builds.includes('B');
    if (isAb) {
        lines.push(`### Web-vitals A/B — ${versions.A.label} (A) vs ${versions.B.label} (B)`);
    } else {
        lines.push(`### Web-vitals — ${versions.A.label}`);
    }
    lines.push('');
    if (report.authVerified === false) {
        lines.push(
            '**Auth was not verified.** The access token was not confirmed by the server, so there is no comparison table.',
        );
        if (report.authWarning) {
            lines.push('');
            lines.push(report.authWarning);
        }
        return lines.join('\n');
    }
    const th = report.config.throttling;
    lines.push(
        `Lighthouse ${report.lighthouseVersion || '?'}, throttling **${th.method}** ` +
            `(cpu ${th.throttling.cpuSlowdownMultiplier}x, rtt ${th.throttling.rttMs} ms, ` +
            `${th.throttling.throughputKbps} kbps). Form factor: ${report.config.formFactor}. ` +
            `Cache: ${report.config.cachePolicy}. target n=${report.config.runsPerBuild}/build, ` +
            `${report.config.warmupRunsPerBuild} warm-up discarded` +
            (isAb ? ', alternating A,B.' : '.'),
    );
    if (report.mode !== 'remote' && report.authMode === 'anonymous') {
        lines.push('');
        lines.push(
            '**Boot-only (no GMS).** Unauthenticated login route — eager JS/CSS/font transfer and parse, not search/dataset payload. Do not treat as a fleet p75.',
        );
    }
    const failedRuns = (report.raw_runs || []).filter((r) => r.error);
    if (failedRuns.length) {
        lines.push('');
        lines.push(
            `**${failedRuns.length}/${(report.raw_runs || []).length} runs failed** (excluded from p50). Dashes are missing data, not a zero-delta.`,
        );
        for (const r of failedRuns.slice(0, 10)) {
            const tag = r.warmup ? 'warmup' : 'measured';
            lines.push(`- \`${r.route}\` ${r.build} #${r.run} (${tag}): ${r.error}`);
        }
        lines.push('');
    }
    lines.push('');
    for (const [route, data] of Object.entries(report.routes)) {
        lines.push(`#### \`${route}\``);
        lines.push('');
        const counts = {};
        for (const build of builds.filter((name) => name === 'A' || name === 'B')) {
            counts[build] = data[build]?.sampleSize ?? 0;
        }
        const sample = measuredSampleNote(counts, report.config.runsPerBuild);
        lines.push(sample.line);
        if (sample.warning) {
            lines.push('');
            lines.push(sample.warning);
        }
        lines.push('');
        if (isAb) {
            lines.push('| metric | A p50 | B p50 | Δ p50 | Δ% | A p95 | B p95 |');
            lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
            for (const key of METRIC_KEYS) {
                const c = data.comparison[key];
                const dpct = c.delta_pct == null ? '—' : `${c.delta_pct >= 0 ? '+' : ''}${c.delta_pct.toFixed(1)}%`;
                lines.push(
                    `| ${key} | ${fmt(data.A[key].p50, key)} | ${fmt(data.B[key].p50, key)} | ` +
                        `${fmt(c.delta, key)} | ${dpct} | ${fmt(data.A[key].p95, key)} | ${fmt(data.B[key].p95, key)} |`,
                );
            }
        } else {
            lines.push('| metric | p50 | p95 |');
            lines.push('| --- | ---: | ---: |');
            for (const key of METRIC_KEYS) {
                lines.push(`| ${key} | ${fmt(data.A[key].p50, key)} | ${fmt(data.A[key].p95, key)} |`);
            }
        }
        lines.push('');
        appendResourceSummary(lines, data, isAb);
        const ea = data.lcpElement.A;
        lines.push(`LCP element A: \`${ea ? ea.selector : 'n/a'}\`${ea && ea.nodeLabel ? ` — ${ea.nodeLabel}` : ''}`);
        if (isAb) {
            const eb = data.lcpElement.B;
            lines.push(
                `LCP element B: \`${eb ? eb.selector : 'n/a'}\`${eb && eb.nodeLabel ? ` — ${eb.nodeLabel}` : ''}`,
            );
        }
        lines.push('');
    }
    lines.push(
        '_These numbers rank changes and gate regressions against the measured origin(s). ' +
            'They do not sum across PRs and do not forecast any fleet-wide production number._',
    );
    return lines.join('\n');
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    const configPath = path.resolve(process.cwd(), args.config || path.join(__dirname, 'config.json'));
    const cfg = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    if (args['token-env'] && args['token-env'] !== 'true') {
        cfg.auth = { ...(cfg.auth || {}), tokenEnv: args['token-env'] };
    }

    cfg.formFactor = cfg.formFactor || 'desktop';
    cfg.cachePolicy = 'cold (fresh incognito context per run)';
    cfg.viewport = cfg.viewport || { width: 1350, height: 940 };
    const { method, throttling } = buildThrottling(cfg, args['throttling-method']);

    const screenEmulation = {
        mobile: cfg.formFactor === 'mobile',
        width: cfg.viewport.width,
        height: cfg.viewport.height,
        deviceScaleFactor: 1,
        disabled: false,
    };

    const flags = {
        logLevel: 'error',
        output: 'json',
        onlyCategories: ['performance'],
        formFactor: cfg.formFactor,
        screenEmulation,
        // Without this override Lighthouse keeps its default mobile UA even
        // when formFactor/screenEmulation are desktop.
        emulatedUserAgent: true,
        throttlingMethod: method,
        throttling,
        // Fresh context already guarantees a cold cache; keep injected cookies.
        disableStorageReset: true,
    };

    const targets = resolveTargets(cfg, args);
    const builds = Object.keys(targets.versions);
    const isAb = builds.includes('A') && builds.includes('B');

    const servers = {};
    let origins;
    const anonymous = isAnonymousMode({ args, cfg });
    const stubApi = anonymous || shouldStubBackend(cfg.backendUrl);
    if (targets.mode === 'remote') {
        origins = targets.origins;
        process.stdout.write(
            `remote ${isAb ? 'A/B' : 'single-origin'} → ${builds.map((k) => `${k}=${origins[k]}`).join('  ')}\n`,
        );
    } else {
        servers.A = await startServer({
            distDir: path.resolve(process.cwd(), cfg.builds.A.dist),
            port: cfg.builds.A.port,
            targetBase: cfg.backendUrl,
            label: cfg.builds.A.label,
            stubApi,
        });
        const served = [`A(${cfg.builds.A.label}) → :${servers.A.port}`];
        origins = { A: loopbackOrigin(servers.A.port) };
        if (targets.versions.B) {
            servers.B = await startServer({
                distDir: path.resolve(process.cwd(), cfg.builds.B.dist),
                port: cfg.builds.B.port,
                targetBase: cfg.backendUrl,
                label: cfg.builds.B.label,
                stubApi,
            });
            served.push(`B(${cfg.builds.B.label}) → :${servers.B.port}`);
            origins.B = loopbackOrigin(servers.B.port);
        }
        process.stdout.write(served.join('   ') + (stubApi ? '  (boot-only, API stubbed)\n' : '\n'));
    }

    const cookieOrigin = origins.A;
    const graphqlOrigin = targets.mode === 'remote' ? origins.A : cfg.backendUrl;
    const auth = await resolveAuth({ cfg, args, cookieOrigin, graphqlOrigin });
    const appVersion = await fetchAppVersion(origins.A, auth.extraHTTPHeaders);
    if (appVersion) process.stdout.write(`instance appVersion (release): ${appVersion}\n`);
    else process.stdout.write(`instance appVersion unavailable from /config\n`);

    const browser = await puppeteer.launch({
        headless: true,
        // Lighthouse is sensitive to --enable-automation; see lighthouse docs/puppeteer.md
        ignoreDefaultArgs: ['--enable-automation'],
        args: [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--proxy-bypass-list=<-loopback>',
        ],
    });

    const rawRuns = [];
    let lighthouseVersion = null;
    const schedule = buildSchedule(cfg.runsPerBuild, cfg.warmupRunsPerBuild, builds);

    try {
        for (const route of cfg.routes) {
            const counters = { A: 0, B: 0 };
            for (const step of schedule) {
                counters[step.build] += 1;
                const runIdx = counters[step.build];
                const { metrics, error, lighthouseVersion: v } = await runOnce({
                    browser,
                    cookies: auth.tokenAuth
                        ? cookiesForTokenAuth({ origin: origins[step.build], actorUrn: auth.actorUrn })
                        : auth.cookies,
                    extraHTTPHeaders: auth.extraHTTPHeaders,
                    sessionBootstrap: auth.sessionBootstrap,
                    origin: origins[step.build],
                    route,
                    flags,
                });
                if (v) lighthouseVersion = v;
                const tag = step.warmup ? 'warmup' : 'measured';
                process.stdout.write(
                    `[${route.name}] ${step.build} #${runIdx} (${tag}) ` +
                        (error
                            ? `ERROR ${error}\n`
                            : `FCP=${metrics.fcp != null ? Math.round(metrics.fcp) : '—'}ms ` +
                              `LCP=${metrics.lcp != null ? Math.round(metrics.lcp) : '—'}ms ` +
                              `TTFB=${metrics.ttfb != null ? Math.round(metrics.ttfb) : '—'}ms ` +
                              `TBT=${metrics.tbt != null ? Math.round(metrics.tbt) : '—'}ms ` +
                              `main=${metrics.mainThread != null ? Math.round(metrics.mainThread) : '—'}ms ` +
                              `bootup=${metrics.bootup != null ? Math.round(metrics.bootup) : '—'}ms ` +
                              `bytes=${metrics.transferredBytes != null ? (metrics.transferredBytes / 1024).toFixed(0) : '—'}KB\n`),
                );
                rawRuns.push({
                    route: route.name,
                    routePath: route.path,
                    build: step.build,
                    warmup: step.warmup,
                    run: runIdx,
                    metrics: metrics || {},
                    error,
                });
            }
        }
    } finally {
        await browser.close();
        if (servers.A) await servers.A.close();
        if (servers.B) await servers.B.close();
    }

    const report = {
        suite: 'web-vitals',
        run_id: new Date().toISOString(),
        mode: targets.mode,
        backend: targets.backend,
        release: appVersion,
        lighthouseVersion,
        versions: {
            ...targets.versions,
            ...(appVersion ? { appVersion } : {}),
        },
        config: {
            runsPerBuild: cfg.runsPerBuild,
            warmupRunsPerBuild: cfg.warmupRunsPerBuild,
            cachePolicy: cfg.cachePolicy,
            formFactor: cfg.formFactor,
            viewport: cfg.viewport,
            throttling: { method, throttling },
            ordering: isAb
                ? 'alternating A,B,A,B (batched ordering is invalid — see README)'
                : 'single origin, sequential runs',
        },
        routes: aggregate(rawRuns, builds),
        raw_runs: rawRuns.map(({ metrics, ...rest }) => ({
            ...rest,
            fcp: metrics.fcp ?? null,
            lcp: metrics.lcp ?? null,
            cls: metrics.cls ?? null,
            tbt: metrics.tbt ?? null,
            tti: metrics.tti ?? null,
            ttfb: metrics.ttfb ?? null,
            transferredBytes: metrics.transferredBytes ?? null,
            mainThread: metrics.mainThread ?? null,
            bootup: metrics.bootup ?? null,
            benchmarkIndex: metrics.benchmarkIndex ?? null,
            lcpElement: metrics.lcpElement ?? null,
            resourceSummary: metrics.resourceSummary ?? null,
        })),
        authMode: anonymous ? 'anonymous' : 'authenticated',
        authVerified: auth.authVerified !== false,
        authWarning: auth.authWarning || null,
        runner: reportRunner(),
        benchmarkIndex: null,
        note: selectReportNote({
            mode: targets.mode,
            authMode: anonymous ? 'anonymous' : 'authenticated',
            stubApi,
        }),
    };
    report.benchmarkIndex = medianBenchmarkIndex(report.raw_runs);

    const outPath = path.resolve(
        process.cwd(),
        args.out || path.join(__dirname, 'results', `web-vitals-${Date.now()}.json`),
    );
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
    const markdown = toMarkdown(report);
    process.stdout.write(`\nreport written: ${outPath}\n\n`);
    process.stdout.write(`${markdown}\n`);
    if (args['md-out'] && args['md-out'] !== 'true') {
        const mdPath = path.resolve(process.cwd(), args['md-out']);
        fs.mkdirSync(path.dirname(mdPath), { recursive: true });
        fs.writeFileSync(mdPath, markdown);
        process.stdout.write(`markdown written: ${mdPath}\n`);
    }
    const measured = rawRuns.filter((r) => !r.warmup);
    if (measured.length > 0 && measured.every((r) => r.error)) {
        process.exitCode = 1;
    }
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectRun) {
    main().catch((err) => {
        process.stderr.write(`web-vitals runner failed: ${err && err.stack ? err.stack : err}\n`);
        process.exit(1);
    });
}
