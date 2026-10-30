import { METRIC_KEYS } from './report.mjs';
import { resourceCategoryTypes } from './resource-summary.mjs';
import { measuredSampleNote } from './sample-size.mjs';
import { JUDGED_METRICS, judgeRoute } from './verdict.mjs';

export function fmt(value, key) {
    if (value == null || Number.isNaN(value)) return '—';
    if (key === 'transferredBytes') return `${(value / 1024).toFixed(1)} KB`;
    if (key === 'cls') return value.toFixed(4);
    return `${Math.round(value)} ms`;
}

function fmtBytes(value) {
    return value == null ? '—' : `${(value / 1024).toFixed(1)} KB`;
}

export function throttlingNote(throttlingConfig) {
    const method = throttlingConfig?.method;
    const settings = throttlingConfig?.throttling || {};
    const cpu = settings.cpuSlowdownMultiplier;
    const latency = method === 'devtools' ? settings.requestLatencyMs : settings.rttMs;
    const kbps = method === 'devtools' ? settings.downloadThroughputKbps : settings.throughputKbps;
    const name = method === 'devtools' ? 'devtools' : 'Lantern';
    const mbps = typeof kbps === 'number' ? `${Math.round(kbps / 1024)} Mbps` : '—';
    return `${name}, CPU ${cpu ?? '—'}×, ${latency ?? '—'} ms, ${mbps}`;
}

export function routeDisplayPath(report, routeName) {
    const configured = report?.config?.routes;
    if (Array.isArray(configured)) {
        const match = configured.find((route) => route.name === routeName);
        if (match?.path) return match.path;
    }
    const run = (report?.raw_runs || []).find((item) => item.route === routeName && item.routePath);
    if (run) return run.routePath;
    if (routeName === 'boot') return '/login';
    if (routeName === 'home') return '/';
    return routeName;
}

function isAb(report) {
    const builds = Object.keys(report?.versions || {});
    return builds.includes('A') && builds.includes('B');
}

function judgmentFor(report, data) {
    if (data?.judgment) return data.judgment;
    return judgeRoute(data, report?.config?.runsPerBuild ?? null);
}

function formatRange(key, range) {
    if (!range) return '—';
    return `${fmt(range.min, key)}–${fmt(range.max, key)}`;
}

function formatMetricClause(key, metric, { omitLabel }) {
    const name = key.toUpperCase();
    const medians = `${fmt(metric.aP50, key)} → ${fmt(metric.bP50, key)}`;
    if (metric.label === 'short' || !metric.a || !metric.b) {
        return omitLabel ? `${name} ${medians}` : `${name} short sample, ${medians}`;
    }
    const ranges = `A ${formatRange(key, metric.a)}, B ${formatRange(key, metric.b)}`;
    const pct =
        metric.deltaPct == null || metric.label === 'noise' || metric.label === 'no change'
            ? ''
            : ` (${metric.deltaPct >= 0 ? '+' : ''}${metric.deltaPct.toFixed(1)}%)`;
    const overlap = metric.label === 'noise' ? ', ranges overlap' : '';
    if (omitLabel) return `${name} ${medians}${pct} (${ranges}${overlap})`;
    return `${name} ${metric.label}, ${medians}${pct} (${ranges}${overlap})`;
}

export function formatRouteSummary(path, judgment, note) {
    if (judgment.label === 'short') {
        const target = judgment.targetN == null ? '?' : judgment.targetN;
        const medians = JUDGED_METRICS.map((key) => formatMetricClause(key, judgment.metrics[key], { omitLabel: true })).join(
            '; ',
        );
        return `\`${path}\` — short sample (A ${judgment.aN}/${target}, B ${judgment.bN}/${target}). ${medians}. _(${note})_`;
    }
    const labels = JUDGED_METRICS.map((key) => judgment.metrics[key].label);
    const shared = labels.every((label) => label === labels[0]) ? labels[0] : null;
    const omitLabel = shared != null && judgment.label !== 'mixed' && judgment.label !== 'quiet';
    const clauses = JUDGED_METRICS.map((key) =>
        formatMetricClause(key, judgment.metrics[key], { omitLabel }),
    ).join('; ');
    if (judgment.label === 'quiet') return `\`${path}\` — ${clauses}. _(${note})_`;
    return `\`${path}\` — ${judgment.label}. ${clauses}. _(${note})_`;
}

function appendResourceSummary(lines, data, ab) {
    const types = resourceCategoryTypes().filter((type) => {
        if (ab) return data.A?.resourceSummary?.[type] || data.B?.resourceSummary?.[type];
        return data.A?.resourceSummary?.[type];
    });
    if (types.length === 0) return;
    lines.push('Resource transfer by type (p50):');
    lines.push('');
    if (ab) {
        lines.push('| type | A bytes | B bytes | Δ bytes | A requests | B requests |');
        lines.push('| --- | ---: | ---: | ---: | ---: | ---: |');
        for (const type of types) {
            const a = data.A.resourceSummary?.[type];
            const b = data.B.resourceSummary?.[type];
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
        for (const type of types) {
            const item = summary?.[type];
            const bytes = item.transferSize.p50;
            const percent = total ? `${((bytes / total) * 100).toFixed(1)}%` : '—';
            lines.push(`| ${type} | ${fmtBytes(bytes)} | ${percent} | ${item.requestCount.p50} |`);
        }
    }
    lines.push('');
}

function renderMetricTable(lines, data, ab) {
    if (ab) {
        lines.push('| metric | A p50 | B p50 | Δ p50 | Δ% | A p95 | B p95 |');
        lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
        for (const key of METRIC_KEYS) {
            const comparison = data.comparison?.[key];
            const a = data.A?.[key];
            const b = data.B?.[key];
            if (!comparison || !a || !b) continue;
            const dpct =
                comparison.delta_pct == null
                    ? '—'
                    : `${comparison.delta_pct >= 0 ? '+' : ''}${comparison.delta_pct.toFixed(1)}%`;
            lines.push(
                `| ${key} | ${fmt(a.p50, key)} | ${fmt(b.p50, key)} | ${fmt(comparison.delta, key)} | ${dpct} | ` +
                    `${fmt(a.p95, key)} | ${fmt(b.p95, key)} |`,
            );
        }
        return;
    }
    lines.push('| metric | p50 | p95 |');
    lines.push('| --- | ---: | ---: |');
    for (const key of METRIC_KEYS) {
        const metric = data.A?.[key];
        if (!metric) continue;
        lines.push(`| ${key} | ${fmt(metric.p50, key)} | ${fmt(metric.p95, key)} |`);
    }
}

function renderExpandedRoute(report, routeName, data) {
    const ab = isAb(report);
    const path = routeDisplayPath(report, routeName);
    const lines = [`#### \`${path}\``, ''];
    const builds = Object.keys(report.versions || {}).filter((name) => name === 'A' || name === 'B');
    const counts = {};
    for (const build of builds) counts[build] = data[build]?.sampleSize ?? 0;
    const sample = measuredSampleNote(counts, report.config?.runsPerBuild);
    lines.push(sample.line);
    if (sample.warning) {
        lines.push('');
        lines.push(sample.warning);
    }
    lines.push('');
    renderMetricTable(lines, data, ab);
    lines.push('');
    appendResourceSummary(lines, data, ab);
    const elementA = data.lcpElement?.A;
    lines.push(
        `LCP element A: \`${elementA ? elementA.selector : 'n/a'}\`${elementA && elementA.nodeLabel ? ` — ${elementA.nodeLabel}` : ''}`,
    );
    if (ab) {
        const elementB = data.lcpElement?.B;
        lines.push(
            `LCP element B: \`${elementB ? elementB.selector : 'n/a'}\`${elementB && elementB.nodeLabel ? ` — ${elementB.nodeLabel}` : ''}`,
        );
    }
    lines.push('');
    return lines;
}

export function renderRouteBlocks(report) {
    const ab = isAb(report);
    const note = throttlingNote(report?.config?.throttling);
    const blocks = [];
    for (const [routeName, data] of Object.entries(report?.routes || {})) {
        if (!ab) {
            blocks.push({ expand: true, lines: renderExpandedRoute(report, routeName, data) });
            continue;
        }
        const judgment = judgmentFor(report, data);
        const summary = formatRouteSummary(routeDisplayPath(report, routeName), judgment, note);
        if (!judgment.expand) {
            blocks.push({ expand: false, lines: [summary, ''] });
            continue;
        }
        blocks.push({ expand: true, lines: [summary, '', ...renderExpandedRoute(report, routeName, data)] });
    }
    return blocks;
}

function failedRunLines(report) {
    const failed = (report?.raw_runs || []).filter((run) => run.error);
    if (failed.length === 0) return [];
    const lines = [
        '',
        `**${failed.length}/${(report.raw_runs || []).length} runs failed** (excluded from the median). Dashes are missing data.`,
    ];
    for (const run of failed.slice(0, 10)) {
        const tag = run.warmup ? 'warmup' : 'measured';
        lines.push(`- \`${run.route}\` ${run.build} #${run.run} (${tag}): ${run.error}`);
    }
    lines.push('');
    return lines;
}

function reportHeading(report) {
    const versions = report.versions || {};
    const builds = Object.keys(versions);
    const ab = builds.includes('A') && builds.includes('B');
    if (ab) return `### Web-vitals A/B — ${versions.A.label} (A) vs ${versions.B.label} (B)`;
    return `### Web-vitals — ${versions.A?.label || 'build'}`;
}

function methodParagraph(report) {
    const throttling = report.config?.throttling;
    const settings = throttling?.throttling || {};
    const ab = isAb(report);
    return (
        `Lighthouse ${report.lighthouseVersion || '?'}, throttling **${throttling?.method || '?'}** ` +
        `(cpu ${settings.cpuSlowdownMultiplier}x, rtt ${settings.rttMs} ms, ` +
        `${settings.throughputKbps} kbps). Form factor: ${report.config?.formFactor}. ` +
        `Cache: ${report.config?.cachePolicy}. target n=${report.config?.runsPerBuild}/build, ` +
        `${report.config?.warmupRunsPerBuild} warm-up discarded` +
        (ab ? ', alternating A,B.' : '.')
    );
}

export function renderReport(report) {
    const lines = [reportHeading(report), ''];
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
    const blocks = renderRouteBlocks(report);
    const anyExpanded = blocks.some((block) => block.expand);
    if (anyExpanded || !isAb(report)) {
        lines.push(methodParagraph(report));
        if (report.mode !== 'remote' && report.authMode === 'anonymous') {
            lines.push('');
            lines.push(
                '**Boot-only (no GMS).** Unauthenticated login route — eager JS/CSS/font transfer and parse, not search/dataset payload. Do not treat as a fleet p75.',
            );
        }
        if (report.mode !== 'remote' && report.authMode === 'stub') {
            lines.push('');
            lines.push(
                '**Signed-in shell (no GMS).** Recorded GraphQL, not a live search or dataset payload. Do not treat as a fleet p75.',
            );
        }
        lines.push(...failedRunLines(report));
        lines.push('');
    } else if ((report.raw_runs || []).some((run) => run.error)) {
        lines.push(...failedRunLines(report));
    }
    for (const block of blocks) lines.push(...block.lines);
    if (anyExpanded || !isAb(report)) {
        lines.push(
            '_These numbers rank changes against the measured origin(s). They do not sum across PRs and do not forecast a fleet p75._',
        );
    }
    return lines.join('\n');
}

export const BOOT_COMMENT_INTRO =
    'No GMS. `/login` is the logged-out page on Lantern at 1× CPU. `/` is the signed-in shell with recorded GraphQL, devtools, CPU 4×. ' +
    'First paint and LCP are judged separately. Noise means the A and B ranges overlap. ' +
    'No change means a separated median under 5% and under 50 ms. ' +
    'Mixed means those two metrics moved in opposite directions. Lab deltas do not forecast a fleet p75.';

export function renderBootComment({ login, home, loginError, homeError }) {
    const blocks = [];
    if (login) blocks.push(...renderRouteBlocks(login));
    if (home) blocks.push(...renderRouteBlocks(home));
    const anyExpanded = blocks.some((block) => block.expand);
    const lines = ['## Boot web vitals (no backend)', ''];
    if (anyExpanded) {
        lines.push(BOOT_COMMENT_INTRO);
        lines.push('');
    }
    if (loginError) {
        lines.push(`\`/login\` — login run failed: ${loginError}`);
        lines.push('');
    }
    if (login) {
        if ((login.raw_runs || []).some((run) => run.error)) lines.push(...failedRunLines(login));
        for (const block of renderRouteBlocks(login)) lines.push(...block.lines);
    }
    if (homeError) {
        lines.push(`\`/\` — home run failed: ${homeError}`);
        lines.push('');
    }
    if (home) {
        if ((home.raw_runs || []).some((run) => run.error)) lines.push(...failedRunLines(home));
        for (const block of renderRouteBlocks(home)) lines.push(...block.lines);
        const unknown = home.stubUnknownOperations || [];
        if (unknown.length > 0) {
            lines.push('');
            lines.push(
                `\`/\` asked for GraphQL operations with no recorded fixture, answered with empty data: ` +
                    `${unknown.map((name) => `\`${name}\``).join(', ')}. ` +
                    'Add them to `e2e-test/perf/vitals/stub-graphql.mjs` so the shell renders as it does signed in.',
            );
        }
    }
    return lines.join('\n').replace(/\n{3,}/g, '\n\n');
}
