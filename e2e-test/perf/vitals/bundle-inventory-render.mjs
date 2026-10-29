/**
 * Markdown / regression rendering for eager bundle inventory reports.
 */
import { shortId } from './bundle-inventory-core.mjs';

const fmt = (n) => (n === null || n === undefined ? '—' : n.toLocaleString('en-US'));
const kb = (n) => `${(n / 1024).toFixed(1)} KB`;
const pct = (n) => `${(n * 100).toFixed(2)}%`;

function table(headers, rows) {
    const align = headers.map((h) => (h.startsWith(':') ? ':--' : '--:'));
    const clean = headers.map((h) => h.replace(/^:/, ''));
    return [
        `| ${clean.join(' | ')} |`,
        `| ${align.join(' | ')} |`,
        ...rows.map((r) => `| ${r.join(' | ')} |`),
    ].join('\n');
}

/** Drop Vite's trailing content hash (8+ chars) so `mui-icons-catalog-<hash>.js` stays `mui-icons-catalog`. */
export function chunkRole(file) {
    return file.replace(/-[A-Za-z0-9_-]{8}\.js$/, '').replace(/\.js$/, '');
}

export function renderMarkdown(report, { top = 30 } = {}) {
    const out = [];
    const { label, commit, generated_at: generatedAt } = report.meta;
    out.push(`# Eager JS bundle inventory — ${label}${commit ? ` @ \`${commit.slice(0, 10)}\`` : ''}`);
    out.push('');
    out.push(
        `Generated ${generatedAt}. Eager = \`<script>\` and modulepreload JS in \`dist/index.html\`; gzip = Node zlib level 9.`,
    );
    out.push('');

    const inv = report.inventory;
    const isLive = Boolean(inv.origin);
    out.push(isLive ? `## Eager JS as served by ${inv.origin}` : '## Eager vs async');
    out.push('');
    if (isLive && inv.app_version) {
        out.push(`Deployed \`appVersion\`: \`${inv.app_version}\` (from \`/config\`).`);
        out.push('');
    }
    const setRows = [
        ['Eager JS', inv.eager_js.count, fmt(inv.eager_js.raw_bytes), `**${fmt(inv.eager_js.gzip_bytes)}**`],
    ];
    if (!isLive) {
        setRows.push(
            ['Eager CSS', inv.eager_css.files.length, fmt(inv.eager_css.raw_bytes), fmt(inv.eager_css.gzip_bytes)],
            ['Async JS', inv.async_js.count, fmt(inv.async_js.raw_bytes), fmt(inv.async_js.gzip_bytes)],
            ['All JS', inv.all_js.count, fmt(inv.all_js.raw_bytes), fmt(inv.all_js.gzip_bytes)],
        );
    }
    out.push(table([':Set', 'Files', 'Raw bytes', 'Gzip bytes'], setRows));
    out.push('');
    if (isLive && inv.eager_js.transfer_bytes !== null) {
        out.push(`Server transfer (Content-Length of encoded responses): **${fmt(inv.eager_js.transfer_bytes)}** bytes.`);
        out.push('');
    }
    out.push('Eager JS files:');
    out.push('');
    out.push(
        table(
            isLive ? [':File', ':Chunk', 'Raw', 'Gzip -9', 'Transfer', ':Encoding'] : [':File', ':Chunk', 'Raw', 'Gzip'],
            inv.eager_js.files.map((f) =>
                isLive
                    ? [
                          `\`${f.file}\``,
                          `\`${chunkRole(f.file)}\``,
                          fmt(f.raw_bytes),
                          fmt(f.gzip_bytes),
                          fmt(f.transfer_bytes),
                          f.content_encoding ?? 'identity',
                      ]
                    : [`\`${f.file}\``, `\`${chunkRole(f.file)}\``, fmt(f.raw_bytes), fmt(f.gzip_bytes)],
            ),
        ),
    );
    if (inv.index_html.modulepreload.length) {
        out.push('');
        out.push(`\`<link rel="modulepreload">\`: ${inv.index_html.modulepreload.map((s) => `\`${s}\``).join(', ')}`);
    }
    out.push('');

    const attr = report.attribution;
    if (attr && attr.packages.length) {
        out.push(`## Top ${top} packages across eager JS (gzip)`);
        out.push('');
        out.push(
            table(
                ['Rank', ':Package', 'Gzip bytes', 'Share of eager gzip', ':Chunks', 'Files'],
                attr.packages
                    .slice(0, top)
                    .map((r, i) => [
                        i + 1,
                        `\`${r.package}\``,
                        fmt(r.gzip_bytes),
                        pct(r.share_of_eager_gzip),
                        r.chunks.map(chunkRole).join(', '),
                        r.source_file_count,
                    ]),
            ),
        );
        out.push('');
        out.push(
            `Sum of per-package gzip blobs: ${fmt(attr.sum_of_package_gzip_bytes)} vs whole-file eager gzip ${fmt(
                attr.eager_whole_file_gzip_bytes,
            )}.`,
        );
        out.push('');

        const reachByPkg = new Map((report.reachability?.packages ?? []).map((p) => [p.package, p]));
        const whyEager = (key) => {
            const r = reachByPkg.get(key);
            if (!r) return '';
            if (r.dynamic_only_share >= 0.999) {
                const site = r.dynamic_only_samples[0]?.dynamic_entry;
                return site
                    ? `**dynamic-only** — \`import()\` in \`${shortId(site.importers[0])}\``
                    : '**dynamic-only**';
            }
            if (!r.static_chain) return '';
            const chain = r.static_chain.map((s) => `\`${s}\``).join(' → ');
            return r.dynamic_only_share > 0.05 ? `${chain} (${pct(r.dynamic_only_share)} dyn-only)` : chain;
        };
        for (const [chunkFile, rows] of Object.entries(attr.per_chunk)) {
            const role = chunkRole(chunkFile);
            const entries = Object.entries(rows);
            if (entries.length <= 1) continue;
            const chunkGz = attr.chunks.find((c) => c.file === chunkFile)?.gzip_bytes ?? 0;
            const shown = entries.slice(0, top);
            const shownGz = shown.reduce((a, [, r]) => a + r.gzip_bytes, 0);
            const restGz = entries.slice(top).reduce((a, [, r]) => a + r.gzip_bytes, 0);
            out.push(`## \`${role}\` chunk — top ${top} packages (gzip of in-chunk blob)`);
            out.push('');
            out.push(`\`${chunkFile}\`: whole-file gzip ${fmt(chunkGz)}; ${entries.length} attribution keys.`);
            out.push('');
            const hasReach = reachByPkg.size > 0;
            out.push(
                table(
                    [
                        'Rank',
                        ':Package',
                        'Gzip bytes',
                        'Share of chunk gzip',
                        'Raw bytes',
                        'Files',
                        ...(hasReach ? [':Why eager (shortest static chain from entry)'] : []),
                    ],
                    shown.map(([key, r], i) => [
                        i + 1,
                        `\`${key}\``,
                        fmt(r.gzip_bytes),
                        chunkGz ? pct(r.gzip_bytes / chunkGz) : '—',
                        fmt(r.raw_bytes),
                        r.source_file_count,
                        ...(hasReach ? [whyEager(key)] : []),
                    ]),
                ),
            );
            out.push('');
            out.push(
                `Top ${shown.length} sum to ${fmt(shownGz)} (${chunkGz ? pct(shownGz / chunkGz) : '—'} of the chunk); ` +
                    `remaining ${Math.max(entries.length - top, 0)} keys sum to ${fmt(restGz)}.`,
            );
            out.push('');
        }

        if (attr.app_source_areas.length) {
            out.push('## First-party `app-source` by directory (gzip of blob)');
            out.push('');
            out.push(
                table(
                    ['Rank', ':Directory', 'Gzip bytes', 'Raw bytes', 'Files'],
                    attr.app_source_areas
                        .slice(0, top)
                        .map((r, i) => [i + 1, `\`${r.package}\``, fmt(r.gzip_bytes), fmt(r.raw_bytes), r.source_file_count]),
                ),
            );
            out.push('');
        }
    }

    const reach = report.reachability;
    if (reach) {
        const gzFor = new Map((attr?.packages ?? []).map((p) => [p.package, p.gzip_bytes]));
        const dynRows = reach.packages
            .filter((p) => p.dynamic_only_rendered_bytes > 0)
            .sort((a, b) => b.dynamic_only_rendered_bytes - a.dynamic_only_rendered_bytes)
            .slice(0, top);
        out.push('## Eager modules reachable only through `import()` (pinned by `manualChunks`)');
        out.push('');
        out.push(
            `${fmt(reach.static_reachable_module_count)} of ${fmt(reach.total_module_count)} modules are statically ` +
                'reachable from the entry. Rows below are packages with bytes in an eager chunk that no static ' +
                'import path reaches; gzip is estimated as the package gzip × dynamic-only share.',
        );
        out.push('');
        out.push(
            table(
                [':Package', 'Dyn-only rendered', 'Pkg rendered', 'Dyn-only share', 'Est. gzip', ':Chunk', ':First dynamic import site'],
                dynRows.map((p) => {
                    const gz = gzFor.get(p.package);
                    const estGz = gz !== undefined ? Math.round(gz * p.dynamic_only_share) : null;
                    const site = p.dynamic_only_samples[0]?.dynamic_entry;
                    const siteText = site
                        ? `\`${shortId(site.importers[0])}\` → \`${shortId(site.module)}\``
                        : p.app_dynamic_importers[0]
                          ? `\`${shortId(p.app_dynamic_importers[0])}\``
                          : '—';
                    return [
                        `\`${p.package}\``,
                        fmt(p.dynamic_only_rendered_bytes),
                        fmt(p.rendered_bytes),
                        pct(p.dynamic_only_share),
                        estGz === null ? '—' : fmt(estGz),
                        p.chunks.map(chunkRole).join(', '),
                        siteText,
                    ];
                }),
            ),
        );
        out.push('');
    }

    if (report.baseline) {
        const b = report.baseline;
        out.push(`## Delta vs baseline (${b.label})`);
        out.push('');
        out.push(
            table(
                [':Package', 'Baseline gzip', 'Now gzip', 'Δ gzip'],
                b.deltas.slice(0, top).map((d) => [
                    `\`${d.package}\``,
                    fmt(d.baseline_gzip_bytes),
                    fmt(d.gzip_bytes),
                    `${d.delta >= 0 ? '+' : '−'}${fmt(Math.abs(d.delta))}`,
                ]),
            ),
        );
        out.push('');
    }

    if (!isLive && inv.async_js?.largest?.length) {
        out.push('## Largest async chunks (gzip)');
        out.push('');
        out.push(
            table(
                [':File', 'Raw', 'Gzip'],
                inv.async_js.largest.slice(0, 15).map((f) => [`\`${f.file}\``, fmt(f.raw_bytes), fmt(f.gzip_bytes)]),
            ),
        );
        out.push('');
    }

    return `${out.join('\n')}\n`;
}

export function diffAgainstBaseline(packages, baselinePackages, label) {
    const base = new Map(baselinePackages.map((p) => [p.package, p.gzip_bytes]));
    const samples = new Map(packages.map((p) => [p.package, p.sample_sources || []]));
    const seen = new Set();
    const deltas = [];
    for (const p of packages) {
        seen.add(p.package);
        const before = base.get(p.package) ?? 0;
        deltas.push({
            package: p.package,
            baseline_gzip_bytes: before,
            gzip_bytes: p.gzip_bytes,
            delta: p.gzip_bytes - before,
            sample_sources: p.sample_sources || [],
        });
    }
    for (const [pkg, before] of base) {
        if (!seen.has(pkg)) {
            deltas.push({
                package: pkg,
                baseline_gzip_bytes: before,
                gzip_bytes: 0,
                delta: -before,
                sample_sources: samples.get(pkg) || [],
            });
        }
    }
    deltas.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
    return { label, deltas };
}

/** Package moves smaller than this are dictionary noise, not a regression to chase. */
export const REGRESSION_MIN_DELTA = 512;

export function notableDeltas(deltas, { top = 20, minAbsDelta = REGRESSION_MIN_DELTA } = {}) {
    return deltas.filter((d) => Math.abs(d.delta) >= minAbsDelta).slice(0, top);
}

function deltaCell(delta) {
    return `${delta >= 0 ? '+' : '−'}${fmt(Math.abs(delta))}`;
}

function regressionTable(deltas) {
    if (!deltas.length) {
        return `_Nothing moved by ${fmt(REGRESSION_MIN_DELTA)} bytes or more._`;
    }
    return table(
        [':Name', 'Accepted gzip', 'Now gzip', 'Δ gzip'],
        deltas.map((d) => [
            `\`${d.package}\``,
            fmt(d.baseline_gzip_bytes),
            fmt(d.gzip_bytes),
            deltaCell(d.delta),
        ]),
    );
}

function exampleSources(deltas) {
    const lines = deltas
        .filter((d) => d.delta > 0 && d.sample_sources?.length)
        .slice(0, 8)
        .map((d) => `- \`${d.package}\` — \`${d.sample_sources.slice(0, 3).join('`, `')}\``);
    if (!lines.length) return '';
    return `\nExamples from groups that grew:\n\n${lines.join('\n')}\n`;
}

/**
 * Short report for an eager-budget failure: which packages and first-party
 * areas moved relative to the accepted attribution, not the whole inventory.
 */
export function renderRegression({
    eagerGzip,
    baselineGzip,
    packageDiff,
    areaDiff,
    phosphorModules,
    muiIconModules,
    top = 20,
} = {}) {
    const packages = notableDeltas(packageDiff?.deltas || [], { top });
    const areas = notableDeltas(areaDiff?.deltas || [], { top });
    const gzipLine =
        baselineGzip == null
            ? `Eager gzip is ${fmt(eagerGzip)}.`
            : `Eager gzip is ${fmt(eagerGzip)} (accepted attribution ${fmt(baselineGzip)}, ${deltaCell((eagerGzip || 0) - baselineGzip)}).`;
    const counts =
        phosphorModules == null
            ? ''
            : `\n\nPhosphor modules in eager chunks: ${fmt(phosphorModules)}. MUI icon modules: ${fmt(muiIconModules || 0)}.`;
    return (
        [
            '## Eager JS regression',
            '',
            gzipLine + counts,
            '',
            'Per-package gzip is the source-mapped bytes of that package compressed on their own, so the rows do not add up to the eager total.',
            '',
            '### Packages',
            '',
            regressionTable(packages),
            '',
            '### First-party areas',
            '',
            regressionTable(areas),
            exampleSources([...packages, ...areas]),
        ].join('\n') + '\n'
    );
}

