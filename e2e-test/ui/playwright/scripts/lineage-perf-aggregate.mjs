#!/usr/bin/env node
/**
 * Aggregate `lineage-perf.json` samples produced by the lineage perf
 * benchmark into variance bands per (scenario, variant, action).
 *
 * The perf spec writes one row per (scenario, variant, iteration). With
 * `LINEAGE_PERF_REPEAT=N` you get N rows per (scenario, variant); this
 * script groups them and reports {n, min, p50, p95, max, mean, stddev}
 * for each measured action's `wallMs` and `longTaskTotalMs`.
 *
 * Usage:
 *   node scripts/lineage-perf-aggregate.mjs            # reads test-results/lineage-perf.json
 *   node scripts/lineage-perf-aggregate.mjs --json     # emit JSON instead of a table
 *   node scripts/lineage-perf-aggregate.mjs --in path  # custom input
 *
 * Bands use linear interpolation (numpy / Excel default). With N=5
 * samples p95 is effectively max — that is intentional; the band still
 * exposes outliers even when the cohort is small.
 */

import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { json: false, input: null, scenario: null, variant: null };
  for (let i = 0; i < args.length; i += 1) {
    const a = args[i];
    if (a === '--json') out.json = true;
    else if (a === '--in') {
      out.input = args[i + 1];
      i += 1;
    } else if (a === '--scenario') {
      out.scenario = args[i + 1];
      i += 1;
    } else if (a === '--variant') {
      out.variant = args[i + 1];
      i += 1;
    } else if (a === '--help' || a === '-h') {
      process.stdout.write(
        [
          'Usage: lineage-perf-aggregate.mjs [--in path] [--json] [--scenario s] [--variant v]',
          '',
          '  --in <path>        path to lineage-perf.json (default: ../test-results/lineage-perf.json)',
          '  --json             emit JSON instead of an ASCII table',
          '  --scenario <name>  filter to a single scenario (substring match)',
          '  --variant <name>   filter to a single variant (substring match)',
        ].join('\n') + '\n',
      );
      process.exit(0);
    }
  }
  return out;
}

function defaultInputPath() {
  return path.resolve(__dirname, '..', 'test-results', 'lineage-perf.json');
}

function percentile(sortedAsc, p) {
  if (sortedAsc.length === 0) return NaN;
  if (sortedAsc.length === 1) return sortedAsc[0];
  const rank = (p / 100) * (sortedAsc.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sortedAsc[lo];
  const frac = rank - lo;
  return sortedAsc[lo] + (sortedAsc[hi] - sortedAsc[lo]) * frac;
}

function stats(values) {
  if (values.length === 0) {
    return { n: 0, min: NaN, p50: NaN, p95: NaN, max: NaN, mean: NaN, stddev: NaN };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  const mean = sorted.reduce((s, v) => s + v, 0) / n;
  const variance = n > 1 ? sorted.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1) : 0;
  return {
    n,
    min: sorted[0],
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    max: sorted[n - 1],
    mean,
    stddev: Math.sqrt(variance),
  };
}

function fmt(value) {
  if (!Number.isFinite(value)) return '–';
  if (value === 0) return '0';
  if (value < 10) return value.toFixed(2);
  if (value < 100) return value.toFixed(1);
  return value.toFixed(0);
}

function aggregate(recordings, filter) {
  // Group: scenario → variant → action.label → metric → samples[]
  const groups = new Map();
  for (const rec of recordings) {
    if (filter.scenario && !rec.scenario.includes(filter.scenario)) continue;
    if (filter.variant && !rec.variant.includes(filter.variant)) continue;
    for (const action of rec.actions ?? []) {
      const key = `${rec.scenario}\t${rec.variant}\t${action.label}`;
      let entry = groups.get(key);
      if (!entry) {
        entry = {
          scenario: rec.scenario,
          variant: rec.variant,
          action: action.label,
          wallMs: [],
          longTaskTotalMs: [],
          longTaskMaxMs: [],
          requestsDuring: [],
          graphqlRequestsDuring: [],
          requestBytesDuring: [],
          timedOutCount: 0,
        };
        groups.set(key, entry);
      }
      if (typeof action.wallMs === 'number') entry.wallMs.push(action.wallMs);
      if (typeof action.longTaskTotalMs === 'number') {
        entry.longTaskTotalMs.push(action.longTaskTotalMs);
      }
      if (typeof action.longTaskMaxMs === 'number') {
        entry.longTaskMaxMs.push(action.longTaskMaxMs);
      }
      if (typeof action.requestsDuring === 'number') {
        entry.requestsDuring.push(action.requestsDuring);
      }
      if (typeof action.graphqlRequestsDuring === 'number') {
        entry.graphqlRequestsDuring.push(action.graphqlRequestsDuring);
      }
      if (typeof action.requestBytesDuring === 'number') {
        entry.requestBytesDuring.push(action.requestBytesDuring);
      }
      if (action.timedOut) entry.timedOutCount += 1;
    }
  }

  const rows = [];
  for (const entry of groups.values()) {
    rows.push({
      scenario: entry.scenario,
      variant: entry.variant,
      action: entry.action,
      wallMs: stats(entry.wallMs),
      longTaskTotalMs: stats(entry.longTaskTotalMs),
      longTaskMaxMs: stats(entry.longTaskMaxMs),
      requestsDuring: stats(entry.requestsDuring),
      graphqlRequestsDuring: stats(entry.graphqlRequestsDuring),
      requestBytesDuring: stats(entry.requestBytesDuring),
      timedOutCount: entry.timedOutCount,
    });
  }
  rows.sort((a, b) => {
    if (a.scenario !== b.scenario) return a.scenario.localeCompare(b.scenario);
    if (a.variant !== b.variant) return a.variant.localeCompare(b.variant);
    return a.action.localeCompare(b.action);
  });
  return rows;
}

function renderTable(rows) {
  if (rows.length === 0) {
    return '(no rows — check input file / filters)';
  }
  const lines = [];
  let currentScenario = null;
  for (const row of rows) {
    if (row.scenario !== currentScenario) {
      lines.push('');
      lines.push(`=== ${row.scenario} ===`);
      lines.push(
        [
          'variant',
          'action',
          'n',
          'wall_p50',
          'wall_p95',
          'wall_max',
          'longTask_total_p50',
          'longTask_max_p95',
          'req_p50',
          'gql_p50',
          'kb_p50',
          'timedOut',
        ]
          .map((s) => s.padEnd(20))
          .join(''),
      );
      currentScenario = row.scenario;
    }
    const kbP50 =
      Number.isFinite(row.requestBytesDuring.p50) && row.requestBytesDuring.p50 > 0
        ? row.requestBytesDuring.p50 / 1024
        : 0;
    lines.push(
      [
        row.variant,
        row.action,
        String(row.wallMs.n),
        fmt(row.wallMs.p50),
        fmt(row.wallMs.p95),
        fmt(row.wallMs.max),
        fmt(row.longTaskTotalMs.p50),
        fmt(row.longTaskMaxMs.p95),
        fmt(row.requestsDuring.p50),
        fmt(row.graphqlRequestsDuring.p50),
        fmt(kbP50),
        row.timedOutCount > 0 ? String(row.timedOutCount) : '',
      ]
        .map((s) => String(s).padEnd(20))
        .join(''),
    );
  }
  return lines.join('\n');
}

function main() {
  const args = parseArgs();
  const inputPath = args.input ?? defaultInputPath();
  if (!existsSync(inputPath)) {
    process.stderr.write(`No such file: ${inputPath}\n`);
    process.exit(1);
  }
  const raw = readFileSync(inputPath, 'utf8');
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    process.stderr.write(`Failed to parse ${inputPath}: ${err.message}\n`);
    process.exit(1);
  }
  if (!Array.isArray(parsed)) {
    process.stderr.write(`Expected ${inputPath} to be a JSON array.\n`);
    process.exit(1);
  }

  const rows = aggregate(parsed, args);

  if (args.json) {
    process.stdout.write(`${JSON.stringify(rows, null, 2)}\n`);
    return;
  }
  process.stdout.write(`${renderTable(rows)}\n`);
}

main();
