/**
 * Lineage rendering perf — user-journey benchmark across feature-flag variants.
 *
 * Three journeys (small graph, chain + column lineage, filtering hub) run once
 * per variant of the lineage perf flags. Each action records wall time, long
 * tasks, and pan/zoom FPS. Synthetic stress, CLL, and mixed-entity matrices
 * are opt-in via env vars.
 *
 * Opt-in: `LINEAGE_PERF=1`. Output: `test-results/lineage-perf.json` keyed by
 * `(scenario, variant)`.
 *
 *     LINEAGE_PERF=1 yarn playwright test \
 *         --config=playwright.no-webserver.config.ts tests/lineage-perf
 */

import { request as playwrightRequest } from '@playwright/test';

import { test } from '../../fixtures/base-test';
import { readGmsToken } from '../../fixtures/login';
import { LineagePerfRecorder, emitRecording, resetRecordingFile } from '../../helpers/lineage-perf-helper';
import {
  type MixedFanoutHandles,
  type SyntheticLineageHandles,
  seedMixedFanoutLineage,
  seedSyntheticLineage,
} from '../../helpers/seeders/lineage-perf-seeder';
import { LineagePerfPage } from '../../pages/lineage-perf.page';
import { users } from '../../data/users';
import { gmsUrl } from '../../utils/constants';

const RUN = process.env.LINEAGE_PERF === '1';

// Number of times to replay each (scenario, variant) pair. Each iteration
// emits its own row in `lineage-perf.json` tagged with `iteration`; use the
// aggregator (`scripts/lineage-perf-aggregate.mjs`) to compute p50/p95 bands.
const REPEAT = (() => {
  const raw = parseInt(process.env.LINEAGE_PERF_REPEAT ?? '1', 10);
  if (!Number.isFinite(raw) || raw < 1) return 1;
  return raw;
})();

function iterSuffix(iter: number): string {
  return REPEAT > 1 ? ` [iter ${iter}/${REPEAT}]` : '';
}

// Comma-separated total-node sizes for the synthetic stress matrix. Each size
// produces a `size/2 + size/2` upstream/downstream fanout under `perfTest`.
const SYNTHETIC_SIZES = (process.env.LINEAGE_PERF_SYNTHETIC_SCALES ?? '100,500')
  .split(',')
  .map((s) => parseInt(s.trim(), 10))
  .filter((n) => Number.isFinite(n) && n > 0);

// CLL matrix: `<datasetsPerSide>x<columnsPerDataset>` (e.g. `10x50`). Empty by
// default since seeding column lineage at scale is expensive.
interface CllEntry {
  datasetsPerSide: number;
  columns: number;
}
const CLL_MATRIX: CllEntry[] = (process.env.LINEAGE_PERF_CLL_MATRIX ?? '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean)
  .map((token) => {
    const [d, c] = token.split('x').map((v) => parseInt(v, 10));
    if (!Number.isFinite(d) || !Number.isFinite(c) || d <= 0 || c <= 0) {
      throw new Error(`Invalid LINEAGE_PERF_CLL_MATRIX entry "${token}"; expected "DxC".`);
    }
    return { datasetsPerSide: d, columns: c };
  });

function cllScale(entry: CllEntry): string {
  return `cll_${entry.datasetsPerSide}x${entry.columns}`;
}

// Mixed-entity matrix: `<label>:ds=N,dj=N,dds=N,c=N,dash=N` (semicolon-
// separated for multiple configs). Empty by default.
//
//   ds=upstream datasets, dj=upstream dataJobs (each gets its own dataFlow +
//   1 input dataset), dds=downstream datasets, c=downstream charts,
//   dash=downstream dashboards (each owns 1 chart).
//
// Example: `small:ds=5,dj=5,dds=5,c=5,dash=3;mid:ds=20,dj=20,dds=20,c=20,dash=10`
interface MixedEntry {
  label: string;
  upstreamDatasets: number;
  upstreamDatajobs: number;
  downstreamDatasets: number;
  downstreamCharts: number;
  downstreamDashboards: number;
}
function parseMixedToken(token: string): MixedEntry {
  const [label, body] = token.split(':');
  if (!label || !body) {
    throw new Error(`Invalid LINEAGE_PERF_MIXED_MATRIX token "${token}"; expected "<label>:ds=<N>,dj=<N>,..."`);
  }
  const counts = new Map<string, number>();
  for (const part of body.split(',')) {
    const [k, v] = part.split('=');
    const n = parseInt(v, 10);
    if (!k || !Number.isFinite(n) || n < 0) {
      throw new Error(`Invalid mixed matrix part "${part}" in token "${token}".`);
    }
    counts.set(k.trim(), n);
  }
  return {
    label: label.trim(),
    upstreamDatasets: counts.get('ds') ?? 0,
    upstreamDatajobs: counts.get('dj') ?? 0,
    downstreamDatasets: counts.get('dds') ?? 0,
    downstreamCharts: counts.get('c') ?? 0,
    downstreamDashboards: counts.get('dash') ?? 0,
  };
}
const MIXED_MATRIX: MixedEntry[] = (process.env.LINEAGE_PERF_MIXED_MATRIX ?? '')
  .split(';')
  .map((s) => s.trim())
  .filter(Boolean)
  .map(parseMixedToken);

function mixedScale(entry: MixedEntry): string {
  return `mixed_${entry.label}`;
}

// ── Seeded URN constants (mirror tests/lineage-v2/v2-lineage-graph.spec.ts) ──

const SMALL_GRAPH_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';
const CHAIN_ROOT_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage.node1_dataset,PROD)';
const CHAIN_NEIGHBOUR_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node2_dataset,PROD)';
const CHAIN_COLUMN_NAME = 'record_id';
const FILTERING_HUB_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node7,PROD)';

// ── Variant matrix ──────────────────────────────────────────────────────────

interface Variant {
  name: string;
  flagString: string;
}

// `baseline` forces every lever off explicitly — empty string would resolve
// to the in-app `'auto'` defaults and contaminate the comparison as
// thresholds move. `defer-minimap` / `no-transition` variants were dropped
// after N=5 showed no p50 benefit and 3–5× wider p95.
const BASELINE_FLAGS = 'virt-off overscan-off';

const VARIANTS: Variant[] = [
  { name: 'baseline', flagString: BASELINE_FLAGS },
  { name: 'virt', flagString: 'virt' },
];

// ── Suite ────────────────────────────────────────────────────────────────────

// Per-test writes are append-style; reset once per worker boot so re-runs
// don't mix historical samples with the current run.
test.beforeAll(() => {
  if (!RUN) return;
  resetRecordingFile();
});

for (let iter = 1; iter <= REPEAT; iter += 1) {
  for (const variant of VARIANTS) {
    test.describe(`lineage perf [${variant.name}]${iterSuffix(iter)}`, () => {
      test.beforeEach(async ({ apiMock }) => {
        test.skip(!RUN, 'Lineage perf benchmark — set LINEAGE_PERF=1 to enable.');
        // Lock V2 lineage so the flags map to a known rendering pipeline.
        await apiMock.setFeatureFlags({ lineageGraphV3: false });
      });

      // ── Journey 1: small graph — load + pan + zoom + node click ──────────────

      test('small graph — load, pan, zoom, click node', async ({ page, logger, logDir }) => {
        test.setTimeout(60_000);
        const lp = new LineagePerfPage(page, logger, logDir);
        await lp.setPerfFlags(variant.flagString);
        const rec = new LineagePerfRecorder(page);
        await rec.install();

        await rec.measure('navigate', async () => {
          await lp.goToLineageGraph('dataset', SMALL_GRAPH_URN);
        });
        await rec.measure('wait-first-node', async () => {
          await lp.waitForReactFlowNode(SMALL_GRAPH_URN);
        });
        await rec.measure('settle', async () => {
          await lp.waitForStableNodeCount();
        });
        await rec.measureWithFps('pan-horizontal', () => lp.panHorizontal());
        await rec.measureWithFps('zoom-out', () => lp.zoomOut(3));
        await rec.measureWithFps('zoom-in', () => lp.zoomIn(3));
        await rec.measure('click-center-node', () => lp.clickNodeBody(SMALL_GRAPH_URN));
        await rec.measure('close-sidebar', () => lp.closeSidebar());

        const summary = await rec.summarise();
        emitRecording({
          scenario: 'small_graph',
          variant: variant.name,
          iteration: iter,
          url: page.url(),
          actions: rec.recordedActions(),
          summary,
        });
      });

      // ── Journey 2: chain graph — column lineage flow ─────────────────────────

      test('chain graph — load, expand columns, column lineage', async ({ page, logger, logDir }) => {
        test.setTimeout(90_000);
        const lp = new LineagePerfPage(page, logger, logDir);
        await lp.setPerfFlags(variant.flagString);
        const rec = new LineagePerfRecorder(page);
        await rec.install();

        await rec.measure('navigate', async () => {
          await lp.goToLineageGraph('dataset', CHAIN_ROOT_URN);
        });
        await rec.measure('wait-first-node', async () => {
          await lp.waitForReactFlowNode(CHAIN_ROOT_URN);
        });
        await rec.measure('settle', async () => {
          await lp.waitForStableNodeCount();
        });
        // Under `virt`, fitView can leave the neighbour off-screen; the
        // unmount would time out the later expand/hover. Zoom out once to
        // keep both nodes mounted. Measured separately to keep pan/zoom clean.
        await rec.measure('pre-interaction-zoom-out', () => lp.zoomOut(1));
        await rec.measureWithFps('pan-horizontal', () => lp.panHorizontal());
        await rec.measureWithFps('zoom-out', () => lp.zoomOut(3));
        await rec.measureWithFps('zoom-in', () => lp.zoomIn(3));
        await rec.measure('click-neighbour', () => lp.clickNodeBody(CHAIN_NEIGHBOUR_URN));

        await rec.measure(
          'expand-columns',
          async () => {
            await lp.expandContractColumns(CHAIN_NEIGHBOUR_URN);
          },
          { timeoutMs: 5000 },
        );
        await rec.measure(
          'hover-column',
          async () => {
            await lp.hoverColumn(CHAIN_NEIGHBOUR_URN, CHAIN_COLUMN_NAME);
            await page
              .locator(`[data-testid^="rf__edge-${CHAIN_ROOT_URN}::${CHAIN_COLUMN_NAME}-"]`)
              .first()
              .waitFor({ state: 'attached', timeout: 3000 });
          },
          { timeoutMs: 5000 },
        );
        await rec.measure('unhover-column', () => lp.unhoverColumn(CHAIN_NEIGHBOUR_URN, CHAIN_COLUMN_NAME), {
          timeoutMs: 3000,
        });
        await rec.measure('contract-columns', () => lp.expandContractColumns(CHAIN_NEIGHBOUR_URN), { timeoutMs: 5000 });

        const summary = await rec.summarise();
        emitRecording({
          scenario: 'chain_graph',
          variant: variant.name,
          iteration: iter,
          url: page.url(),
          actions: rec.recordedActions(),
          summary,
        });
      });

      // ── Journey 3: filtering hub — expand/contract neighbours ────────────────

      test('filtering hub — load, expand-one, contract', async ({ page, logger, logDir }) => {
        test.setTimeout(90_000);
        const lp = new LineagePerfPage(page, logger, logDir);
        await lp.setPerfFlags(variant.flagString);
        const rec = new LineagePerfRecorder(page);
        await rec.install();

        await rec.measure('navigate', async () => {
          await lp.goToLineageGraph('dataset', FILTERING_HUB_URN);
        });
        await rec.measure('wait-first-node', async () => {
          await lp.waitForReactFlowNode(FILTERING_HUB_URN);
        });
        await rec.measure('settle', async () => {
          await lp.waitForStableNodeCount();
        });
        // See `chain graph` — same fitView+virt off-screen problem.
        await rec.measure('pre-interaction-zoom-out', () => lp.zoomOut(1));
        await rec.measureWithFps('pan-horizontal', () => lp.panHorizontal());
        await rec.measureWithFps('zoom-out', () => lp.zoomOut(3));
        await rec.measureWithFps('zoom-in', () => lp.zoomIn(3));
        await rec.measure(
          'expand-one',
          async () => {
            await lp.expandOne(FILTERING_HUB_URN);
            await lp.waitForStableNodeCount();
          },
          { timeoutMs: 6000 },
        );
        await rec.measure(
          'contract',
          async () => {
            await lp.contract(FILTERING_HUB_URN);
            await lp.waitForStableNodeCount();
          },
          { timeoutMs: 6000 },
        );

        const summary = await rec.summarise();
        emitRecording({
          scenario: 'filtering_hub',
          variant: variant.name,
          iteration: iter,
          url: page.url(),
          actions: rec.recordedActions(),
          summary,
        });
      });
    });
  }
}

// ── Synthetic stress matrix ─────────────────────────────────────────────────
//
// Same journey at production scale (100/500/1000+ nodes). Reduced variant
// set: `defer-minimap` / `no-transition` removed (see comment above
// `VARIANTS`); the remaining levers are strict virt vs overscan-buffered virt.

const SYNTHETIC_VARIANTS: Variant[] = [
  { name: 'baseline', flagString: BASELINE_FLAGS },
  { name: 'virt', flagString: 'virt' },
  // Strict-vs-buffered trade-off: overscan kills pop-in but mounts ~30%
  // more nodes, so pan worst-frame is slower than `virt` alone.
  { name: 'virt+overscan', flagString: 'virt overscan' },
];

// Wrap in a describe so the seeding beforeAll only fires when synthetic
// tests are actually selected — otherwise the small-scale tests above would
// pay the 1000-dataset ingest cost on every run.
test.describe('lineage perf synthetic suite', () => {
  test.skip(SYNTHETIC_SIZES.length === 0, 'No synthetic scales configured.');

  const seededRoots = new Map<number, string>();

  test.beforeAll(async () => {
    if (!RUN) return;
    // Default 60 s is too tight: seeding 1000 datasets × (stub +
    // schemaMetadata + upstreamLineage w/ per-column FGLs) is ~3000
    // sequential ingest calls plus OpenSearch index polling. Allow 30 min.
    test.setTimeout(30 * 60_000);
    const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
    try {
      const gmsToken = readGmsToken(users.admin.username);
      for (const size of SYNTHETIC_SIZES) {
        const half = Math.floor(size / 2);
        const handles = await seedSyntheticLineage(apiContext, gmsToken, {
          scale: String(size),
          upstreamCount: half,
          downstreamCount: size - half,
        });
        seededRoots.set(size, handles.rootUrn);
      }
    } finally {
      await apiContext.dispose();
    }
  });

  for (let iter = 1; iter <= REPEAT; iter += 1) {
    for (const size of SYNTHETIC_SIZES) {
      for (const variant of SYNTHETIC_VARIANTS) {
        test.describe(`scale ${size} [${variant.name}]${iterSuffix(iter)}`, () => {
          test.beforeEach(async ({ apiMock }) => {
            test.skip(!RUN, 'Lineage perf benchmark — set LINEAGE_PERF=1 to enable.');
            await apiMock.setFeatureFlags({ lineageGraphV3: false });
          });

          test(`synthetic ${size} — load, expand all, pan, zoom`, async ({ page, logger, logDir }) => {
            // At 500 nodes, navigation + fetch + expand can each take seconds.
            test.setTimeout(180_000);
            const lp = new LineagePerfPage(page, logger, logDir);
            await lp.setPerfFlags(variant.flagString);
            const rec = new LineagePerfRecorder(page);
            await rec.install();

            const rootUrn = seededRoots.get(size);
            if (!rootUrn) {
              throw new Error(`Synthetic root URN missing for size ${size}; beforeAll seed must have failed.`);
            }

            await rec.measure('navigate', async () => {
              await lp.goToLineageGraph('dataset', rootUrn);
            });
            await rec.measure('wait-first-node', async () => {
              await lp.waitForReactFlowNode(rootUrn);
            });
            await rec.measure('settle-default', async () => {
              await lp.waitForStableNodeCount({ maxMs: 15_000 });
            });

            // Under virt + fitView at scale, filter-node wrappers can land off-
            // screen and make expand-fanout a no-op. Zoom out first to mount them.
            await rec.measure('pre-expand-zoom-out', () => lp.zoomOut(2));

            // Force-load every neighbour past LINEAGE_FILTER_PAGINATION (4) so
            // the measurement covers the full corpus, not the default cap.
            await rec.measure('expand-fanout', async () => {
              await lp.expandFanoutFully();
            });
            await rec.measure('settle-expanded', async () => {
              await lp.waitForStableNodeCount({ maxMs: 30_000 });
            });

            await rec.measureWithFps('pan-horizontal', () => lp.panHorizontal());
            await rec.measureWithFps('zoom-out', () => lp.zoomOut(3));
            await rec.measureWithFps('zoom-in', () => lp.zoomIn(3));

            // Expand columns on the root so the measurement reflects the
            // CLL-render cost users see in production (root carries
            // DEFAULT_SYNTHETIC_COLUMN_COUNT columns + per-column FGLs).
            // Captures column-row mount + edge-highlight pass on hover.
            await rec.measure('expand-columns-root', () => lp.expandContractColumns(rootUrn));
            await rec.measureWithFps('hover-column-root', async () => {
              await lp.hoverColumn(rootUrn, 'col_001');
              await page.waitForTimeout(150);
              await lp.unhoverColumn(rootUrn, 'col_001');
            });

            await rec.measure('click-root', () => lp.clickNodeBody(rootUrn));
            await rec.measure('close-sidebar', () => lp.closeSidebar());

            const summary = await rec.summarise();
            emitRecording({
              scenario: `synthetic_${size}`,
              variant: variant.name,
              iteration: iter,
              url: page.url(),
              actions: rec.recordedActions(),
              summary,
            });
          });
        });
      }
    }
  }
});

// ── Column-lineage stress matrix ────────────────────────────────────────────
//
// Fanout corpora carrying per-column FGLs. Each expand-columns click adds
// hundreds of SVG paths; each column hover triggers an edge-highlight pass
// over every incident FGL. Opt-in via `LINEAGE_PERF_CLL_MATRIX` because
// seeding column lineage is expensive.
test.describe('lineage perf column-lineage suite', () => {
  test.skip(CLL_MATRIX.length === 0, 'No CLL matrix configured.');

  const seeded = new Map<string, SyntheticLineageHandles>();

  test.beforeAll(async () => {
    if (!RUN) return;
    // Default 60 s is too tight for high-column-count matrices; FGL volume
    // multiplies sequential ingest cost. Match the synthetic suite budget.
    test.setTimeout(30 * 60_000);
    const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
    try {
      const gmsToken = readGmsToken(users.admin.username);
      for (const entry of CLL_MATRIX) {
        const handles = await seedSyntheticLineage(apiContext, gmsToken, {
          scale: cllScale(entry),
          upstreamCount: entry.datasetsPerSide,
          downstreamCount: entry.datasetsPerSide,
          columnCount: entry.columns,
        });
        seeded.set(cllScale(entry), handles);
      }
    } finally {
      await apiContext.dispose();
    }
  });

  for (let iter = 1; iter <= REPEAT; iter += 1) {
    for (const entry of CLL_MATRIX) {
      for (const variant of SYNTHETIC_VARIANTS) {
        test.describe(`cll ${entry.datasetsPerSide}×${entry.columns} [${variant.name}]${iterSuffix(iter)}`, () => {
          test.beforeEach(async ({ apiMock }) => {
            test.skip(!RUN, 'Lineage perf benchmark — set LINEAGE_PERF=1 to enable.');
            await apiMock.setFeatureFlags({ lineageGraphV3: false });
          });

          test(`cll ${cllScale(entry)} — load, expand columns, hover, pan`, async ({ page, logger, logDir }) => {
            // Column expand+hover at 50–100 cols can be slow on baseline.
            test.setTimeout(240_000);
            const lp = new LineagePerfPage(page, logger, logDir);
            await lp.setPerfFlags(variant.flagString);
            const rec = new LineagePerfRecorder(page);
            await rec.install();

            const handles = seeded.get(cllScale(entry));
            if (!handles) {
              throw new Error(`Synthetic CLL handles missing for ${cllScale(entry)}; beforeAll seed must have failed.`);
            }
            const { rootUrn, columnNames, downstreamUrns } = handles;
            // Column UI paginates at NUM_COLUMNS_PER_PAGE (5) — pick hover
            // targets from the first page so they're DOM-attached. Total column
            // count still drives FGL data + edge-highlight cost (what we measure).
            const hoverCol = columnNames[0];
            const secondHoverCol = columnNames[Math.min(2, columnNames.length - 1)];
            const downstreamUrn = downstreamUrns[0];

            await rec.measure('navigate', () => lp.goToLineageGraph('dataset', rootUrn));
            await rec.measure('wait-first-node', () => lp.waitForReactFlowNode(rootUrn));
            await rec.measure('settle-default', () => lp.waitForStableNodeCount({ maxMs: 15_000 }));

            await rec.measure('pre-expand-zoom-out', () => lp.zoomOut(2));

            // Expand fanout both ways so column hover hits the worst case:
            // every root column reaches every upstream/downstream peer.
            await rec.measure('expand-fanout', () => lp.expandFanoutFully());
            await rec.measure('settle-expanded', () => lp.waitForStableNodeCount({ maxMs: 30_000 }));

            // Wait for the column-expand affordance to attach — it only renders
            // once the bulk-lineage fetch populates the node's column count.
            await page
              .locator(`[data-testid="lineage-node-${rootUrn}"] [data-testid="expand-contract-columns"]`)
              .first()
              .waitFor({ state: 'attached', timeout: 20_000 });

            await rec.measure(
              'expand-columns-root',
              async () => {
                await lp.expandContractColumns(rootUrn);
                await lp.waitForStableNodeCount({ maxMs: 10_000, stableMs: 600 });
              },
              { timeoutMs: 12_000 },
            );
            await rec.measure('hover-column-root', () => lp.hoverColumn(rootUrn, hoverCol), { timeoutMs: 6_000 });
            await rec.measure('unhover-column-root', () => lp.unhoverColumn(rootUrn, hoverCol), { timeoutMs: 3_000 });

            // Large fanouts can leave the downstream entity unfetched long
            // after the root. Skip gracefully if the affordance never attaches.
            const downExpandButton = page
              .locator(`[data-testid="lineage-node-${downstreamUrn}"] [data-testid="expand-contract-columns"]`)
              .first();
            const downExpandReady = await downExpandButton
              .waitFor({ state: 'attached', timeout: 20_000 })
              .then(() => true)
              .catch(() => false);

            if (downExpandReady) {
              await rec.measure(
                'expand-columns-downstream',
                async () => {
                  await lp.expandContractColumns(downstreamUrn);
                  await lp.waitForStableNodeCount({ maxMs: 10_000, stableMs: 600 });
                },
                { timeoutMs: 12_000 },
              );
            }
            await rec.measure('hover-column-after-down-expand', () => lp.hoverColumn(rootUrn, secondHoverCol), {
              timeoutMs: 6_000,
            });
            await rec.measure('unhover-column-after-down-expand', () => lp.unhoverColumn(rootUrn, secondHoverCol), {
              timeoutMs: 3_000,
            });

            // ── Pan / zoom under load ───────────────────────────────────
            await rec.measureWithFps('pan-horizontal', () => lp.panHorizontal());
            await rec.measureWithFps('zoom-out', () => lp.zoomOut(3));
            await rec.measureWithFps('zoom-in', () => lp.zoomIn(3));

            // ── Tear down columns; measure contract cost too ───────────
            await rec.measure('contract-columns-root', () => lp.expandContractColumns(rootUrn), { timeoutMs: 6_000 });

            const summary = await rec.summarise();
            emitRecording({
              scenario: cllScale(entry),
              variant: variant.name,
              iteration: iter,
              url: page.url(),
              actions: rec.recordedActions(),
              summary,
            });
          });
        });
      }
    }
  }
});

// ── Mixed-entity stress matrix ──────────────────────────────────────────────
//
// Exercises every edge shape between heterogeneous entities (Dataset, DataJob,
// DataFlow, Chart, Dashboard) so virt is measured against "real" lineage
// topologies, not just dataset fanouts.
test.describe('lineage perf mixed-entity suite', () => {
  test.skip(MIXED_MATRIX.length === 0, 'No mixed-entity matrix configured.');

  const seeded = new Map<string, MixedFanoutHandles>();

  test.beforeAll(async () => {
    if (!RUN) return;
    // Default 60 s is too tight: datasets now carry schemas + per-column
    // FGLs, multiplying sequential ingest cost. Match the other suites.
    test.setTimeout(30 * 60_000);
    const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
    try {
      const gmsToken = readGmsToken(users.admin.username);
      for (const entry of MIXED_MATRIX) {
        const handles = await seedMixedFanoutLineage(apiContext, gmsToken, {
          scale: entry.label,
          upstreamDatasets: entry.upstreamDatasets,
          upstreamDatajobs: entry.upstreamDatajobs,
          downstreamDatasets: entry.downstreamDatasets,
          downstreamCharts: entry.downstreamCharts,
          downstreamDashboards: entry.downstreamDashboards,
        });
        seeded.set(mixedScale(entry), handles);
      }
    } finally {
      await apiContext.dispose();
    }
  });

  for (let iter = 1; iter <= REPEAT; iter += 1) {
    for (const entry of MIXED_MATRIX) {
      for (const variant of SYNTHETIC_VARIANTS) {
        test.describe(`mixed ${entry.label} [${variant.name}]${iterSuffix(iter)}`, () => {
          test.beforeEach(async ({ apiMock }) => {
            test.skip(!RUN, 'Lineage perf benchmark — set LINEAGE_PERF=1 to enable.');
            await apiMock.setFeatureFlags({ lineageGraphV3: false });
          });

          test(`mixed ${entry.label} — load, expand, pan, zoom`, async ({ page, logger, logDir }) => {
            test.setTimeout(240_000);
            const lp = new LineagePerfPage(page, logger, logDir);
            await lp.setPerfFlags(variant.flagString);
            const rec = new LineagePerfRecorder(page);
            await rec.install();

            const handles = seeded.get(mixedScale(entry));
            if (!handles) {
              throw new Error(`Mixed handles missing for ${mixedScale(entry)}; beforeAll seed must have failed.`);
            }
            const { rootUrn } = handles;

            await rec.measure('navigate', () => lp.goToLineageGraph('dataset', rootUrn));
            await rec.measure('wait-first-node', () => lp.waitForReactFlowNode(rootUrn));
            await rec.measure('settle-default', () => lp.waitForStableNodeCount({ maxMs: 15_000 }));

            await rec.measure('pre-expand-zoom-out', () => lp.zoomOut(2));

            // expandFanoutFully clicks Show More/All/Max for every entity type,
            // so DataJobs and Charts mount alongside Datasets.
            await rec.measure('expand-fanout', () => lp.expandFanoutFully());
            await rec.measure('settle-expanded', () => lp.waitForStableNodeCount({ maxMs: 30_000 }));

            await rec.measureWithFps('pan-horizontal', () => lp.panHorizontal());
            await rec.measureWithFps('zoom-out', () => lp.zoomOut(3));
            await rec.measureWithFps('zoom-in', () => lp.zoomIn(3));

            // Expand columns on the root so the measurement reflects the
            // CLL-render cost on heterogeneous lineage. Dataset nodes carry
            // DEFAULT_SYNTHETIC_COLUMN_COUNT columns + per-column FGLs;
            // DataJob/Chart/Dashboard neighbours simply ignore the action.
            await rec.measure('expand-columns-root', () => lp.expandContractColumns(rootUrn));
            await rec.measureWithFps('hover-column-root', async () => {
              await lp.hoverColumn(rootUrn, 'col_001');
              await page.waitForTimeout(150);
              await lp.unhoverColumn(rootUrn, 'col_001');
            });

            await rec.measure('click-root', () => lp.clickNodeBody(rootUrn));
            await rec.measure('close-sidebar', () => lp.closeSidebar());

            const summary = await rec.summarise();
            emitRecording({
              scenario: mixedScale(entry),
              variant: variant.name,
              iteration: iter,
              url: page.url(),
              actions: rec.recordedActions(),
              summary,
            });
          });
        });
      }
    }
  }
});
