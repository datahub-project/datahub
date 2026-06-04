/**
 * Screenshot-export stress test for lineage V2 under virtualisation.
 *
 * Verifies that `forceMountAll` in the screenshot button restores a fully-
 * mounted DOM for html-to-image even when virt has unmounted off-screen
 * nodes. For each scale × variant: expand the fanout, snapshot heap,
 * click the camera button, race the download event against the button
 * re-enabling, and record click→download wall time + the resulting PNG
 * byte size (the only reliable ground truth that capture saw the full
 * graph — a virtualised-viewport screenshot is < ~50 KB).
 *
 * Opt-in via `LINEAGE_PERF=1 LINEAGE_PERF_SCREENSHOT=1`.
 */

import { expect, request as playwrightRequest, type Page } from '@playwright/test';

import { test } from '../../fixtures/base-test';
import { readGmsToken } from '../../fixtures/login';
import { seedSyntheticLineage } from '../../helpers/seeders/lineage-perf-seeder';
import { LineagePerfPage } from '../../pages/lineage-perf.page';
import { users } from '../../data/users';
import { gmsUrl } from '../../utils/constants';

const RUN = process.env.LINEAGE_PERF === '1';
const SCREENSHOT_RUN = process.env.LINEAGE_PERF_SCREENSHOT === '1';

const SCREENSHOT_SCALES: number[] = (process.env.LINEAGE_PERF_SCREENSHOT_SCALES ?? '100,500,1000')
  .split(',')
  .map((s) => parseInt(s.trim(), 10))
  .filter((n) => Number.isFinite(n) && n > 0);

interface ScreenshotVariant {
  name: string;
  flagString: string;
}

// Only baseline + virt — overscan / `all` don't change screenshot
// behaviour, just the everyday render, so they'd add wall time without
// new information.
const BASELINE_FLAGS = 'virt-off overscan-off';
const SCREENSHOT_VARIANTS: ScreenshotVariant[] = [
  { name: 'baseline', flagString: BASELINE_FLAGS },
  { name: 'virt', flagString: 'virt' },
];

interface ScreenshotMeasurement {
  scale: number;
  variant: string;
  clickToDownloadMs: number;
  downloadByteCount: number;
  heapBeforeMb: number | null;
  heapAfterMb: number | null;
  heapDeltaMb: number | null;
  /** Mounted nodes immediately before the click (virt may be filtering). */
  domNodesBeforeCapture: number;
  /** Mounted nodes after capture finished (button restored virt). */
  domNodesAfterCapture: number;
}

const measurements: ScreenshotMeasurement[] = [];

async function readHeapMb(page: Page): Promise<number | null> {
  const bytes = await page.evaluate(() => {
    const mem = (performance as unknown as { memory?: { usedJSHeapSize: number } }).memory;
    return mem ? mem.usedJSHeapSize : null;
  });
  return bytes != null ? Math.round((bytes / 1024 / 1024) * 10) / 10 : null;
}

test.describe('lineage perf screenshot suite', () => {
  test.skip(!RUN || !SCREENSHOT_RUN, 'Set LINEAGE_PERF=1 LINEAGE_PERF_SCREENSHOT=1 to run.');
  test.skip(SCREENSHOT_SCALES.length === 0, 'No screenshot scales configured.');

  const seededRoots = new Map<number, string>();

  test.beforeAll(async () => {
    if (!RUN || !SCREENSHOT_RUN) return;
    // Match the synthetic suite seed budget — schemas + per-column FGLs
    // make seeding three scales non-trivial on a cold DataHub.
    test.setTimeout(30 * 60_000);
    const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
    try {
      const gmsToken = readGmsToken(users.admin.username);
      for (const size of SCREENSHOT_SCALES) {
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

  test.afterAll(async () => {
    if (measurements.length === 0) return;
    // Emit as a test artifact rather than `console.log` so the summary
    // survives CI log truncation and can be pasted into the rollout RFC
    // without re-instrumentation.
    const columns = [
      'scale',
      'variant',
      'clickToDownloadMs',
      'downloadByteCount',
      'heapBeforeMb',
      'heapAfterMb',
      'heapDeltaMb',
      'domNodesBeforeCapture',
      'domNodesAfterCapture',
    ] as const;
    const lines = [columns.join('\t')];
    for (const m of measurements) {
      lines.push(
        [
          m.scale,
          m.variant,
          m.clickToDownloadMs,
          m.downloadByteCount,
          m.heapBeforeMb,
          m.heapAfterMb,
          m.heapDeltaMb,
          m.domNodesBeforeCapture,
          m.domNodesAfterCapture,
        ].join('\t'),
      );
    }
    const { mkdirSync, writeFileSync } = await import('node:fs');
    const path = await import('node:path');
    const outDir = path.resolve(__dirname, '..', '..', 'test-results');
    mkdirSync(outDir, { recursive: true });
    writeFileSync(path.join(outDir, 'lineage-screenshot-stress.tsv'), `${lines.join('\n')}\n`);
  });

  for (const size of SCREENSHOT_SCALES) {
    for (const variant of SCREENSHOT_VARIANTS) {
      test.describe(`screenshot scale ${size} [${variant.name}]`, () => {
        test.beforeEach(async ({ apiMock }) => {
          test.skip(!RUN || !SCREENSHOT_RUN);
          await apiMock.setFeatureFlags({ lineageGraphV3: false });
        });

        test(`screenshot ${size} — capture full graph`, async ({ page, logger, logDir }) => {
          // html-to-image serialises every SVG path + every node — give
          // it room at 1000.
          test.setTimeout(180_000);
          const lp = new LineagePerfPage(page, logger, logDir);
          await lp.setPerfFlags(variant.flagString);

          const rootUrn = seededRoots.get(size);
          if (!rootUrn) {
            throw new Error(`Screenshot seed missing for scale ${size}.`);
          }

          await lp.goToLineageGraph('dataset', rootUrn);
          await lp.waitForReactFlowNode(rootUrn);
          await lp.waitForStableNodeCount({ maxMs: 15_000 });

          // Mount the full neighbour set so capture matches what a user
          // would see after exploring, not the default-fanout subset.
          await lp.zoomOut(2);
          await lp.expandFanoutFully();
          await lp.waitForStableNodeCount({ maxMs: 30_000 });

          const heapBeforeMb = await readHeapMb(page);
          const domNodesBeforeCapture = await page.evaluate(
            () => document.querySelectorAll('.react-flow__node').length,
          );

          // Capture page errors during the capture window so a silent
          // html-to-image failure (e.g. tainted canvas) surfaces as an
          // assertion message instead of a 90 s wait-for-download timeout.
          const captureSideErrors: string[] = [];
          const onConsoleError = (msg: import('@playwright/test').ConsoleMessage) => {
            if (msg.type() === 'error') captureSideErrors.push(`[console.error] ${msg.text()}`);
          };
          const onPageError = (err: Error) => {
            captureSideErrors.push(`[pageerror] ${err.message}`);
          };
          page.on('console', onConsoleError);
          page.on('pageerror', onPageError);

          const screenshotButton = page.locator('.anticon-camera').first().locator('xpath=ancestor::button[1]');
          await expect(screenshotButton).toBeVisible({ timeout: 10_000 });

          // Race the download event against the button re-enabling: the
          // V2 button disables itself while capturing and re-enables in
          // a try/finally. If it re-enables without firing a download,
          // capture failed (caught by `getPreviewImage`'s `.catch` and
          // surfaced as an antd `message.error` to the user).
          const downloadPromise = page.waitForEvent('download', { timeout: 90_000 });

          const captureFinishedPromise = (async () => {
            const deadline = Date.now() + 90_000;
            while (Date.now() < deadline) {
              const isDisabled = await screenshotButton.evaluate((el) => el.hasAttribute('disabled'));
              if (isDisabled) {
                while (Date.now() < deadline) {
                  await page.waitForTimeout(200);
                  const stillDisabled = await screenshotButton.evaluate((el) => el.hasAttribute('disabled'));
                  if (!stillDisabled) return 'capture-finished' as const;
                }
                return 'capture-finished' as const;
              }
              await page.waitForTimeout(50);
            }
            return 'capture-finished' as const;
          })();

          const clickStart = Date.now();
          await screenshotButton.click();
          const winner = await Promise.race([
            downloadPromise.then(() => 'download' as const).catch(() => 'capture-finished' as const),
            captureFinishedPromise,
          ]);
          const clickToDownloadMs = Date.now() - clickStart;

          page.off('console', onConsoleError);
          page.off('pageerror', onPageError);

          if (winner !== 'download') {
            const detail =
              captureSideErrors.length > 0 ? captureSideErrors.join('\n  ') : '(no console / page errors captured)';
            throw new Error(
              `Screenshot capture finished without firing a download event after ${clickToDownloadMs} ms. Console / page errors during capture:\n  ${detail}`,
            );
          }

          const download = await downloadPromise;
          const downloadPath = await download.path();
          if (!downloadPath) {
            throw new Error('Screenshot download had no path.');
          }
          const { statSync } = await import('node:fs');
          const downloadByteCount = statSync(downloadPath).size;

          const heapAfterMb = await readHeapMb(page);
          const heapDeltaMb =
            heapBeforeMb != null && heapAfterMb != null ? Math.round((heapAfterMb - heapBeforeMb) * 10) / 10 : null;

          const domNodesAfterCapture = await page.evaluate(() => document.querySelectorAll('.react-flow__node').length);

          measurements.push({
            scale: size,
            variant: variant.name,
            clickToDownloadMs,
            downloadByteCount,
            heapBeforeMb,
            heapAfterMb,
            heapDeltaMb,
            domNodesBeforeCapture,
            domNodesAfterCapture,
          });

          // Sanity floor: a virtualised screenshot of just the viewport
          // would be a small PNG (< ~50 KB at 1280×720 with little
          // content). A full-graph capture is multi-MB at 500/1000
          // nodes. We don't assert tighter than a non-trivial floor
          // because compression is content-dependent.
          expect(downloadByteCount).toBeGreaterThan(10_000);
        });
      });
    }
  }
});
