/**
 * Accessibility audit for lineage V2 under virtualisation.
 *
 * Virt unmounts off-screen nodes, which means assistive tech can't
 * navigate to them via the standard tab order or screen-reader DOM
 * walk. This spec runs axe-core in a few states to surface:
 *   - WCAG / best-practice violations on the visible portion of the
 *     graph (independent of virt — these are pre-existing if any).
 *   - Whether enabling `virt` introduces any *new* a11y violations
 *     against the current rules (e.g. orphaned labels, missing
 *     `aria-hidden` on tentative edges, etc.).
 *
 * Surfaces violations as test failures only when `LINEAGE_A11Y_FAIL=1`
 * is set; otherwise we log them as test attachments so this can
 * baseline the audit without blocking CI.
 *
 * Opt-in via `LINEAGE_A11Y=1`. The spec uses the existing small-graph
 * fixture so it runs in a few seconds.
 */

import AxeBuilder from '@axe-core/playwright';
import { expect, request as playwrightRequest } from '@playwright/test';

import { test } from '../../fixtures/base-test';
import { readGmsToken } from '../../fixtures/login';
import { seedSyntheticLineage } from '../../helpers/seeders/lineage-perf-seeder';
import { LineagePerfPage } from '../../pages/lineage-perf.page';
import { users } from '../../data/users';
import { gmsUrl } from '../../utils/constants';

const RUN = process.env.LINEAGE_A11Y === '1';
const FAIL_ON_VIOLATIONS = process.env.LINEAGE_A11Y_FAIL === '1';
// Opt in to the large-graph scan separately because it requires the
// synthetic 100 fixture (~30 s seed on a cold DB).
const RUN_LARGE = process.env.LINEAGE_A11Y_LARGE === '1';

const SMALL_GRAPH_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';
const LARGE_GRAPH_SCALE = 100;

interface VariantSpec {
  name: string;
  flagString: string;
}
const BASELINE_FLAGS = 'virt-off overscan-off';
const VARIANTS: VariantSpec[] = [
  { name: 'baseline', flagString: BASELINE_FLAGS },
  { name: 'virt', flagString: 'virt' },
];

interface ViolationSummary {
  id: string;
  impact: string | null | undefined;
  help: string;
  nodes: number;
  sampleHtml: string;
}

function summarise(results: Awaited<ReturnType<AxeBuilder['analyze']>>): ViolationSummary[] {
  return results.violations.map((v) => ({
    id: v.id,
    impact: v.impact,
    help: v.help,
    nodes: v.nodes.length,
    sampleHtml: v.nodes[0]?.html?.slice(0, 200) ?? '',
  }));
}

async function runAxeAndReport(
  page: import('@playwright/test').Page,
  label: string,
  testInfo: import('@playwright/test').TestInfo,
): Promise<void> {
  // Skip the page chrome (header, nav, etc.) — the goal is lineage-
  // specific issues. Restrict to WCAG A/AA to keep first-pass output
  // actionable; best-practice rules are opinionated and noisier.
  const results = await new AxeBuilder({ page })
    .include('.react-flow')
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .analyze();
  const summary = summarise(results);
  const summaryJson = JSON.stringify(
    { label, totalViolations: results.violations.length, violations: summary },
    null,
    2,
  );
  await testInfo.attach(`a11y-${label}.json`, {
    body: summaryJson,
    contentType: 'application/json',
  });

  if (FAIL_ON_VIOLATIONS) {
    expect(results.violations, JSON.stringify(summary, null, 2)).toEqual([]);
  }
}

test.describe('lineage a11y audit', () => {
  test.skip(!RUN, 'Set LINEAGE_A11Y=1 to run.');

  for (const variant of VARIANTS) {
    test(`small graph axe-core scan [${variant.name}]`, async ({ page, logger, logDir }, testInfo) => {
      test.setTimeout(60_000);
      const lp = new LineagePerfPage(page, logger, logDir);
      await lp.setPerfFlags(variant.flagString);
      await lp.goToLineageGraph('dataset', SMALL_GRAPH_URN);
      await lp.waitForReactFlowNode(SMALL_GRAPH_URN);
      await lp.waitForStableNodeCount({ maxMs: 10_000 });

      await runAxeAndReport(page, `small-${variant.name}`, testInfo);
    });
  }

  // Large-graph scan: the only place where virt actually unmounts nodes
  // (small graph fits in viewport). Compares whether virt regresses on
  // any axe rule once off-screen nodes leave the DOM.
  test.describe('large graph axe-core scan', () => {
    test.skip(!RUN_LARGE, 'Set LINEAGE_A11Y_LARGE=1 to run the synthetic large-graph audit.');

    let rootUrn: string | null = null;

    test.beforeAll(async () => {
      if (!RUN || !RUN_LARGE) return;
      test.setTimeout(30 * 60_000);
      const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
      try {
        const gmsToken = readGmsToken(users.admin.username);
        const handles = await seedSyntheticLineage(apiContext, gmsToken, {
          scale: String(LARGE_GRAPH_SCALE),
          upstreamCount: LARGE_GRAPH_SCALE / 2,
          downstreamCount: LARGE_GRAPH_SCALE / 2,
        });
        rootUrn = handles.rootUrn;
      } finally {
        await apiContext.dispose();
      }
    });

    for (const variant of VARIANTS) {
      test(`large graph axe-core scan [${variant.name}]`, async ({ page, logger, logDir }, testInfo) => {
        test.setTimeout(90_000);
        if (!rootUrn) throw new Error('Large-graph seed missing.');
        const lp = new LineagePerfPage(page, logger, logDir);
        await lp.setPerfFlags(variant.flagString);
        await lp.goToLineageGraph('dataset', rootUrn);
        await lp.waitForReactFlowNode(rootUrn);
        await lp.waitForStableNodeCount({ maxMs: 15_000 });
        await lp.zoomOut(2);
        await lp.expandFanoutFully();
        await lp.waitForStableNodeCount({ maxMs: 30_000 });

        await runAxeAndReport(page, `large-${variant.name}`, testInfo);
      });
    }
  });
});
